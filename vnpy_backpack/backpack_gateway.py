import base64
import json
import urllib
import hashlib
import hmac
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from asyncio import run_coroutine_threadsafe
from zoneinfo import ZoneInfo
from time import sleep
from cryptography.hazmat.primitives.asymmetric import ed25519
from aiohttp import ClientSSLError

from vnpy_evo.event import Event, EventEngine
from vnpy_evo.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)

from vnpy_evo.trader.gateway import BaseGateway
from vnpy_evo.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy_evo.trader.event import EVENT_TIMER
from vnpy_evo.trader.utility import round_to

from vnpy_rest import RestClient, Request, Response
from vnpy_websocket import WebsocketClient


# Timezone constant
UTC_TZ = ZoneInfo("Asia/Shanghai")

# Real server hosts
REST_HOST = "https://api.backpack.exchange"
WEBSOCKET_HOST = "wss://ws.backpack.exchange"

# order type map
ORDERTYPE_VT2BACKPACK: dict[OrderType, str] = {
    OrderType.LIMIT: "Limit",
    OrderType.MARKET: "Market",
    OrderType.FOK: "FOK",
    OrderType.IOC: "IOC",
}

ORDERTYPE_BACKPACK2VT: dict[str, OrderType] = {v:k for k, v in ORDERTYPE_VT2BACKPACK.items()}

# direction map
DIRECTION_VT2BACKPACK: dict[Direction, str] = {
    Direction.LONG: "Bid",
    Direction.SHORT: "Ask"
}
DIRECTION_BACKPACK2VT: dict[str, Direction] = {v:k for k, v in DIRECTION_VT2BACKPACK.items()}

# order status map
STATUS_BACKPACK2VT: dict[str, Status] = {
    "New": Status.NOTTRADED,
    "PartiallyFilled": Status.PARTTRADED,
    "Filled": Status.ALLTRADED,
    "Cancelled": Status.CANCELLED,
    "?": Status.REJECTED,
    "Expired": Status.CANCELLED
}

INTERVAL_VT2BACKPACK: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
    Interval.WEEKLY: "1w",
}

TIMEDELTA_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
    Interval.WEEKLY: timedelta(weeks=1),
}

WEBSOCKET_TIMEOUT = 24 * 60 * 60

symbol_contract_map: dict[str, ContractData] = {}


def generate_datetime(timestamp: str, microsec: bool=True) -> datetime:
    """Generate datetime object from Backpack timestamp"""
    dt: datetime = datetime.fromtimestamp(float(timestamp) / 1e6) if microsec else datetime.fromtimestamp(float(timestamp))
    dt: datetime = dt.replace(tzinfo=UTC_TZ)
    return dt

class Security(Enum):
    """Security type"""
    NONE = 0
    SIGNED = 1
    API_KEY = 2


class BackpackGateway(BaseGateway):
    """
    The Backpack spot trading gateway for VeighNa.
    """

    default_name: str = "BACKPACK"

    default_setting: dict = {
        "API Key": "",
        "API Secret": "",
        "Server": ["REAL"],
        "Kline Stream": ["False", "True"],
        "Proxy Host": "",
        "Proxy Port": 0
    }

    exchanges: Exchange = [Exchange.BACKPACK]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """
        The init method of the gateway.

        event_engine: the global event engine object of VeighNa
        gateway_name: the unique name for identifying the gateway
        """
        super().__init__(event_engine, gateway_name)

        self.trade_ws_api: BackpackTradeWebsocketApi = BackpackTradeWebsocketApi(self)
        self.market_ws_api: BackpackDataWebsocketApi = BackpackDataWebsocketApi(self)
        self.rest_api: BackpackRestApi = BackpackRestApi(self)

        self.orders: dict[str, OrderData] = {}

    def connect(self, setting: dict):
        """Start server connections"""
        key: str = setting["API Key"]
        secret: str = setting["API Secret"]
        server: str = setting["Server"]
        kline_stream: bool = setting["Kline Stream"] == "True"
        proxy_host: str = setting["Proxy Host"]
        proxy_port: int = setting["Proxy Port"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        self.market_ws_api.connect(key, secret, server, kline_stream, proxy_host, proxy_port)
        self.trade_ws_api.connect(key, secret, server, proxy_host, proxy_port)


    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        self.market_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """Send new order"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel existing order"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """Not required since Backpack provides websocket update"""
        pass

    def query_position(self) -> None:
        """Use rest api to query position data"""
        return self.rest_api.query_position()

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """Close server connections"""
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

    def on_order(self, order: OrderData) -> None:
        """Save a copy of order and then push"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """Get previously saved order"""
        return self.orders.get(orderid, None)

class BackpackRestApi(RestClient):
    """backpack REST API client"""

    def __init__(self, gateway: BackpackGateway) -> None:
        super().__init__()

        self.gateway: BackpackGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api : BackpackTradeWebsocketApi = BackpackTradeWebsocketApi(self.gateway)
        self.market_ws_api : BackpackDataWebsocketApi = BackpackDataWebsocketApi(self.gateway)

        self.key = ""
        self.secret = ""

        self.window = 5000

        self.keep_alive_count = 0
        self.connect_time = 0

        self.order_count = 0
        self.order_prefix :str = ""
        self.time_offset = 0
    def sign(self, request: Request) -> None:
        """Standard callback for signing a request"""
        if request.extra is None:
            return request
        security: Security = request.extra["security"]
        if security == Security.NONE:
            return request
        
        if request.params and request.data:
            params = {**request.params, **json.loads(request.data)}
        elif request.params:
            params = request.params
        elif request.data:
            params = json.loads(request.data)
        else:
            params = dict()
        
        if 'postOnly' in params:
            params = params.copy()
            params['postOnly'] = str(params['postOnly']).lower()

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)
            if request.extra["instruction"]:
                query_list = [("instruction", request.extra["instruction"])]
            else:
                query_list = []
            query_list += [(k, v)for k, v in sorted(params.items())] 
            query_list += [("timestamp", str(timestamp)), ("window", str(self.window))]
            query: str = urllib.parse.urlencode(query_list)


            # sign the query with ed25519
            signature: bytes = self.private_key.sign(query.encode("utf-8"))
            signature: str = base64.b64encode(signature).decode("utf-8")

            # query += "&signature={}".format(signature)

        # 添加请求头
        headers = {
            "X-Timestamp": str(timestamp),
            "X-Window": str(self.window),
            "X-API-Key": self.key,
            "X-Signature": signature,
            "Content-Type": "application/json; charset=utf-8"
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request
    
    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret
        self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(secret))
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.order_prefix = '1'+datetime.now().strftime("%d%H%M%S")
        

        self.init(REST_HOST, proxy_host, proxy_port)

        self.start()

        self.gateway.write_log("REST API started")

        self.query_time()
        self.query_contract()

    def query_time(self) -> None:
        """Query server time"""
        extra: dict = {"security": Security.NONE}

        path: str = "/api/v1/time"

        self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            extra=extra
        )

    def query_account(self) -> None:
        """Query account balance"""
        extra: dict = {"security": Security.SIGNED,
                       "instruction": "balanceQuery"}
        self.add_request(
            method="GET",
            path="/api/v1/capital",
            callback=self.on_query_account,
            extra=extra,
        )

    def query_position(self) -> None:
        """Query position data"""
        extra: dict = {"security": Security.SIGNED,
                       "instruction": "balanceQuery"}
        self.add_request(
            method="GET",
            path="/api/v1/capital",
            callback=self.on_query_account,
            extra=extra
        )

    def query_order(self) -> None:
        """Query open orders"""
        extra: dict = {"security": Security.SIGNED,
                       "instruction": "orderQueryAll"}
        self.add_request(
            method="GET",
            path="/api/v1/orders",
            callback=self.on_query_order,
            extra=extra,
        )

    def query_contract(self) -> None:
        """Query available contracts"""
        extra: dict = {"security": Security.NONE}
        
        self.add_request(
            method="GET",
            path="/api/v1/markets",
            callback=self.on_query_contract,
            extra=extra
        )

    def send_order(self, req: OrderRequest) -> str:
        """Send a new order """
        # Generate new order id
        self.order_count += 1
        orderid: str = self.order_prefix + str(self.order_count)

        # Push a submitting order event
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        # Create order parameters
        extra: dict = {
            "security": Security.SIGNED,
            "order": order,
            "instruction": "orderExecute",
                       }

        params: dict = {
            "symbol": req.symbol.upper(),
            "side": DIRECTION_VT2BACKPACK[req.direction],
            "quantity": "{:.8f}".format(req.volume),
            "clientId": int(orderid),
            "timeInForce": "",
            "selfTradePrevention": "RejectBoth"
        }

        if req.type == OrderType.LIMIT:
            params["timeInForce"] = "GTC"
            params["price"] = "{:8f}".format(req.price)
            params['orderType'] = "Limit"
            params['postOnly'] = True
        
        elif req.type == OrderType.FOK:
            params['orderType'] = "Limit"
            params['timeInForce'] = "FOK"
            params["price"] = "{:8f}".format(req.price)
    

        elif req.type == OrderType.STOP:
            params['orderType'] = "Market"
            params["triggerPrice"] = "{:8f}".format(req.price)

        elif req.type == OrderType.MARKET:
            params['orderType'] = "Market"
            params['timeInForce'] = "IOC"


        self.add_request(
            method="POST",
            path="/api/v1/order",
            callback=self.on_send_order,
            extra=extra,
            data=json.dumps(params),
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid
    
    def cancel_order(self, req: CancelRequest) -> None:
        """Cancel an existing order"""
        extra: dict = {
            "security": Security.SIGNED,
            "instruction": "orderCancel",
            "req": req
            }
        params: dict = {
            "symbol": req.symbol.upper(),
            "clientId": int(req.orderid)
        }

        self.add_request(
            method="DELETE",
            path="/api/v1/order",
            callback=self.on_cancel_order,
            extra=extra,
            data=json.dumps(params),
        )

    def update_time_offset(self) -> None:
        """Update server time offset to wsApi"""
        self.trade_ws_api.time_offset = self.time_offset
        self.market_ws_api.time_offset = self.time_offset

    def on_query_time(self, data: str, request: Request) -> None:
        """Callback of server time query"""
        local_time = int(time.time() * 1000)
        server_time = int(data)
        self.time_offset = local_time - server_time

        self.gateway.write_log(f"Server time updated, local offset: {self.time_offset}ms")

        # Query private data after time offset is calculated
        if self.key and self.secret:
            self.query_account()
            self.query_order()

    def on_query_account(self, data: dict, request: Request) -> None:
        """Callback of account balance query"""
        for account_id, account_data in data.items():
            if account_id in ["USDT", "USDC"]:

                account: AccountData = AccountData(
                    accountid=account_id,
                    balance=float(account_data["available"]) + float(account_data["locked"]),
                    frozen=float(account_data["locked"]),
                    gateway_name=self.gateway_name
                )

                if account.balance:
                    self.gateway.on_account(account)
            else:
                position: PositionData = PositionData(
                    symbol=account_id,
                    exchange=Exchange.BACKPACK,
                    direction=Direction.NET,
                    volume=float(account_data["available"]) + float(account_data["locked"]),
                    gateway_name=self.gateway_name
                )
                if position.volume:
                    self.gateway.on_position(position)



        self.gateway.write_log("Account balance data is received")

    def on_query_order(self, data: dict, request: Request) -> None:
        """Callback of open orders query"""
        for d in data:
            if d["orderType"] not in ORDERTYPE_BACKPACK2VT:
                continue

            order: OrderData = OrderData(
                orderid=d["clientId"],
                symbol=d["symbol"].lower(),
                exchange=Exchange.BACKPACK,
                price=float(d["price"]),
                volume=float(d["quanlity"]),
                type=ORDERTYPE_BACKPACK2VT[d["orderType"]],
                direction=DIRECTION_BACKPACK2VT[d["side"]],
                traded=float(d["executedQuantity"]),
                status=STATUS_BACKPACK2VT.get(d["status"], None),
                datetime=generate_datetime(d["createdAt"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.gateway.write_log("Open orders data is received")

    def on_query_contract(self, data: list, request: Request) -> None:
        """Callback of available contracts query"""
        for d in data:
            base_currency: str = d["baseSymbol"]
            quote_currency: str = d["quoteSymbol"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: int = 1
            min_volume: int = 1

            for filter_type, filter in d["filters"].items():
                if filter_type == "price":
                    pricetick = float(filter["tickSize"])
                elif filter_type == "quantity":
                    min_volume = float(filter["stepSize"])

            contract: ContractData = ContractData(
                symbol=d["symbol"].lower(),
                exchange=Exchange.BACKPACK,
                name=name,
                pricetick=pricetick,
                size=1,
                min_volume=min_volume,
                product=Product.SPOT,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("Available contracts data is received")

    def on_send_order(self, data: dict, request: Request) -> None:
        """Successful callback of send_order"""
        pass

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """Failed callback of send_order"""
        order: OrderData = request.extra["order"]
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"Send order failed, status code: {status_code}, message: {request.response.text}, request data: {request.data}"
        self.gateway.write_log(msg)
        

    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """Error callback of send_order"""
        order: OrderData = request.extra["order"]
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, (ConnectionError, ClientSSLError)):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """Successful callback of cancel_order"""
        pass

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """Failed callback of cancel_order"""
        if request.extra["order"]:
            order = request.extra["order"]
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg = f"Cancel orde failed, status code: {status_code}, message: {request.response.text}, order: {request.extra['order']} "
        self.gateway.write_log(msg)
    
    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """Query kline history data"""
        history: list[BarData] = []
        limit: int = 1000
        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # Create query parameters
            params: dict = {
                "symbol": req.symbol.upper(),
                "interval": INTERVAL_VT2BACKPACK[req.interval],
                "limit": limit,
                "startTime": start_time,
            }

            if req.end:
                end_time: int = int(datetime.timestamp(req.end))
                params["endTime"] = end_time 

            resp: Response = self.request(
                "GET",
                "/api/v1/klines",
                params=params,
            )

            # Break the loop if request failed
            if resp.status_code // 100 != 2:
                msg: str = f"Query kline history failed, status code: {resp.status_code}, message: {resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg: str = f"No kline history data is received, start time: {start_time}"
                    self.gateway.write_log(msg)
                    break

                buf: list[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=datetime.strptime(row['start'], 
                                                   "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("UTC")).astimezone(UTC_TZ),
                        interval=req.interval,
                        volume=float(row['volume']),
                        turnover=None,#TODO need to check the meaning of trades
                        open_price=float(row['open']),
                        high_price=float(row['high']),
                        low_price=float(row['low']),
                        close_price=float(row['close']),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                history.extend(buf)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime
                msg: str = f"Query kline history finished, {req.symbol} - {req.interval.value}, {begin} - {end}"
                self.gateway.write_log(msg)

                # Break the loop if the latest data received
                if len(data) < limit:
                    break

                # Update query start time
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

            # Wait to meet request flow limit
            sleep(0.5)

        # Remove the unclosed kline
        if history:
            history.pop(-1)
        return history
    

class BackpackTradeWebsocketApi(WebsocketClient):
    """Backpack Websocket API"""

    def __init__(self, gateway: BackpackGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BackpackGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        self.time_offset: int = 0
        self.key = ""
        self.secret = ""
        self.private_key = ""
        self.window = 5000

    def connect(self, key, secret, server: str, proxy_host: int, proxy_port: int) -> None:
        """Start server connection"""
        self.key = key
        self.secret = secret
        self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(secret))
        self.init(WEBSOCKET_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)
        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Trade Websocket API is connected")
        self.subscribe()
        

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        stream: str = packet.get("stream", None)
        if not stream:
            return
        data: dict = packet["data"]
        order_events = ["orderAccepted", "orderCancelled", "orderExpired", "orderFill"]
        if data["e"] in order_events:
            self.on_order(data)
        elif data["e"] == "listenKeyExpired":
            self.on_listen_key_expired()

    def on_listen_key_expired(self) -> None:
        """Callback of listen key expired"""
        self.gateway.write_log("Listen key is expired")
        self.disconnect()

    def disconnect(self) -> None:
        """"Close server connection"""
        self._active = False
        ws = self._ws
        if ws:
            coro = ws.close()
            run_coroutine_threadsafe(coro, self._loop)

    def generate_signature(self, instruction: str):
        sign_str = f"instruction={instruction}" if instruction else ""

        timestamp: int = int(time.time() * 1000)
        window = self.window
        if self.time_offset > 0:
            timestamp -= abs(self.time_offset)
        elif self.time_offset < 0:
            timestamp += abs(self.time_offset)
        sign_str += f"&timestamp={timestamp}&window={window}"
        signature: bytes = self.private_key.sign(sign_str.encode("utf-8"))
        signature: str = base64.b64encode(signature).decode("utf-8")
        return [self.key, signature, str(timestamp), str(window)]

    def subscribe(self) -> None:
        """Subscribe order update"""
        
        signature = self.generate_signature("subscribe")
        req: dict = {
            "method": "SUBSCRIBE",
            "params": ["account.orderUpdate"],
            "signature": signature
        }
        self.send_packet(req)

    def on_order(self, data: dict) -> None:
        """Callback of order and trade update"""
        if data["o"].capitalize() not in ORDERTYPE_BACKPACK2VT:
            return
        orderid: str = data["c"]

        offset = self.gateway.get_order(orderid).offset if self.gateway.get_order(orderid) else None

        order: OrderData = OrderData(
            symbol=data["s"].lower(),
            exchange=Exchange.BACKPACK,
            orderid=orderid,
            type=ORDERTYPE_BACKPACK2VT[data["o"].capitalize()],
            direction=DIRECTION_BACKPACK2VT[data["S"]],
            price=float(data["p"]),
            volume=float(data["q"]),
            traded=float(data["z"]),
            status=STATUS_BACKPACK2VT[data["X"]],
            datetime=generate_datetime(data["T"]),
            gateway_name=self.gateway_name,
            offset=offset
        )

        self.gateway.on_order(order)

        # Round trade volume to meet step size
        trade_volume = float(data["q"])-float(data["z"])
        contract: ContractData = symbol_contract_map.get(order.symbol, None)
        if contract:
            trade_volume = round_to(trade_volume, contract.min_volume)

        if not trade_volume:
            return

        trade: TradeData = TradeData(
            symbol=order.symbol,
            exchange=order.exchange,
            orderid=order.orderid,
            tradeid=data["t"],
            direction=order.direction,
            price=float(data["p"]),
            volume=trade_volume,
            datetime=generate_datetime(data["T"]),
            gateway_name=self.gateway_name,
            offset=offset
        )
        self.gateway.on_trade(trade)
        # call rest api to update position
        self.gateway.query_position()


    def on_position(self, packet: dict) -> None:
        """Callback of position update"""
        """Currently Backpack does not support position update"""
        pass

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Trade Websocket API is disconnected")


class BackpackDataWebsocketApi(WebsocketClient):
    """The data websocket API of BinanceSpotGateway"""

    def __init__(self, gateway: BackpackGateway) -> None:
        """
        The init method of the api.

        gateway: the parent gateway object for pushing callback data.
        """
        super().__init__()

        self.gateway: BackpackGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: dict[str, TickData] = {}
        self.key = ""
        self.secret = ""
        self.private_key = ""
        self.window = 5000
        self.time_offset = 0

        self.subscribed: dict[str, SubscribeRequest] = {}

    def connect(
        self,
        key,
        secret,
        server: str,
        kline_stream: bool,
        proxy_host: str,
        proxy_port: int,
    ) -> None:
        """Start server connection"""

        self.key = key
        self.secret = secret
        self.private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(secret))
        
        self.kline_stream = kline_stream

        self.init(WEBSOCKET_HOST, proxy_host, proxy_port, receive_timeout=WEBSOCKET_TIMEOUT)

        self.start()

    def on_connected(self) -> None:
        """Callback when server is connected"""
        self.gateway.write_log("Data Websocket API is connected")
        for req in list(self.subscribed.values()):
            self.subscribe(req)

    def subscribe(self, req: SubscribeRequest) -> None:
        """Subscribe market data"""
        
        self.subscribed[req.vt_symbol] = req


        # Initialize tick object
        tick: TickData = TickData(
            symbol=req.symbol,
            name=symbol_contract_map[req.symbol].name,
            exchange=req.exchange,
            datetime=datetime.now(UTC_TZ),
            gateway_name=self.gateway_name,
        )

        self.ticks[req.symbol] = tick

        channels = [
            f"ticker.{req.symbol.upper()}",
            f"depth.{req.symbol.upper()}",
            f"bookTicker.{req.symbol.upper()}"
        ]


        if self.kline_stream:
            channels.append(f"kline.1m.{req.symbol.upper()}")
        # signature = self.generate_signature("subscribe")
        req: dict = {
            "method": "SUBSCRIBE",
            "params": channels,
            # "signature": signature
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """Callback of data update"""
        stream: str = packet.get("stream", None)

        if not stream:
            return

        data: dict = packet["data"]

        channel, symbol = stream.split(".")[0], stream.split(".")[-1].lower()
        tick: TickData = self.ticks[symbol]

        if channel == "ticker":
            tick.volume = float(data['v'])
            tick.turnover = float(data['V'])
            tick.open_price = float(data['o'])
            tick.high_price = float(data['h'])
            tick.low_price = float(data['l'])
            tick.last_price = float(data['c'])
            tick.datetime = generate_datetime(data['E'])
            
        elif channel == "bookTicker":
            tick.datetime = generate_datetime(data['E'])
            tick.bid_price_1 = float(data['b'])
            tick.bid_volume_1 = float(data['B'])
            tick.ask_price_1 = float(data['a'])
            tick.ask_volume_1 = float(data['A'])

        elif channel == "depth":
            #TODO
            pass
        else:
            # Kline data
            # Check if bar is closed
            bar_ready: bool = data.get("X", False)
            if not bar_ready:
                return

            dt: datetime = datetime.strptime(data["t"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=ZoneInfo("UTC")).astimezone(UTC_TZ)
            if not tick.extra:
                tick.extra = {}
            data["v"] = data["v"] if data["v"] else 0
            if not data['o']:
                pass
            else:
                tick.extra["bar"] = BarData(
                    symbol=symbol.upper(),
                    exchange=Exchange.BACKPACK,
                    datetime=dt.replace(second=0, microsecond=0),
                    interval=Interval.MINUTE,
                    volume=float(data["v"]),
                    turnover=None,
                    open_price=float(data["o"]),
                    high_price=float(data["h"]),
                    low_price=float(data["l"]),
                    close_price=float(data["c"]),
                    gateway_name=self.gateway_name
                )

        if tick.last_price:
            tick.localtime = datetime.now()
            self.gateway.on_tick(copy(tick))

    def on_disconnected(self) -> None:
        """Callback when server is disconnected"""
        self.gateway.write_log("Data Websocket API is disconnected")
