
from vnpy_evo.event import EventEngine
from vnpy_evo.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
from vnpy_chartwizard import ChartWizardApp

from vnpy_backpack import BackpackGateway
from vnpy_okx import OkxGateway


def main():
    """主入口函数"""
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BackpackGateway)
    main_engine.add_gateway(OkxGateway)
    main_engine.add_app(ChartWizardApp)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showNormal()

    qapp.exec()


if __name__ == "__main__":
    main()
