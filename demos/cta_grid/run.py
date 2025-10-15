from wtpy import WtBtEngine, EngineType
from wtpy.apps import WtBtAnalyst

import sys

sys.path.append('../Strategies')
from StraGrid import GridStra

if __name__ == "__main__":
    # 创建一个运行环境，并加入策略
    engine = WtBtEngine(EngineType.ET_CTA)
    engine.init('../common/', "configbt.yaml")
    engine.configBacktest(202501010930, 202510151500)
    engine.configBTStorage(mode="csv", path="../storage/")
    engine.commitBTConfig()
    straInfo = GridStra(name='pygsol_IF', code="CFFEX.IF.HOT", barCnt=50, period="m5", short_days=5, long_days=10, \
                        num=5, p1=0.05, p2=-0.05, capital=10000000, margin_rate=0.1, stop_loss=0.8)
    engine.set_cta_strategy(straInfo)

    engine.run_backtest()

    analyst = WtBtAnalyst()
    analyst.add_strategy("pygsol_IF", folder="./outputs_bt/", init_capital=10000000, rf=0.02, annual_trading_days=240)
    analyst.run()

    kw = input('press any key to exit\n')
    engine.release_backtest()
