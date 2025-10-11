"""
天勤数据工厂测试脚本
支持从环境变量加载账号信息

环境变量配置:
    TQ_USERNAME: 天勤账户用户名
    TQ_PASSWORD: 天勤账户密码
    
使用方法:
    1. 复制 env.example 为 .env
    2. 在 .env 文件中填入真实的天勤账户信息
    3. 或者设置系统环境变量 TQ_USERNAME 和 TQ_PASSWORD
"""

from ctypes import POINTER
import datetime
import os
from wtpy.apps.datahelper import DHFactory as DHF

# 加载环境变量
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # 如果没有安装python-dotenv，继续使用系统环境变量
    pass


# hlper = DHF.createHelper("baostock")
# hlper.auth()

# tushare
# hlper = DHF.createHelper("tushare")
# hlper.auth(**{"token":"xxxxxxxxxxx", "use_pro":True})

# rqdata
# hlper = DHF.createHelper("rqdata")
# hlper.auth(**{"username":"00000000", "password":"0000000"})

# tqsdk - 从环境变量获取账号信息
def get_tq_credentials():
    """从环境变量获取天勤账号信息"""
    username = os.getenv('TQ_USERNAME')
    password = os.getenv('TQ_PASSWORD')

    if not username or not password:
        raise ValueError(
            "未找到天勤账号信息！\n"
            "请设置环境变量 TQ_USERNAME 和 TQ_PASSWORD，\n"
            "或创建 .env 文件并填入账号信息。\n"
            "参考 env.example 文件格式。"
        )

    return username, password


try:
    username, password = get_tq_credentials()
    hlper = DHF.createHelper("tqsdk")
    hlper.auth(**{"username": username, "password": password})
    print(f"天勤账号认证成功: {username}")
except ValueError as e:
    print(f"配置错误: {e}")
    exit(1)
except Exception as e:
    print(f"天勤认证失败: {e}")
    exit(1)

# 落地股票列表
# hlper.dmpCodeListToFile("stocks.json", hasStock=False)

# 下载K线数据
# hlper.dmpBarsToFile(folder='./', codes=["CFFEX.IF.HOT"], period='min1', start_date=datetime.datetime(2025, 10, 1, 1, 0), end_date=datetime.datetime(2025, 10, 10, 15, 0))
# hlper.dmpBarsToFile(folder='./', codes=["DCE.jm.HOT"], period='min1', start_date=datetime.datetime(2023, 1, 1, 1, 0), end_date=datetime.datetime(2025, 9, 30, 15, 0))
# hlper.dmpBarsToFile(folder='./', codes=["CFFEX.IF.HOT","CFFEX.IC.HOT"], period='min5')
# hlper.dmpBarsToFile(folder='./', codes=["SZSE.399005","SZSE.399006","SZSE.399303"], period='day')

# 下载复权因子
# hlper.dmpAdjFactorsToFile(codes=["SSE.600000",'SZSE.000001'], filename="./adjfactors.json")

# 初始化数据库
# dbHelper = MysqlHelper("127.0.0.1","root","","test", 5306)
# dbHelper.initDB()

# 将数据下载到数据库
# hlper.dmpBarsToDB(dbHelper, codes=["CFFEX.IF.2103"], period="day")
# hlper.dmpAdjFactorsToDB(dbHelper, codes=["SSE.600000",'SSE.600001'])

# 将数据直接落地成dsb
# cb如果传None 则使用 内置 cb_store_bar_to_dsb 来处理
hlper.dmpBars(codes=["DCE.jm.HOT"], cb=None, start_date=datetime.datetime(2016, 1, 1), end_date=datetime.datetime(2025, 10, 11), period="min1")
