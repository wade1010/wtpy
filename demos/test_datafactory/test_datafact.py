from ctypes import POINTER
import datetime
import os
from wtpy.WtCoreDefs import WTSBarStruct
from wtpy.apps.datahelper import DHFactory as DHF
from bar_processor import on_bars_block

# hlper = DHF.createHelper("baostock")
# hlper.auth()

# tushare
# hlper = DHF.createHelper("tushare")
# hlper.auth(**{"token":"xxxxxxxxxxx", "use_pro":True})

# rqdata
# hlper = DHF.createHelper("rqdata")
# hlper.auth(**{"username":"00000000", "password":"0000000"})

# tqsdk
hlper = DHF.createHelper("tqsdk")
hlper.auth(**{"username": "", "password": ""})

# 落地股票列表
# hlper.dmpCodeListToFile("stocks.json")

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

hlper.dmpBars(codes=["DCE.jm.HOT"], cb=on_bars_block, start_date=datetime.datetime(2020, 1, 1), end_date=datetime.datetime(2025, 10, 11), period="min1")
