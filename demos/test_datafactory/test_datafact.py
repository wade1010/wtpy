from ctypes import POINTER
import datetime
import os
from wtpy.WtCoreDefs import WTSBarStruct
from wtpy.apps.datahelper import DHFactory as DHF

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
def on_bars_block(exchg: str, stdCode: str, firstBar: POINTER(WTSBarStruct), count: int, period: str):
    from wtpy.wrapper import WtDataHelper
    from ctypes import POINTER, cast
    import numpy as np

    dtHelper = WtDataHelper()
    if stdCode[-4:] == '.HOT':
        stdCode = stdCode[:-4] + "_HOT"
    else:
        ay = stdCode.split(".")
        if exchg == 'CZCE':
            stdCode = ay[1] + ay[2][1:]
        else:
            stdCode = ay[1] + ay[2]

    filename = f"../storage/his/{period}/{exchg}/"
    if not os.path.exists(filename):
        os.makedirs(filename)
    filename += f"{stdCode}.dsb"
    if period == "day":
        period = "d"
    elif period == "min1":
        period = "m1"
    else:
        period = "m5"

    # 读取现有数据
    existing_data = None
    existing_count = 0
    if os.path.exists(filename):
        try:
            existing_data = dtHelper.read_dsb_bars(filename)
            if existing_data:
                existing_count = len(existing_data)
                print(f"[DSB] {stdCode} 已有数据: {existing_count} 条")
            else:
                print(f"[DSB] {stdCode} DSB文件存在但无数据")
        except Exception as e:
            print(f"[DSB] 读取 {stdCode} 已有数据失败: {str(e)}")
    else:
        print(f"[DSB] {stdCode} DSB文件不存在，将新建")

    # 将新数据转换为numpy数组
    print(f"[批次] 接收到新数据: {count} 条")
    new_bars = []
    for i in range(count):
        bar = firstBar[i]
        new_bars.append((bar.date, bar.reserve, bar.time, bar.open, bar.high, bar.low, bar.close, bar.settle, bar.money, bar.vol, bar.hold, bar.diff))

    # 合并数据并去重
    if existing_data is not None and len(existing_data) > 0:
        # 获取现有数据的numpy数组
        existing_array = existing_data.ndarray

        # 创建新数据的numpy数组
        new_array = np.array(new_bars, dtype=existing_array.dtype)

        # 合并数据
        combined_array = np.concatenate([existing_array, new_array])
        combined_count = len(combined_array)
        print(f"[合并] 合并后总数据: {combined_count} 条 (已有: {existing_count} + 新增: {count})")

        # 根据date和time去重并排序，保留后者（最新数据）
        # 创建复合键用于去重
        if period == "d":
            # 日线数据只按date去重，保留最后出现的
            keys = combined_array['date']
        else:
            # 分钟线数据按date和time组合去重，保留最后出现的
            keys = combined_array['date'] * 100000000 + combined_array['time']

        # 反向查找唯一值的索引，这样可以保留最后出现的记录
        _, unique_indices = np.unique(keys[::-1], return_index=True)
        # 由于我们反向了数组，需要调整索引
        unique_indices = len(keys) - 1 - unique_indices

        # 获取去重后的数据并排序
        unique_array = combined_array[np.sort(unique_indices)]
        duplicate_count = combined_count - len(unique_array)
        actual_new_count = len(unique_array) - existing_count
        print(f"[去重] 去重处理: 删除重复 {duplicate_count} 条，实际新增 {actual_new_count} 条")

        # 创建新的WTSBarStruct数组
        final_count = len(unique_array)
        BUFFER = WTSBarStruct * final_count
        buffer = BUFFER()

        for i in range(final_count):
            buffer[i].date = unique_array[i]['date']
            buffer[i].reserve = unique_array[i]['reserve']
            buffer[i].time = unique_array[i]['time']
            buffer[i].open = unique_array[i]['open']
            buffer[i].high = unique_array[i]['high']
            buffer[i].low = unique_array[i]['low']
            buffer[i].close = unique_array[i]['close']
            buffer[i].settle = unique_array[i]['settle']
            buffer[i].money = unique_array[i]['turnover']
            buffer[i].vol = unique_array[i]['volume']
            buffer[i].hold = unique_array[i]['open_interest']
            buffer[i].diff = unique_array[i]['diff']

        # 保存数据并显示统计报告
        dtHelper.store_bars(filename, cast(buffer, POINTER(WTSBarStruct)), final_count, period)
        
        # 显示完整统计报告
        print(f"[统计] {stdCode} 数据处理完成:")
        print(f"  - DSB原有数据: {existing_count} 条")
        print(f"  - 本批次接收: {count} 条")
        print(f"  - 合并后总计: {combined_count} 条")
        print(f"  - 去重删除: {duplicate_count} 条")
        print(f"  - 实际新增: {actual_new_count} 条")
        print(f"  - 最终保存: {final_count} 条")
        if count > 0:
            duplicate_rate = (duplicate_count / count) * 100
            print(f"  - 重复数据率: {duplicate_rate:.1f}%")
    else:
        # 没有现有数据，直接保存新数据
        dtHelper.store_bars(filename, firstBar, count, period)
        print(f"[统计] {stdCode} 数据处理完成:")
        print(f"  - DSB原有数据: 0 条")
        print(f"  - 本批次接收: {count} 条")
        print(f"  - 新建文件保存: {count} 条")


hlper.dmpBars(codes=["DCE.jm.HOT"], cb=on_bars_block, start_date=datetime.datetime(2020, 1, 1), end_date=datetime.datetime(2025, 10, 11), period="min1")
