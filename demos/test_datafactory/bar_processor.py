from ctypes import POINTER, cast
import os
import numpy as np
from wtpy.WtCoreDefs import WTSBarStruct
from wtpy.wrapper.WtDtHelper import WtDataHelper


def on_bars_block(exchg: str, stdCode: str, firstBar: POINTER(WTSBarStruct), count: int, period: str):
    """
    处理K线数据块的回调函数
    
    @exchg      交易所代码
    @stdCode    标准代码
    @firstBar   K线数据指针
    @count      K线数据条数
    @period     K线周期
    """
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
        # 将现有数据转换为相同格式
        existing_bars = []
        for bar in existing_data:
            existing_bars.append((bar['date'], bar['reserve'], bar['time'], bar['open'], bar['high'], bar['low'], bar['close'], bar['settle'], bar['turnover'], bar['volume'], bar['open_interest'], bar['diff']))
        
        # 合并数据
        combined_bars = existing_bars + new_bars
        combined_count = len(combined_bars)
        
        # 创建结构化数组并去重
        dtype = [('date', 'u4'), ('reserve', 'u4'), ('time', 'u4'), ('open', 'f8'), ('high', 'f8'), ('low', 'f8'), ('close', 'f8'), ('settle', 'f8'), ('turnover', 'f8'), ('volume', 'f8'), ('open_interest', 'f8'), ('diff', 'f8')]
        combined_array = np.array(combined_bars, dtype=dtype)
        
        # 按日期和时间排序并去重
        combined_array = np.sort(combined_array, order=['date', 'time'])
        unique_array, unique_indices = np.unique(combined_array[['date', 'time']], return_index=True)
        unique_array = combined_array[unique_indices]
        
        final_count = len(unique_array)
        duplicate_count = combined_count - final_count
        actual_new_count = final_count - existing_count

        # 创建WTSBarStruct数组
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
