import numpy as np

from wtpy.apps.datahelper.DHDefs import BaseDataHelper, DBHelper
from wtpy.WtCoreDefs import WTSBarStruct
from tqsdk import TqApi, TqAuth
from datetime import datetime, timedelta
import time as sleep_time
import json
import os


def stdCodeToTQ(stdCode: str):
    items = stdCode.split(".")
    exchg = items[0]
    if len(items) == 2 and exchg in ['SSE', 'SZSE']:
        # 简单股票代码，格式如SSE.600000
        return stdCode
    elif items[1] in ["IDX", "ETF", "STK", "OPT"]:
        # 标准股票代码，格式如SSE.IDX.000001
        return exchg + "." + items[2]
    elif len(items) == 3 and exchg in ["SHFE", "CFFEX", "DCE", "CZCE", "INE", "GFEX"]:
        # 标准期货代码，格式如CFFEX.IF.2103
        if items[2] != 'HOT':
            return exchg + '.' + items[1] + items[2]
        else:
            return "KQ.m@" + exchg + '.' + items[1]
    else:
        return stdCode


class DHTqSdk(BaseDataHelper):

    def __init__(self):
        BaseDataHelper.__init__(self)
        self.username = None
        self.password = None
        print("TqSdk helper has been created.")
        return

    def auth(self, **kwargs):
        if self.isAuthed:
            return
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        if self.username == "" or self.password == "":
            raise Exception("username or password is null.")
        self.isAuthed = True
        print("TqSdk has been authorized.")

    def dmpCodeListToFile(self, filename: str, hasIndex: bool = True, hasStock: bool = True):
        api = TqApi(auth=TqAuth(self.username, self.password))
        stocks = {
            "SSE": {},
            "SZSE": {}
        }
        futures = {
            "SHFE": {},
            "CFFEX": {},
            "DCE": {},
            "CZCE": {},
            "INE": {},
            "GFEX": {}
        }
        if hasStock:
            print("Fetching stock list...")
            for exchange in stocks.keys():
                code_list = api.query_quotes(ins_class="STOCK", exchange_id=exchange)
                code_list_info = api.query_symbol_info(code_list)
                for idx, row in code_list_info.iterrows():
                    sInfo = dict()
                    rawcode = row["instrument_id"].split('.')[-1]
                    sInfo["exchg"] = exchange
                    sInfo["code"] = rawcode
                    sInfo["name"] = row["instrument_name"]
                    sInfo["product"] = "STK"
                    stocks[sInfo["exchg"]][rawcode] = sInfo
        if hasIndex:
            print("Fetching Index list...")
            for exchange in stocks.keys():
                code_list = api.query_quotes(ins_class="INDEX", exchange_id=exchange)
                code_list_info = api.query_symbol_info(code_list)
                for idx, row in code_list_info.iterrows():
                    sInfo = dict()
                    rawcode = row["instrument_id"].split('.')[-1]
                    sInfo["exchg"] = exchange
                    sInfo["code"] = rawcode
                    sInfo["name"] = row["instrument_name"]
                    sInfo["product"] = "IDX"
                    stocks[sInfo["exchg"]][rawcode] = sInfo
        print(f"Writing code list into file {filename}...")
        stocks.update(futures)
        f = open(filename, 'w', encoding='utf-8')
        f.write(json.dumps(stocks, sort_keys=True, indent=4, ensure_ascii=False))
        f.close()
        api.close()

    def dmpAdjFactorsToFile(self, codes: list, filename: str):
        raise Exception("TqSdk has not Adj Factors api")

    def dmpAdjFactorsToDB(self, dbHelper: DBHelper, codes: list):
        raise Exception("TqSdk has not Adj Factors api")

    def dmpBarsToFile1(self, folder: str, codes: list, start_date: datetime = None, end_date: datetime = None, period="day"):
        """
        需要专业版
        """
        api = TqApi(auth=TqAuth(self.username, self.password))
        if start_date is None:
            start_date = datetime(year=1990, month=1, day=1)
        if end_date is None:
            end_date = datetime.now()
        freq = ''
        filetag = ''
        if period == 'day':
            freq = 86400
            filetag = 'd'
        elif period == 'min5':
            freq = 300
            filetag = 'm5'
        elif period == "min1":
            freq = 60
            filetag = 'm1'
        elif isinstance(period, int):
            freq = period
            if (0 < period <= 86400) or period % 86400 == 0:
                filetag = str(freq)
            else:
                raise Exception("Unrecognized period")
        else:
            raise Exception("Unrecognized period")

        count = 0
        length = len(codes)
        for stdCode in codes:
            count += 1
            print(f"Fetching {period} bars of {stdCode}({count}/{length})...")
            code = stdCodeToTQ(stdCode)
            try:
                df_bars = api.get_kline_data_series(symbol=code, duration_seconds=freq, start_dt=start_date, end_dt=end_date, adj_type=None)
            except Exception as e:
                api.close()
                raise Exception(f"{e}")
            content = "date,time,open,high,low,close,volume\n"
            for idx, row in df_bars.iterrows():
                trade_date = datetime.fromtimestamp(row["datetime"] / 1000000000)
                date = trade_date.strftime("%Y-%m-%d")
                if freq == 86400:
                    time = '0'
                else:
                    time = trade_date.strftime("%H:%M:%S")
                o = str(row["open"])
                h = str(row["high"])
                l = str(row["low"])
                c = str(row["close"])
                v = str(row["volume"])
                items = [date, time, o, h, l, c, v]
                content += ','.join(items) + "\n"
                filename = "%s_%s.csv" % (stdCode, filetag)
                filepath = os.path.join(folder, filename)
                print(f"Writing bars into file {filepath}...")
                f = open(filepath, "w", encoding="utf-8")
                f.write(content)
                f.close()
        api.close()

    def dmpBarsToFile(self, folder: str, codes: list, start_date: datetime = None, end_date: datetime = None, period="day"):
        '''
        改造成回测模式，支持day、min1、min5
        每获取一批数据就追加到CSV文件中
        从前往后获取数据，保证数据顺序正确，适合追加模式写入

        将K线导出到指定的目录下的csv文件，文件名格式如SSE.600000_d.csv
        @folder 要输出的文件夹
        @codes  股票列表，格式如["SSE.600000","SZSE.000001"]
        @start_date 开始日期，datetime类型，传None则自动设置为1990-01-01
        @end_date   结束日期，datetime类型，传None则自动设置为当前日期
        @period K线周期，支持day、min1、min5
        '''
        from tqsdk import TqSim, TqBacktest
        from tqsdk.exceptions import BacktestFinished
        import os
        if start_date is None:
            start_date = datetime(year=1990, month=1, day=1)
        if end_date is None:
            end_date = datetime.now()

        # 设置周期和文件标签
        if period == 'day':
            freq = 86400
            filetag = 'd'
        elif period == 'min5':
            freq = 300
            filetag = 'm5'
        elif period == "min1":
            freq = 60
            filetag = 'm1'
        else:
            raise Exception("不支持的周期，仅支持day、min1、min5")

        count = 0
        length = len(codes)

        for stdCode in codes:
            count += 1
            if length > 1:
                print(f"[任务] 开始获取 {stdCode} 的 {period} K线 ({count}/{length})")
            else:
                print(f"[任务] 开始获取 {stdCode} 的 {period} K线")
            code = stdCodeToTQ(stdCode)
            # 准备文件路径
            filename = "%s_%s.csv" % (stdCode, filetag)
            filepath = os.path.join(folder, filename)

            # 如文件已存在，先删除，确保重新开始
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                    print(f"[文件] 发现旧文件，已删除: {filepath}")
                except Exception as e:
                    print(f"[文件] 删除旧文件失败: {filepath}，错误: {e}")

            with open(filepath, "w", encoding="utf-8") as f:
                f.write("date,time,open,high,low,close,volume,open_interest\n")

            # 已存在的日期时间集合，用于去重
            existing_datetimes = set()

            # 当前开始时间，从用户指定的起始时间开始
            current_start = start_date

            while current_start < end_date:
                # 转换为日期格式，去掉时间部分
                start_np = np.datetime64(current_start.date(), 'D')
                end_np = np.datetime64((end_date + timedelta(days=1)).date(), 'D')

                # 计算工作日（默认排除周末） 不包含 end_date 所以前面加1天
                days_diff = np.busday_count(start_np, end_np)

                if freq == 60:  # 1分钟
                    estimated_remaining_bars = days_diff * 600
                elif freq == 300:  # 5分钟
                    estimated_remaining_bars = days_diff * 120
                elif freq == 86400:  # 日线
                    estimated_remaining_bars = days_diff + 7
                else:
                    raise Exception("不支持的周期，仅支持day、min1、min5")

                # 确定批次大小，最大不超过10000
                batch_size = min(10000, int(estimated_remaining_bars))
                if batch_size <= 100:
                    batch_size = 100  # 至少取一个批次

                print(f"[窗口] 计划窗口: {current_start} -> {end_date}，估算剩余: {estimated_remaining_bars} 条，拟取: {batch_size} 条")

                # 根据batch_size计算需要的时间范围（按工作日推进），分钟周期按每天600分钟估算
                if freq == 86400:  # 日线
                    # 日线：每天1条 => 需要的交易日数 = batch_size
                    trading_days_needed = int(np.ceil(batch_size))
                    start_np_d = np.datetime64(current_start.date(), 'D')
                    end_np_d = np.busday_offset(start_np_d, trading_days_needed, roll='following')
                    end_dt = datetime.strptime(np.datetime_as_string(end_np_d), "%Y-%m-%d")
                elif freq == 60:  # 1分钟
                    # 1分钟：每天约600条 => 需要的交易日数 = ceil(batch_size / 600)
                    trading_days_needed = int(np.ceil(batch_size / 250))
                    start_np_d = np.datetime64(current_start.date(), 'D')
                    end_np_d = np.busday_offset(start_np_d, trading_days_needed, roll='following')
                    end_dt = datetime.strptime(np.datetime_as_string(end_np_d), "%Y-%m-%d")
                elif freq == 300:  # 5分钟
                    # 5分钟：每天约120条 => 需要的交易日数 = ceil(batch_size / 120)
                    trading_days_needed = int(np.ceil(batch_size / 45))
                    start_np_d = np.datetime64(current_start.date(), 'D')
                    end_np_d = np.busday_offset(start_np_d, trading_days_needed, roll='following')
                    end_dt = datetime.strptime(np.datetime_as_string(end_np_d), "%Y-%m-%d")
                else:
                    raise Exception("不支持的周期，仅支持day、min1、min5")

                end_dt = datetime.combine(end_dt.date(), current_start.time())
                if end_dt >= end_date:
                    backtest_start = end_date
                else:
                    backtest_start = end_dt
                backtest_end = backtest_start + timedelta(days=8)  # 这里加8天是确保回测范围不会都在非交易范围内

                print(f"[回测] 回测窗口: {backtest_start} -> {backtest_end}，目标条数: {batch_size} (交易日数估算: {trading_days_needed})")

                try:
                    # 使用回测模式获取历史数据
                    with TqApi(
                            account=TqSim(),
                            auth=TqAuth('15600097398', 'wade1010'),
                            backtest=TqBacktest(start_dt=backtest_start, end_dt=backtest_end),
                            disable_print=True
                    ) as api:

                        # 获取K线数据
                        klines = api.get_kline_serial(
                            symbol=code,
                            duration_seconds=freq,
                            data_length=batch_size
                        )

                        # 等待数据更新
                        api.wait_update()

                        # 收集数据
                        current_data = []
                        latest_datetime = None
                        is_first = True
                        is_first_valid = True
                        for i in range(len(klines)):
                            if klines.iloc[i]['datetime'] > 0:  # 有效数据
                                # 转换时间戳（纳秒转秒）
                                timestamp_ns = klines.iloc[i]['datetime']
                                timestamp_s = timestamp_ns / 1000000000
                                trade_datetime = datetime.fromtimestamp(timestamp_s)
                                if is_first:
                                    print(f"[数据] 首条K线时间: {trade_datetime}")
                                    is_first = False
                                    if current_start < trade_datetime:
                                        print(f"[提示] 首条K线时间晚于 current_start: {current_start} -> {trade_datetime}")

                                # 确保在请求的时间范围内
                                if start_date <= trade_datetime <= end_date:
                                    # 去重检查
                                    if trade_datetime in existing_datetimes:
                                        continue
                                    if is_first_valid:
                                        print(f"[数据] 首条有效K线时间: {trade_datetime}")
                                        is_first_valid = False
                                    date_str = trade_datetime.strftime("%Y-%m-%d")
                                    time_str = '00:00:00' if freq == 86400 else trade_datetime.strftime("%H:%M:%S")

                                    bar_data = {
                                        "date": date_str,
                                        "time": time_str,
                                        "open": float(klines.iloc[i]['open']),
                                        "high": float(klines.iloc[i]['high']),
                                        "low": float(klines.iloc[i]['low']),
                                        "close": float(klines.iloc[i]['close']),
                                        "volume": float(klines.iloc[i]['volume']),
                                        "open_interest": float(klines.iloc[i].get('close_oi', 0))
                                    }

                                    existing_datetimes.add(trade_datetime)  # 添加到去重集合
                                    current_data.append(bar_data)

                                    # 跟踪最新的时间点
                                    if latest_datetime is None or trade_datetime > latest_datetime:
                                        latest_datetime = trade_datetime

                        # 如果获取到数据
                        if current_data:
                            # 追加写入到CSV文件
                            print(f"[写入] 追加 {len(current_data)} 条 -> {filepath}")
                            print(f"[范围] 批次首尾: {current_data[0]['date']} {current_data[0]['time']} -> {current_data[-1]['date']} {current_data[-1]['time']}")
                            with open(filepath, "a", encoding="utf-8") as f:
                                for bar in current_data:
                                    items = [
                                        bar["date"],
                                        bar["time"],
                                        str(bar["open"]),
                                        str(bar["high"]),
                                        str(bar["low"]),
                                        str(bar["close"]),
                                        str(bar["volume"]),
                                        str(bar["open_interest"])
                                    ]
                                    f.write(','.join(items) + "\n")

                            # 设置下一次查询的开始时间
                            if latest_datetime:
                                # 根据频率设置下一次查询的开始时间
                                if freq == 60:  # 1分钟
                                    current_start = latest_datetime + timedelta(minutes=1)
                                elif freq == 300:  # 5分钟
                                    current_start = latest_datetime + timedelta(minutes=5)
                                elif freq == 86400:  # 日线
                                    current_start = latest_datetime + timedelta(days=1)

                                print(f"[切换] 下一次查询开始时间 -> {current_start}")
                            else:
                                # 没有获取到最新时间点，可能表示没有更多数据
                                print("[切换] 未获取到数据，本次向后滑动时间窗口")
                                if freq == 60:  # 1分钟
                                    current_start = backtest_end + timedelta(minutes=1)
                                elif freq == 300:  # 5分钟
                                    current_start = backtest_end + timedelta(minutes=5)
                                elif freq == 86400:  # 日线
                                    current_start = backtest_end + timedelta(days=1)
                        else:
                            # 没有数据，向后移动时间窗口
                            print("[切换] 此窗口无数据，向后滑动时间窗口")
                            current_start = backtest_end
                except BacktestFinished:
                    print("[回测] 回测窗口内数据已结束")
                    # 回测结束后，从结束时间后继续
                    current_start = backtest_end

                except Exception as e:
                    print(f"[异常] 获取数据出错: {str(e)}")
                    # 出错后尝试调整窗口重试
                    print(f"[切换] 调整窗口并重试: {current_start} -3天")
                    current_start = current_start - timedelta(days=3)
                    sleep_time.sleep(3)
                    # 上面的情况，获取过去的时间刚好是参加期间就报错，比如下面例子：
                    # with TqApi(
                    #         account=TqSim(),
                    #         auth=TqAuth('xxx', 'xxx'),
                    #         backtest=TqBacktest(start_dt=datetime(2024, 2, 8), end_dt=datetime(2024, 2, 15, 0, 1)),
                    #         disable_print=True
                    # ) as api:
                    #     # 获取K线数据
                    #     klines = api.get_kline_serial(
                    #         symbol=code,
                    #         duration_seconds=freq,
                    #         data_length=10000
                    #     )
                    # 后来发现，回测的开始时间和结束时间在非交易日之间就会报错
                print(f"[状态] current_start = {current_start}\n")
            # 完成一个代码的数据获取
            if existing_datetimes:
                print(f"[完成] {stdCode} 数据收集完成，累计记录: {len(existing_datetimes)} 条")
            else:
                print(f"[完成] {stdCode} 无可收集数据")

    def dmpBarsToDB(self, dbHelper: DBHelper, codes: list, start_date: datetime = None, end_date: datetime = None,
                    period: str = "day"):
        api = TqApi(auth=TqAuth(self.username, self.password))
        if start_date is None:
            start_date = datetime(year=1990, month=1, day=1)

        if end_date is None:
            end_date = datetime.now()
        freq = ''
        if period == 'day':
            freq = 86400
        elif period == 'min5':
            freq = 300
        elif period == "min1":
            freq = 60
        else:
            raise Exception("Unrecognized period")
        count = 0
        length = len(codes)
        for stdCode in codes:
            count += 1
            print(f"Fetching {period} bars of {stdCode}({count}/{length})...")
            code = stdCodeToTQ(stdCode)
            df_bars = api.get_kline_data_series(symbol=code, duration_seconds=freq, start_dt=start_date, end_dt=end_date, adj_type=None)
            exchg = code.split('.')[0]
            rawcode = code.split('.')[-1]
            total_nums = len(df_bars)
            bars = []
            cur_num = 0
            for idx, row in df_bars.iterrows():
                trade_date = datetime.fromtimestamp(row["datetime"] / 1000000000)
                date = int(trade_date.strftime("%Y%m%d"))
                if freq == 86400:
                    time = '0'
                else:
                    time = int(trade_date.strftime("%H%M"))
                curBar = {
                    "exchange": exchg,
                    "code": rawcode,
                    "date": date,
                    "time": time,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                }

                bars.append(curBar)
                cur_num += 1
                if cur_num % 500 == 0:
                    print("Processing bars %d/%d..." % (cur_num, total_nums))

            print("Writing bars into database...")
            dbHelper.writeBars(bars, period)
        api.close()

    def dmpBars(self, codes: list, cb, start_date: datetime = None, end_date: datetime = None, period: str = "day"):
        '''
        将K线导出到指定的目录下的csv文件，文件名格式如SSE.600000_d.csv
        @cb     回调函数，格式如cb(exchg:str, code:str, firstBar:POINTER(WTSBarStruct), count:int, period:str)
        @codes  股票列表，格式如["SSE.600000","SZSE.000001"]
        @start_date 开始日期，datetime类型，传None则自动设置为1990-01-01
        @end_date   结束日期，datetime类型，传None则自动设置为当前日期
        @period K线周期，支持day、min1、min5
        '''
        from tqsdk import TqSim, TqBacktest
        from tqsdk.exceptions import BacktestFinished

        if start_date is None:
            start_date = datetime(year=1990, month=1, day=1)
        if end_date is None:
            end_date = datetime.now()

        # 周期
        if period == 'day':
            freq = 86400
        elif period == 'min5':
            freq = 300
        elif period == 'min1':
            freq = 60
        else:
            raise Exception("不支持的周期，仅支持day、min1、min5")

        count = 0
        length = len(codes)
        for stdCode in codes:
            count += 1
            if length > 1:
                print(f"[任务] 开始获取 {stdCode} 的 {period} K线 ({count}/{length})")
            else:
                print(f"[任务] 开始获取 {stdCode} 的 {period} K线")

            code = stdCodeToTQ(stdCode)
            existing_datetimes = set()
            current_start = start_date

            while current_start < end_date:
                # 计算预计剩余条数，用于确定批次大小
                start_np = np.datetime64(current_start.date(), 'D')
                end_np = np.datetime64((end_date + timedelta(days=1)).date(), 'D')
                days_diff = np.busday_count(start_np, end_np)

                if freq == 60:
                    estimated_remaining_bars = days_diff * 600
                elif freq == 300:
                    estimated_remaining_bars = days_diff * 120
                elif freq == 86400:
                    estimated_remaining_bars = days_diff + 7
                else:
                    raise Exception("不支持的周期，仅支持day、min1、min5")

                batch_size = min(10000, int(estimated_remaining_bars))
                if batch_size <= 100:
                    batch_size = 100

                print(f"[窗口] 计划窗口: {current_start} -> {end_date}，估算剩余: {estimated_remaining_bars} 条，拟取: {batch_size} 条")

                # 按交易日推进窗口
                if freq == 86400:
                    trading_days_needed = int(np.ceil(batch_size))
                    start_np_d = np.datetime64(current_start.date(), 'D')
                    end_np_d = np.busday_offset(start_np_d, trading_days_needed, roll='following')
                    end_dt = datetime.strptime(np.datetime_as_string(end_np_d), "%Y-%m-%d")
                elif freq == 60:
                    trading_days_needed = int(np.ceil(batch_size / 250))
                    start_np_d = np.datetime64(current_start.date(), 'D')
                    end_np_d = np.busday_offset(start_np_d, trading_days_needed, roll='following')
                    end_dt = datetime.strptime(np.datetime_as_string(end_np_d), "%Y-%m-%d")
                elif freq == 300:
                    trading_days_needed = int(np.ceil(batch_size / 45))
                    start_np_d = np.datetime64(current_start.date(), 'D')
                    end_np_d = np.busday_offset(start_np_d, trading_days_needed, roll='following')
                    end_dt = datetime.strptime(np.datetime_as_string(end_np_d), "%Y-%m-%d")
                else:
                    raise Exception("不支持的周期，仅支持day、min1、min5")

                end_dt = datetime.combine(end_dt.date(), current_start.time())
                if end_dt >= end_date:
                    backtest_start = end_date
                else:
                    backtest_start = end_dt
                backtest_end = backtest_start + timedelta(days=8)  # 这里加8天是确保回测范围不会都在非交易范围内

                print(f"[回测] 回测窗口: {backtest_start} -> {backtest_end}，目标条数: {batch_size} (交易日数估算: {trading_days_needed})")

                try:
                    from tqsdk import TqApi
                    with TqApi(
                            account=TqSim(),
                            auth=TqAuth(self.username, self.password),
                            backtest=TqBacktest(start_dt=backtest_start, end_dt=backtest_end),
                            disable_print=True
                    ) as api:
                        klines = api.get_kline_serial(symbol=code, duration_seconds=freq, data_length=batch_size)
                        api.wait_update()

                        # 收集并构造成 WTSBarStruct 缓冲
                        records = []
                        latest_datetime = None
                        for i in range(len(klines)):
                            if klines.iloc[i]['datetime'] > 0:
                                ts_ns = klines.iloc[i]['datetime']
                                ts_s = ts_ns / 1000000000
                                trade_dt = datetime.fromtimestamp(ts_s)
                                if start_date <= trade_dt <= end_date:
                                    if trade_dt in existing_datetimes:
                                        continue
                                    existing_datetimes.add(trade_dt)
                                    records.append(klines.iloc[i])
                                    if latest_datetime is None or trade_dt > latest_datetime:
                                        latest_datetime = trade_dt

                        if records:
                            BUFFER = WTSBarStruct * len(records)
                            buffer = BUFFER()
                            cur_idx = 0
                            for row in records:
                                trade_dt = datetime.fromtimestamp(row['datetime'] / 1000000000)
                                curBar = buffer[cur_idx]
                                curBar.date = int(trade_dt.strftime("%Y%m%d"))
                                if freq == 86400:
                                    curBar.time = 0
                                else:
                                    curBar.time = int(trade_dt.strftime("%H%M")) + (curBar.date - 19900000) * 10000
                                curBar.open = row['open']
                                curBar.high = row['high']
                                curBar.low = row['low']
                                curBar.close = row['close']
                                curBar.vol = row['volume']
                                curBar.hold = row['close_oi']
                                cur_idx += 1

                            ay = stdCode.split(".")
                            cb(ay[0], stdCode, buffer, len(records), period)

                            # 推进起始时间
                            if latest_datetime:
                                if freq == 60:
                                    current_start = latest_datetime + timedelta(minutes=1)
                                elif freq == 300:
                                    current_start = latest_datetime + timedelta(minutes=5)
                                elif freq == 86400:
                                    current_start = latest_datetime + timedelta(days=1)
                                print(f"[切换] 下一次查询开始时间 -> {current_start}")
                            else:
                                # 无最新时间，按窗口滑动
                                if freq == 60:
                                    current_start = backtest_end + timedelta(minutes=1)
                                elif freq == 300:
                                    current_start = backtest_end + timedelta(minutes=5)
                                elif freq == 86400:
                                    current_start = backtest_end + timedelta(days=1)
                        else:
                            # 此窗口无数据，直接滑动
                            current_start = backtest_end
                except BacktestFinished:
                    print("[回测] 回测窗口内数据已结束")
                    current_start = backtest_end
                except Exception as e:
                    print(f"[异常] 获取数据出错: {str(e)}")
                    print(f"[切换] 调整窗口并重试: {current_start} -3天")
                    current_start = current_start - timedelta(days=3)
                    sleep_time.sleep(3)

                print(f"[状态] current_start = {current_start}\n")
