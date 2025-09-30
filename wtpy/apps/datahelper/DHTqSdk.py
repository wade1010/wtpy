from wtpy.apps.datahelper.DHDefs import BaseDataHelper, DBHelper
from wtpy.WtCoreDefs import WTSBarStruct
from tqsdk import TqApi, TqAuth
from datetime import datetime, timedelta
import json
import os
import logging


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
        logging.info("TqSdk helper has been created.")
        return

    def auth(self, **kwargs):
        if self.isAuthed:
            return
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        api = TqApi(auth=TqAuth(kwargs["username"], kwargs["password"]))
        self.isAuthed = True
        api.close()
        logging.info("TqSdk has been authorized.")

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
            logging.info("Fetching stock list...")
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
            logging.info("Fetching Index list...")
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
        logging.info("Writing code list into file %s..." % (filename))
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
            logging.info("Fetching %s bars of %s(%d/%s)..." % (period, stdCode, count, length))
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
                logging.info("Writing bars into file %s..." % (filepath))
                f = open(filepath, "w", encoding="utf-8")
                f.write(content)
                f.close()
        api.close()

    def dmpBarsToFile(self, folder: str, codes: list, start_date: datetime = None, end_date: datetime = None, period="day"):
        """
        改造成回测模式，支持day、min1、min5
        每获取一批数据就追加到CSV文件中
        从前往后获取数据，保证数据顺序正确，适合追加模式写入
        """
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
            logging.info("Fetching %s bars of %s(%d/%s)..." % (period, stdCode, count, length))
            code = stdCodeToTQ(stdCode)

            # 准备文件路径
            filename = "%s_%s.csv" % (stdCode, filetag)
            filepath = os.path.join(folder, filename)

            # 创建文件并写入表头
            if not os.path.exists(filepath):
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write("date,time,open,high,low,close,volume,open_interest\n")

            # 已存在的日期时间集合，用于去重
            existing_datetimes = set()

            # 当前开始时间，从用户指定的起始时间开始
            current_start = start_date

            while current_start < end_date:
                # 计算预估的数据总条数
                days_diff = (end_date - current_start).days

                if freq == 60:  # 1分钟
                    estimated_remaining_bars = days_diff * 600
                elif freq == 300:  # 5分钟
                    estimated_remaining_bars = days_diff * 120,
                elif freq == 86400:  # 日线
                    estimated_remaining_bars = days_diff + 7
                else:
                    raise Exception("不支持的周期，仅支持day、min1、min5")

                # 确定批次大小，最大不超过10000
                batch_size = min(10000, int(estimated_remaining_bars))
                if batch_size <= 0:
                    batch_size = 10000  # 至少取一个批次

                logging.info(f"从 {current_start} 到 {end_date} 预计剩余数据量: {estimated_remaining_bars} 条，本次获取 {batch_size} 条")

                if batch_size < 10000:
                    backtest_start = end_date
                    backtest_end = end_date + timedelta(minutes=1)
                else:
                    if freq == 86400:  # 日线
                        backtest_start = current_start + timedelta(days=int(batch_size / 0.72))  # 一周最起码有2天不交易 5/7约等于0.72
                    else:  # 分钟
                        backtest_start = current_start + timedelta(minutes=int(batch_size / (0.72 * 0.5)))  # 一般不全是交易时间，国内一天不会超过11个小时
                    backtest_end = backtest_start + timedelta(minutes=1)

                # 确保不超过用户指定的结束时间
                if backtest_end > end_date:
                    backtest_end = end_date

                logging.info(f"获取{backtest_end}之前的{batch_size}条数据...")

                try:
                    # 使用回测模式获取历史数据
                    api = TqApi(
                        account=TqSim(),
                        auth=TqAuth(self.username, self.password),
                        backtest=TqBacktest(start_dt=backtest_start, end_dt=backtest_end)
                    )

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

                    for i in range(len(klines)):
                        if klines.iloc[i]['datetime'] > 0:  # 有效数据
                            # 转换时间戳（纳秒转秒）
                            timestamp_ns = klines.iloc[i]['datetime']
                            timestamp_s = timestamp_ns / 1000000000
                            trade_date = datetime.fromtimestamp(timestamp_s)

                            # 确保在请求的时间范围内
                            if start_date <= trade_date <= end_date:
                                # 去重检查
                                if trade_date in existing_datetimes:
                                    continue

                                date_str = trade_date.strftime("%Y-%m-%d")
                                time_str = '0' if freq == 86400 else trade_date.strftime("%H:%M:%S")

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

                                existing_datetimes.add(trade_date)  # 添加到去重集合
                                current_data.append(bar_data)

                                # 跟踪最新的时间点
                                if latest_datetime is None or trade_date > latest_datetime:
                                    latest_datetime = trade_date

                    # 关闭API连接
                    api.close()

                    # 如果获取到数据
                    if current_data:
                        # 按日期时间排序当前批次的数据
                        current_data.sort(key=lambda x: (x["date"], x["time"]))

                        # 追加写入到CSV文件
                        logging.info("追加 %d 条数据到文件 %s..." % (len(current_data), filepath))
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

                            logging.info(f"下一次查询开始时间设置为: {current_start}")
                        else:
                            # 没有获取到最新时间点，可能表示没有更多数据
                            logging.info("没有获取到数据，尝试向后移动时间窗口")
                            if freq == 60:  # 1分钟
                                current_start = backtest_end + timedelta(minutes=1)
                            elif freq == 300:  # 5分钟
                                current_start = backtest_end + timedelta(minutes=5)
                            elif freq == 86400:  # 日线
                                current_start = backtest_end + timedelta(days=1)
                    else:
                        # 没有数据，向后移动时间窗口
                        logging.info("此时间段没有数据，向后移动时间窗口")
                        current_start = backtest_end

                except BacktestFinished:
                    logging.info("回测完成")
                    # 回测结束后，从结束时间后继续
                    current_start = backtest_end

                except Exception as e:
                    logging.error(f"获取数据时出错: {str(e)}")
                    api.close()
                    # 出错后尝试向后移动时间窗口
                    current_start = backtest_end

            # 完成一个代码的数据获取
            if existing_datetimes:
                logging.info("完成 %s 的数据收集，总记录数: %d" % (stdCode, len(existing_datetimes)))
            else:
                logging.warning("没有为 %s 收集到任何数据" % stdCode)

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
            logging.info("Fetching %s bars of %s(%d/%s)..." % (period, stdCode, count, length))
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
                    logging.info("Processing bars %d/%d..." % (cur_num, total_nums))

            logging.info("Writing bars into database...")
            dbHelper.writeBars(bars, period)
        api.close()

    def dmpBars(self, codes: list, cb, start_date: datetime = None, end_date: datetime = None, period: str = "day"):
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
        elif isinstance(period, int):
            if (0 < period <= 86400) or period % 86400 == 0:
                freq = period
            else:
                raise Exception("Unrecognized period")
        else:
            raise Exception("Unrecognized period")
        count = 0
        length = len(codes)
        for stdCode in codes:
            count += 1
            logging.info("Fetching %s bars of %s(%d/%s)..." % (period, stdCode, count, length))
            code = stdCodeToTQ(stdCode)
            try:
                df_bars = api.get_kline_data_series(symbol=code, duration_seconds=freq, start_dt=start_date, end_dt=end_date, adj_type=None)
            except Exception as e:
                api.close()
                raise Exception(f"{e}")
            total_nums = len(df_bars)
            BUFFER = WTSBarStruct * len(df_bars)
            buffer = BUFFER()
            cur_num = 0
            for idx, row in df_bars.iterrows():
                curBar = buffer[cur_num]
                trade_date = datetime.fromtimestamp(row["datetime"] / 1000000000)
                curBar.date = int(trade_date.strftime("%Y%m%d"))
                if period == 'day':
                    curBar.time = 0
                else:
                    curBar.time = int(trade_date.strftime("%H%M")) + (curBar.date - 19900000) * 10000
                curBar.open = row["open"]
                curBar.high = row["high"]
                curBar.low = row["low"]
                curBar.close = row["close"]
                curBar.vol = row["volume"]
                # curBar.money = None
                # if "open_interest" in row:
                #     curBar.hold = row["open_interest"]
                cur_num += 1
                if cur_num % 500 == 0:
                    logging.info("Processing bars %d/%d..." % (cur_num, total_nums))
            ay = stdCode.split(".")
            cb(ay[0], stdCode, buffer, total_nums, period)
        api.close()
