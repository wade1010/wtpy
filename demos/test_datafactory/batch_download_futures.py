#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批量下载期货主连K线数据并保存为dsb格式
通过天勤量化获取所有主连合约，然后批量调用dmpBars下载数据

环境变量配置:
    TQ_USERNAME: 天勤账户用户名
    TQ_PASSWORD: 天勤账户密码
    
使用方法:
    1. 复制 env.example 为 .env
    2. 在 .env 文件中填入真实的天勤账户信息
    3. 或者设置系统环境变量 TQ_USERNAME 和 TQ_PASSWORD
"""

import datetime
import os
import logging
from wtpy.apps.datahelper import DHFactory as DHF
from tqsdk import TqApi, TqAuth
from wtpy.wrapper import WtDataHelper

# 环境变量和配置管理
try:
    from dotenv import load_dotenv

    load_dotenv()  # 加载.env文件
except ImportError:
    logging.warning("未安装python-dotenv包，将直接从系统环境变量读取配置")

# 配置日志
# 删除旧的日志文件
log_file = 'batch_download.log'
if os.path.exists(log_file):
    os.remove(log_file)
    print(f"已删除旧日志文件: {log_file}")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class FuturesBatchDownloader:
    """期货主连K线数据批量下载器"""

    def __init__(self, username: str = None, password: str = None):
        """
        初始化下载器

        Args:
            username: 天勤账号用户名，如果为None则从环境变量TQ_USERNAME读取
            password: 天勤账号密码，如果为None则从环境变量TQ_PASSWORD读取
        """
        # 从环境变量获取配置
        self.username = username or os.getenv('TQ_USERNAME')
        self.password = password or os.getenv('TQ_PASSWORD')

        # 验证配置
        if not self.username:
            raise ValueError("天勤用户名未配置，请设置环境变量TQ_USERNAME或传入username参数")
        if not self.password:
            raise ValueError("天勤密码未配置，请设置环境变量TQ_PASSWORD或传入password参数")

        logging.info(f"使用天勤账号: {self.username}")

        self.hlper = None
        self.futures_list = {}

        # 支持的交易所
        self.exchanges = {
            "SHFE": "上海期货交易所",
            "CFFEX": "中国金融期货交易所",
            "DCE": "大连商品交易所",
            "CZCE": "郑州商品交易所",
            "INE": "上海国际能源交易中心",
            "GFEX": "广州期货交易所"
        }

        # 支持的K线周期
        self.periods = ["min1", "min5", "day"]

    def init_helper(self):
        """初始化数据助手"""
        try:
            self.hlper = DHF.createHelper("tqsdk")
            self.hlper.auth(username=self.username, password=self.password)
            logging.info("天勤SDK数据助手初始化成功")
            return True
        except Exception as e:
            logging.error(f"初始化数据助手失败: {e}")
            return False

    def get_futures_cont_list(self):
        """
        获取所有期货主连合约列表

        Returns:
            dict: 按交易所分类的期货主连合约信息
        """
        try:
            api = TqApi(auth=TqAuth(self.username, self.password))

            futures = {
                "SHFE": {},
                "CFFEX": {},
                "DCE": {},
                "CZCE": {},
                "INE": {},
                "GFEX": {}
            }

            logging.info("开始获取期货主连合约列表...")

            try:
                # 一次性获取所有主连合约，不指定exchange_id
                logging.info("正在获取所有期货主连合约...")
                code_list = api.query_quotes(ins_class="CONT")
                code_list_info = api.query_symbol_info(code_list)

                # 按交易所分类处理
                for idx, row in code_list_info.iterrows():
                    underlying_symbol = row["underlying_symbol"]
                    exchange = underlying_symbol.split('.')[0]  # 从instrument_id中提取交易所

                    # 只处理我们支持的交易所
                    if exchange in futures:
                        instrument_id = row["instrument_id"]
                        fInfo = dict()
                        rawcode = instrument_id.split('.')[-1]
                        fInfo["exchg"] = exchange
                        fInfo["code"] = rawcode
                        fInfo["trading_time_day"] = row["trading_time_day"]
                        fInfo["trading_time_night"] = row["trading_time_night"]
                        fInfo["name"] = row["instrument_name"]
                        fInfo["full_code"] = f"{exchange}.{rawcode}.HOT"
                        futures[exchange][rawcode] = fInfo

                # 输出各交易所统计信息
                for exchange in futures.keys():
                    count = len(futures[exchange])
                    if count > 0:
                        logging.info(f"{self.exchanges.get(exchange, exchange)}: 获取到 {count} 个主连合约")

            except Exception as e:
                logging.error(f"获取主连合约失败: {e}")

            api.close()
            self.futures_list = futures

            # 统计总数
            total_count = sum(len(contracts) for contracts in futures.values())
            logging.info(f"总共获取到 {total_count} 个期货主连合约")

            return futures

        except Exception as e:
            logging.error(f"获取期货主连合约列表失败: {e}")
            return {}

    def should_update_data(self, last_time: datetime.datetime, trading_time_day: list, trading_time_night: list, period: str = "min1") -> bool:
        """
        判断是否需要更新K线数据
        
        Args:
            last_time: DSB文件中最后一条数据的时间
            trading_time_day: 日盘交易时间段，格式：[['09:00:00', '10:15:00'], ['10:30:00', '11:30:00'], ...]
            trading_time_night: 夜盘交易时间段，格式：[['21:00:00', '23:00:00']] 或 [['21:00:00', '25:00:00']]
            period: K线周期，用于计算末尾时间偏移
        
        Returns:
            bool: True表示需要更新，False表示不需要更新
        """
        try:
            now = datetime.datetime.now()
            current_time = now.time()

            # 1. 如果当前时间在交易时间内，都需要更新
            if self._is_in_trading_time(current_time, trading_time_day, trading_time_night):
                logging.info(f"当前时间 {current_time} 在交易时间内，需要更新数据")
                return True

            # 2. 非交易时间段的判断
            # 获取最后一个交易时间点
            last_trading_time = self._get_last_trading_time(trading_time_day, trading_time_night)

            # 如果DSB文件中的最后时间早于最后一个交易时间点，需要更新
            if last_time < last_trading_time:
                logging.info(f"DSB最后时间 {last_time} 早于最后交易时间 {last_trading_time}，需要更新数据+++++++++++++++++++++++++++")
                return True
            else:
                logging.info(f"DSB最后时间 {last_time} 不早于最后交易时间 {last_trading_time}，无需更新数据--------------------")
                return False

        except Exception as e:
            logging.error(f"判断是否需要更新数据时出错: {e}")
            # 出错时默认需要更新
            return True

    def _is_in_trading_time(self, current_time: datetime.time, trading_time_day: list, trading_time_night: list) -> bool:
        """
        判断当前时间是否在交易时间内
        """
        # 获取当前日期和星期
        now = datetime.datetime.now()
        weekday = now.weekday()  # 0=周一, 1=周二, ..., 6=周日

        # 周末交易时间判断
        if trading_time_night is None or len(trading_time_night) == 0:
            # 没有夜盘，周末都不在交易时间内
            if weekday == 5 or weekday == 6:  # 周六或周日
                return False
        else:
            # 有夜盘的情况
            if weekday == 5:  # 周六
                # 周六2:30之后不在交易时间内
                if current_time >= datetime.time(2, 30, 0):
                    return False
            elif weekday == 6:  # 周日
                # 周日都不在交易时间内
                return False
        # 检查日盘时间
        for time_range in trading_time_day:
            start_time = datetime.time.fromisoformat(time_range[0])
            end_time = datetime.time.fromisoformat(time_range[1])
            if start_time <= current_time <= end_time:
                return True

        # 检查夜盘时间
        if trading_time_night:
            for time_range in trading_time_night:
                start_time = datetime.time.fromisoformat(time_range[0])
                end_time_str = time_range[1]

                # 处理跨日情况
                if end_time_str.startswith('2'):
                    hour = int(end_time_str.split(':')[0])
                    if hour >= 24:
                        # 跨日时间
                        actual_hour = hour - 24
                        minute = int(end_time_str.split(':')[1])
                        second = int(end_time_str.split(':')[2])

                        # 检查是否在夜盘时间内（跨日）
                        if current_time >= start_time or current_time <= datetime.time(actual_hour, minute, second):
                            return True
                    else:
                        # 不跨日的夜盘时间
                        end_time = datetime.time.fromisoformat(end_time_str)
                        if start_time <= current_time <= end_time:
                            return True
                else:
                    # 正常的夜盘时间
                    end_time = datetime.time.fromisoformat(end_time_str)
                    if start_time <= current_time <= end_time:
                        return True

        return False

    def get_dsb_last_time(self, full_code: str, period: str = "min1"):
        """
        获取dsb文件中的最后时间

        Args:
            full_code: 完整合约代码，如 "DCE.jm.HOT"
            period: K线周期，支持 min1, min5, day

        Returns:
            datetime.datetime or None: 最后时间，如果文件不存在或读取失败返回None
        """
        try:
            # 构造dsb文件路径
            exchg = full_code.split('.')[0]
            code_part = full_code.split('.')[1] + "_HOT"  # 转换为dsb文件命名格式
            dsb_file = f"../storage/his/{period}/{exchg}/{exchg}.{code_part}.dsb"
            # 检查文件是否存在
            if not os.path.exists(dsb_file):
                logging.info(f"DSB文件不存在: {dsb_file}")
                return None

            # 读取dsb文件
            dtHelper = WtDataHelper()
            kline_data = dtHelper.read_dsb_bars(dsb_file, period == "day")

            if kline_data is None or len(kline_data) == 0:
                logging.info(f"DSB文件无数据: {dsb_file}")
                return None

            # 获取最后一条数据的时间
            # 使用bartimes字段，格式为YYYYMMDDHHMM
            last_bartime = kline_data.bartimes[-1]

            if period == "day":
                # 日线数据，只取日期部分YYYYMMDD
                date_str = str(last_bartime)[:8]
                last_time = datetime.datetime.strptime(date_str, "%Y%m%d")
            else:
                # 分钟线数据，格式为YYYYMMDDHHMM
                time_str = str(last_bartime)
                last_time = datetime.datetime.strptime(time_str, "%Y%m%d%H%M")

            logging.info(f"DSB文件 {dsb_file} 最后时间: {last_time}")
            return last_time

        except Exception as e:
            logging.error(f"读取DSB文件最后时间失败 {full_code}: {e}")
            return None

    def _get_last_trading_time(self, trading_time_day: list, trading_time_night: list) -> datetime.datetime:
        """
        获取最后一个交易时间点，包含日期
        
        Args:
            trading_time_day: 日盘交易时间段
            trading_time_night: 夜盘交易时间段
            
        Returns:
            datetime.datetime: 最后一个交易时间点
        """
        now = datetime.datetime.now()
        # now = datetime.datetime(2025, 10, 14, 3, 52, 34)
        current_time = now.time()
        weekday = now.weekday()  # 0=周一, 1=周二, ..., 6=周日

        # 周末情况处理
        if weekday == 5 or weekday == 6:  # 周六或周日
            if trading_time_night and len(trading_time_night) > 0:
                # 检查是否有大于24小时的夜盘时间
                for time_range in trading_time_night:
                    end_time_str = time_range[1]
                    if end_time_str.startswith('2'):
                        hour = int(end_time_str.split(':')[0])
                        if hour >= 24:
                            # 有大于24的情况，最后交易时间点是本周所在周六的该时间段末尾
                            actual_hour = hour - 24
                            minute = int(end_time_str.split(':')[1])
                            second = int(end_time_str.split(':')[2])
                            # 计算本周的周六日期
                            days_until_saturday = (5 - now.weekday()) % 7
                            if now.weekday() == 6:  # 如果今天是周日，周六是昨天
                                days_until_saturday = -1
                            saturday = now.date() + datetime.timedelta(days=days_until_saturday)
                            return datetime.datetime.combine(saturday, datetime.time(actual_hour, minute, second))

            # 其他周末情况，最后交易时间是本周周五的最后一个时间段末尾
            # 计算本周的周五日期
            days_until_friday = (4 - now.weekday()) % 7
            if now.weekday() == 5:  # 如果今天是周六，周五是昨天
                days_until_friday = -1
            elif now.weekday() == 6:  # 如果今天是周日，周五是前天
                days_until_friday = -2
            friday = now.date() + datetime.timedelta(days=days_until_friday)

            # 找到最后一个时间段的末尾
            all_times = trading_time_day
            if trading_time_night:
                all_times.extend(trading_time_night)
            last_time_range = all_times[-1]
            end_time_str = last_time_range[1]
            end_time = datetime.time.fromisoformat(end_time_str)
            return datetime.datetime.combine(friday, end_time)

        # 非周末情况
        else:
            # 合并所有交易时间段
            all_times = trading_time_day
            if trading_time_night:
                all_times.extend(trading_time_night)

            # 找到当前时间在哪两个时间段之间
            for i in range(len(all_times) - 1):
                current_end = datetime.time.fromisoformat(all_times[i][1])
                next_start = datetime.time.fromisoformat(all_times[i + 1][0])
                # 正常情况：当前时间在两个时间段之间
                if current_end <= current_time <= next_start:
                    return datetime.datetime.combine(now.date(), current_end)

            # 如果没有找到合适的间隔，返回最后一个时间段的末尾
            last_time_range = all_times[-1]
            end_time_str = last_time_range[1]

            hour = int(end_time_str.split(':')[0])
            minute = int(end_time_str.split(':')[1])
            second = int(end_time_str.split(':')[2])
            if hour <= current_time.hour < 24:
                # 在范围内，使用当前日期
                return datetime.datetime.combine(now.date(), datetime.time(hour, minute, second))
            else:
                # 不在范围内，使用前一天日期
                return datetime.datetime.combine(now.date() - datetime.timedelta(days=1), datetime.time(hour, minute, second))

    def download_single_contract(self, full_code: str, start_date: datetime.datetime,
                                 end_date: datetime.datetime, period: str = "min1",
                                 is_incremental: bool = False):
        """
        下载单个合约的K线数据

        Args:
            full_code: 完整合约代码，如 "DCE.jm.HOT"
            start_date: 开始日期
            end_date: 结束日期
            period: K线周期，支持 min1, min5, day
            is_incremental: 是否增量下载，如果为True则从dsb文件的最后时间开始下载

        Returns:
            bool: 下载是否成功
        """
        try:
            actual_start_date = start_date

            # 如果是增量下载，尝试从dsb文件获取最后时间
            if is_incremental:
                code_info_array = full_code.split('.')
                trading_time_day = self.futures_list.get(code_info_array[0]).get(code_info_array[1])['trading_time_day']
                trading_time_night = self.futures_list.get(code_info_array[0]).get(code_info_array[1])['trading_time_night']
                last_time = self.get_dsb_last_time(full_code, period)

                if last_time is not None:
                    logging.info(f"DSB文件最后时间: {last_time}")

                    # 判断是否需要更新数据
                    need_update = self.should_update_data(last_time, trading_time_day, trading_time_night, period)

                    if not need_update:
                        logging.info(f"{full_code} {period} 当前数据已是最新，跳过下载")
                        return True

                    logging.info(f"实际开始时间已从增量点回溯10天, from {last_time} to {last_time - datetime.timedelta(days=10)}")
                    actual_start_date = last_time - datetime.timedelta(days=10)
                    # 如果计算出的开始时间已经超过结束时间，说明数据已经是最新的
                    if actual_start_date > end_date:
                        logging.info(f"{full_code} {period} 数据已经是最新的，无需下载")
                        return True

                    logging.info(f"增量下载 {full_code} {period} 数据，从 {actual_start_date} 开始")
                else:
                    logging.info(f"未找到 {full_code} {period} 的历史数据，执行全量下载")

            logging.info(f"开始下载 {full_code} {period} 数据，时间范围: {actual_start_date} 到 {end_date}")

            # 调用dmpBars下载数据，cb=None表示使用内置的cb_store_bar_to_dsb处理器
            self.hlper.dmpBars(
                codes=[full_code],
                cb=None,
                start_date=actual_start_date,
                end_date=end_date,
                period=period
            )

            logging.info(f"成功下载 {full_code} {period} 数据")
            return True

        except Exception as e:
            logging.error(f"下载 {full_code} {period} 数据失败: {e}")
            return False

    def batch_download_all_contracts(self, start_date: datetime.datetime,
                                     end_date: datetime.datetime,
                                     periods: list = None,
                                     exchanges: list = None,
                                     is_incremental: bool = False):
        """
        批量下载所有主连合约的K线数据

        Args:
            start_date: 开始日期
            end_date: 结束日期
            periods: K线周期列表，默认为 ["min1", "min5", "day"]
            exchanges: 交易所列表，默认为所有交易所
            is_incremental: 是否增量下载，如果为True则从dsb文件的最后时间开始下载
        """
        if not self.hlper:
            logging.error("数据助手未初始化，请先调用init_helper()")
            return

        if not self.futures_list:
            logging.info("期货合约列表为空，开始获取...")
            self.get_futures_cont_list()

        if periods is None:
            periods = self.periods

        if exchanges is None:
            exchanges = list(self.exchanges.keys())

        # 先计算总任务数
        total_tasks = 0
        for exchange in exchanges:
            if exchange in self.futures_list:
                total_tasks += len(self.futures_list[exchange]) * len(periods)

        if total_tasks == 0:
            logging.warning("没有找到需要下载的合约")
            return

        # 统计信息
        success_count = 0
        failed_count = 0
        current_task = 0

        logging.info("=" * 60)
        logging.info("开始批量下载所有合约")
        logging.info(f"周期: {periods}")
        logging.info(f"交易所: {[self.exchanges.get(ex, ex) for ex in exchanges]}")
        if is_incremental:
            logging.info("增量更新模式开启，将结束时间设置为当前时间（精确到分钟）")
            end_date = datetime.datetime.now().replace(second=0, microsecond=0)
        logging.info(f"时间范围: {start_date} 到 {end_date}")
        logging.info(f"总任务数: {total_tasks}")
        logging.info("=" * 60)

        for exchange_idx, exchange in enumerate(exchanges, 1):
            if exchange not in self.futures_list:
                logging.warning(f"交易所 {exchange} 不在合约列表中，跳过")
                continue

            contracts = self.futures_list[exchange]
            exchange_name = self.exchanges.get(exchange, exchange)

            logging.info(f"\n[{exchange_idx}/{len(exchanges)}] 开始处理 {exchange_name}")
            logging.info(f"该交易所共有 {len(contracts)} 个合约，{len(periods)} 个周期")

            for contract_idx, (contract_code, contract_info) in enumerate(contracts.items(), 1):
                full_code = contract_info["full_code"]
                contract_name = contract_info["name"]

                logging.info(f"  [{contract_idx}/{len(contracts)}] 合约: {full_code} ({contract_name})")

                for period_idx, period in enumerate(periods, 1):
                    current_task += 1

                    logging.info(f"    [{period_idx}/{len(periods)}] 下载 {period} 数据 - 进度: {current_task}/{total_tasks} ({current_task / total_tasks * 100:.1f}%)")

                    try:
                        # 下载数据
                        success = self.download_single_contract(
                            full_code, start_date, end_date, period, is_incremental
                        )

                        if success:
                            success_count += 1
                            logging.info(f"    ✓ {full_code} {period} 下载成功 [成功:{success_count} 失败:{failed_count}]")
                        else:
                            failed_count += 1
                            logging.warning(f"    ✗ {full_code} {period} 下载失败 [成功:{success_count} 失败:{failed_count}]")

                    except Exception as e:
                        failed_count += 1
                        logging.error(f"    ✗ {full_code} {period} 发生异常: {e} [成功:{success_count} 失败:{failed_count}]")
                        continue

                # 每个合约完成后显示阶段性统计
                if len(periods) > 1:
                    logging.info(f"  合约 {full_code} 完成，当前总体进度: {current_task}/{total_tasks} ({current_task / total_tasks * 100:.1f}%)")

        # 输出最终统计结果
        logging.info("\n" + "=" * 60)
        logging.info("批量下载完成!")
        logging.info(f"总任务数: {total_tasks}")
        logging.info(f"成功: {success_count} ({success_count / total_tasks * 100:.1f}%)")
        logging.info(f"失败: {failed_count} ({failed_count / total_tasks * 100:.1f}%)")
        if failed_count > 0:
            logging.warning(f"有 {failed_count} 个任务失败，请检查上述错误日志")
        else:
            logging.info("所有任务都成功完成！")
        logging.info("=" * 60)

    def download_specific_contracts(self, contract_codes: list,
                                    start_date: datetime.datetime,
                                    end_date: datetime.datetime,
                                    periods: list = None,
                                    is_incremental: bool = False):
        """
        下载指定合约的K线数据

        Args:
            contract_codes: 合约代码列表，如 ["DCE.jm.HOT", "CFFEX.IF.HOT"]
            start_date: 开始日期
            end_date: 结束日期
            periods: K线周期列表
            is_incremental: 是否增量下载，如果为True则从dsb文件的最后时间开始下载
        """
        if not self.hlper:
            logging.error("数据助手未初始化，请先调用init_helper()")
            return

        if periods is None:
            periods = self.periods

        success_count = 0
        failed_count = 0
        total_tasks = len(contract_codes) * len(periods)
        current_task = 0

        logging.info("=" * 60)
        logging.info("开始下载指定合约")
        logging.info(f"合约列表: {contract_codes}")
        logging.info(f"周期: {periods}")
        logging.info(f"时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
        logging.info(f"总任务数: {total_tasks}")
        logging.info("=" * 60)

        for contract_idx, contract_code in enumerate(contract_codes, 1):
            logging.info(f"\n[{contract_idx}/{len(contract_codes)}] 处理合约: {contract_code}")

            for period_idx, period in enumerate(periods, 1):
                current_task += 1

                logging.info(f"  [{period_idx}/{len(periods)}] 下载 {period} 数据 - 进度: {current_task}/{total_tasks} ({current_task / total_tasks * 100:.1f}%)")

                try:
                    success = self.download_single_contract(
                        contract_code, start_date, end_date, period, is_incremental
                    )

                    if success:
                        success_count += 1
                        logging.info(f"  ✓ {contract_code} {period} 下载成功 [成功:{success_count} 失败:{failed_count}]")
                    else:
                        failed_count += 1
                        logging.warning(f"  ✗ {contract_code} {period} 下载失败 [成功:{success_count} 失败:{failed_count}]")

                except Exception as e:
                    failed_count += 1
                    logging.error(f"  ✗ {contract_code} {period} 发生异常: {e} [成功:{success_count} 失败:{failed_count}]")
                    continue

            # 每个合约完成后显示阶段性统计
            if len(periods) > 1:
                logging.info(f"合约 {contract_code} 完成，当前总体进度: {current_task}/{total_tasks} ({current_task / total_tasks * 100:.1f}%)")

        # 输出最终统计结果
        logging.info("\n" + "=" * 60)
        logging.info("指定合约下载完成!")
        logging.info(f"总任务数: {total_tasks}")
        logging.info(f"成功: {success_count} ({success_count / total_tasks * 100:.1f}%)")
        logging.info(f"失败: {failed_count} ({failed_count / total_tasks * 100:.1f}%)")
        if failed_count > 0:
            logging.warning(f"有 {failed_count} 个任务失败，请检查上述错误日志")
        else:
            logging.info("所有任务都成功完成！")
        logging.info("=" * 60)


def main():
    """主函数示例"""
    try:
        # 创建下载器（自动从环境变量读取账号信息）
        downloader = FuturesBatchDownloader()

        # 初始化
        if not downloader.init_helper():
            logging.error("初始化失败，退出程序")
            return

    except ValueError as e:
        logging.error(f"配置错误: {e}")
        logging.error("请确保已设置环境变量TQ_USERNAME和TQ_PASSWORD，或创建.env文件")
        return
    except Exception as e:
        logging.error(f"初始化下载器失败: {e}")
        return

    # 设置下载参数
    start_date = datetime.datetime(2016, 1, 1)
    end_date = datetime.datetime(2025, 10, 11)
    periods = ["min1", "min5", "day"]  # 可以根据需要调整

    # 方式1: 下载所有主连合约
    downloader.batch_download_all_contracts(
        start_date=start_date,
        end_date=end_date,
        periods=periods,
        # exchanges=["DCE", "CFFEX"],  # 可以指定特定交易所
        is_incremental=True
    )

    # 方式2: 下载指定合约
    # specific_contracts = [
    #     "DCE.jm.HOT",
    #     "CFFEX.IF.HOT",
    #     "CFFEX.IC.HOT",
    #     "SHFE.rb.HOT"
    # ]
    #
    # downloader.download_specific_contracts(
    #     contract_codes=specific_contracts,
    #     start_date=start_date,
    #     end_date=end_date,
    #     periods=periods
    # )


if __name__ == "__main__":
    main()
