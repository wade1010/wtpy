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

# 环境变量和配置管理
try:
    from dotenv import load_dotenv

    load_dotenv()  # 加载.env文件
except ImportError:
    logging.warning("未安装python-dotenv包，将直接从系统环境变量读取配置")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_download.log', encoding='utf-8'),
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

    def download_single_contract(self, full_code: str, start_date: datetime.datetime,
                                 end_date: datetime.datetime, period: str = "min1"):
        """
        下载单个合约的K线数据

        Args:
            full_code: 完整合约代码，如 "DCE.jm.HOT"
            start_date: 开始日期
            end_date: 结束日期
            period: K线周期，支持 min1, min5, day

        Returns:
            bool: 下载是否成功
        """
        try:
            logging.info(f"开始下载 {full_code} {period} 数据，时间范围: {start_date} 到 {end_date}")

            # 调用dmpBars下载数据，cb=None表示使用内置的cb_store_bar_to_dsb处理器
            self.hlper.dmpBars(
                codes=[full_code],
                cb=None,
                start_date=start_date,
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
                                     exchanges: list = None):
        """
        批量下载所有主连合约的K线数据

        Args:
            start_date: 开始日期
            end_date: 结束日期
            periods: K线周期列表，默认为 ["min1", "min5", "day"]
            exchanges: 交易所列表，默认为所有交易所
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

        # 统计信息
        total_contracts = 0
        success_count = 0
        failed_count = 0

        logging.info(f"开始批量下载，周期: {periods}, 交易所: {exchanges}")
        logging.info(f"时间范围: {start_date} 到 {end_date}")

        for exchange in exchanges:
            if exchange not in self.futures_list:
                logging.warning(f"交易所 {exchange} 不在合约列表中，跳过")
                continue

            contracts = self.futures_list[exchange]
            logging.info(f"开始处理 {self.exchanges.get(exchange, exchange)} 的 {len(contracts)} 个合约")

            for contract_code, contract_info in contracts.items():
                full_code = contract_info["full_code"]
                contract_name = contract_info["name"]

                logging.info(f"处理合约: {full_code} ({contract_name})")

                for period in periods:
                    total_contracts += 1

                    try:
                        # 下载数据
                        success = self.download_single_contract(  # ALL
                            full_code, start_date, end_date, period
                        )

                        if success:
                            success_count += 1
                        else:
                            failed_count += 1

                    except Exception as e:
                        logging.error(f"处理 {full_code} {period} 时发生异常: {e}")
                        failed_count += 1
                        continue

        # 输出统计结果
        logging.info("=" * 50)
        logging.info("批量下载完成!")
        logging.info(f"总任务数: {total_contracts}")
        logging.info(f"成功: {success_count}")
        logging.info(f"失败: {failed_count}")
        logging.info(f"成功率: {success_count / total_contracts * 100:.2f}%" if total_contracts > 0 else "0%")
        logging.info("=" * 50)

    def download_specific_contracts(self, contract_codes: list,
                                    start_date: datetime.datetime,
                                    end_date: datetime.datetime,
                                    periods: list = None):
        """
        下载指定合约的K线数据

        Args:
            contract_codes: 合约代码列表，如 ["DCE.jm.HOT", "CFFEX.IF.HOT"]
            start_date: 开始日期
            end_date: 结束日期
            periods: K线周期列表
        """
        if not self.hlper:
            logging.error("数据助手未初始化，请先调用init_helper()")
            return

        if periods is None:
            periods = self.periods

        success_count = 0
        failed_count = 0
        total_tasks = len(contract_codes) * len(periods)

        logging.info(f"开始下载指定合约，共 {len(contract_codes)} 个合约，{len(periods)} 个周期")
        logging.info(f"时间范围: {start_date} 到 {end_date}")

        for contract_code in contract_codes:
            logging.info(f"处理合约: {contract_code}")

            for period in periods:
                try:
                    success = self.download_single_contract(
                        contract_code, start_date, end_date, period
                    )

                    if success:
                        success_count += 1
                    else:
                        failed_count += 1

                except Exception as e:
                    logging.error(f"处理 {contract_code} {period} 时发生异常: {e}")
                    failed_count += 1
                    continue

        # 输出统计结果
        logging.info("=" * 50)
        logging.info("指定合约下载完成!")
        logging.info(f"总任务数: {total_tasks}")
        logging.info(f"成功: {success_count}")
        logging.info(f"失败: {failed_count}")
        logging.info(f"成功率: {success_count / total_tasks * 100:.2f}%" if total_tasks > 0 else "0%")
        logging.info("=" * 50)


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
