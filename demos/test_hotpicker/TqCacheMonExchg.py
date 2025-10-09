#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
天勤量化主力合约缓存监控和更新工具
用于更新hots.json文件中的主力合约信息
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from pathlib import Path

try:
    from tqsdk import TqApi, TqAuth
    import pandas as pd
    from dotenv import load_dotenv
except ImportError as e:
    print(f"请安装必要的依赖包: pip install tqsdk pandas python-dotenv")
    print(f"错误详情: {e}")
    sys.exit(1)

# 加载环境变量
load_dotenv()


class TqHotContractUpdater:
    """天勤主力合约更新器"""

    def __init__(self, username: str = None, password: str = None, hots_file_path: str = None):
        """
        初始化更新器
        
        Args:
            username: 天勤账户用户名（从环境变量TQ_USERNAME获取）
            password: 天勤账户密码（从环境变量TQ_PASSWORD获取）
            hots_file_path: hots.json文件路径
        """
        self.username = username or os.getenv('TQ_USERNAME')
        self.password = password or os.getenv('TQ_PASSWORD')

        if not self.username or not self.password:
            raise ValueError("请设置环境变量TQ_USERNAME和TQ_PASSWORD，或直接传入用户名和密码")

        # 设置hots.json文件路径
        if hots_file_path:
            self.hots_file_path = Path(hots_file_path)
        else:
            # 默认路径为相对于当前脚本的../common/hots.json
            current_dir = Path(__file__).parent
            self.hots_file_path = current_dir.parent / "common" / "hots.json"

        self.api = None
        self.logger = self._setup_logger()

        # 交易所映射
        self.exchange_mapping = {
            'CFFEX': '中金所',
            'SHFE': '上期所',
            'DCE': '大商所',
            'CZCE': '郑商所',
            'INE': '能源交易所',
            'GFEX': '广州期货交易所'
        }

    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('TqHotContractUpdater')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def connect_api(self, max_retries: int = 3) -> bool:
        """连接天勤API，支持重试机制"""
        for attempt in range(max_retries):
            try:
                self.api = TqApi(auth=TqAuth(self.username, self.password))
                self.logger.info("天勤API连接成功")
                return True
            except Exception as e:
                self.logger.warning(f"天勤API连接失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    self.logger.error(f"天勤API连接最终失败: {e}")
        return False

    def disconnect_api(self):
        """断开天勤API连接"""
        if self.api:
            try:
                self.api.close()
                self.logger.info("天勤API连接已关闭")
            except Exception as e:
                self.logger.error(f"关闭天勤API连接时出错: {e}")

    def load_hots_json(self) -> Dict:
        """加载现有的hots.json文件"""
        try:
            if self.hots_file_path.exists():
                with open(self.hots_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.logger.info(f"成功加载hots.json文件: {self.hots_file_path}")
                return data
            else:
                self.logger.warning(f"hots.json文件不存在: {self.hots_file_path}")
                return {}
        except Exception as e:
            self.logger.error(f"加载hots.json文件失败: {e}")
            return {}

    def save_hots_json(self, data: Dict):
        """保存hots.json文件"""
        try:
            # 确保目录存在
            self.hots_file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(self.hots_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"成功保存hots.json文件: {self.hots_file_path}")
        except Exception as e:
            self.logger.error(f"保存hots.json文件失败: {e}")
            raise

    def get_all_main_contracts(self) -> List[str]:
        """获取所有主连合约代码"""
        try:
            if not self.api:
                self.logger.error("API未连接，无法获取主连合约")
                return []

            # 查询所有主连合约
            main_contracts = self.api.query_quotes(ins_class="CONT")

            if not main_contracts:
                self.logger.warning("未获取到任何主连合约")
                return []

            # 过滤有效的合约代码
            valid_contracts = [c for c in main_contracts if c and '@' in c]
            self.logger.info(f"获取到 {len(valid_contracts)} 个有效主连合约（总共 {len(main_contracts)} 个）")
            return valid_contracts
        except Exception as e:
            self.logger.error(f"获取主连合约失败: {e}")
            return []

    def get_historical_main_contracts(self, symbol: str, days: int) -> pd.DataFrame:
        """获取指定主连合约的历史标的"""
        try:
            if not self.api:
                self.logger.error("API未连接，无法获取历史数据")
                return pd.DataFrame()

            hist_data = self.api.query_his_cont_quotes(symbol=symbol, n=days)

            if hist_data is None or hist_data.empty:
                self.logger.warning(f"未获取到 {symbol} 的历史数据")
                return pd.DataFrame()

            # 验证数据格式
            required_columns = ['date', symbol]
            missing_columns = [col for col in required_columns if col not in hist_data.columns]
            if missing_columns:
                self.logger.error(f"{symbol} 历史数据缺少必要列: {missing_columns}")
                return pd.DataFrame()

            self.logger.debug(f"成功获取 {symbol} 历史数据，共 {len(hist_data)} 条记录")
            return hist_data
        except Exception as e:
            self.logger.error(f"获取 {symbol} 历史主力合约失败: {e}")
            return pd.DataFrame()

    def parse_contract_info(self, contract_code: str) -> tuple:
        """解析合约代码，返回交易所和品种"""
        try:
            # 格式如: KQ.m@DCE.a -> DCE, a
            if '@' in contract_code:
                parts = contract_code.split('@')
                if len(parts) == 2:
                    exchange_product = parts[1]
                    if '.' in exchange_product:
                        exchange, product = exchange_product.split('.', 1)
                        return exchange, product
            return None, None
        except Exception as e:
            self.logger.error(f"解析合约代码失败 {contract_code}: {e}")
            return None, None

    def format_date(self, date_str: str) -> int:
        """格式化日期为整数格式YYYYMMDD"""
        try:
            if isinstance(date_str, str):
                # 处理不同的日期格式
                if '-' in date_str:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                else:
                    date_obj = datetime.strptime(date_str, '%Y%m%d')
                return int(date_obj.strftime('%Y%m%d'))
            return int(date_str)
        except Exception as e:
            self.logger.error(f"日期格式化失败 {date_str}: {e}")
            return 0

    def update_product_hots(self, exchange: str, product: str, hist_data: pd.DataFrame) -> List[Dict]:
        """更新单个品种的主力合约数据"""
        hots_data = []

        try:
            # 按日期排序
            hist_data = hist_data.sort_values('date')

            prev_contract = ""
            for _, row in hist_data.iterrows():
                current_contract_full = row.get(f'KQ.m@{exchange}.{product}', '')
                date_val = self.format_date(str(row['date']).split(' ')[0])

                # 提取合约代码，去除交易所前缀
                current_contract = ""
                if current_contract_full:
                    # 如果包含交易所前缀，则去除（格式如 SHFE.rb2501 -> rb2501）
                    if '.' in current_contract_full:
                        current_contract = current_contract_full.split('.', 1)[1]
                    else:
                        current_contract = current_contract_full

                # 如果主力合约发生变化
                if current_contract != prev_contract and current_contract:
                    hot_record = {
                        "date": date_val,
                        "from": prev_contract,
                        "newclose": 0.0,  # 需要获取实际收盘价
                        "oldclose": 0.0,  # 需要获取实际收盘价
                        "to": current_contract
                    }
                    hots_data.append(hot_record)
                    prev_contract = current_contract

            self.logger.info(f"更新 {exchange}.{product} 主力合约数据，共 {len(hots_data)} 条记录")
            return hots_data

        except Exception as e:
            self.logger.error(f"更新 {exchange}.{product} 主力合约数据失败: {e}")
            return []

    def update_all_hots(self, days: int = 1000) -> bool:
        """更新所有主力合约数据"""
        try:
            # 连接API
            if not self.connect_api():
                return False

            # 加载现有数据
            hots_data = self.load_hots_json()

            # 获取所有主连合约
            main_contracts = self.get_all_main_contracts()

            updated_count = 0
            failed_count = 0
            total_contracts = len(main_contracts)

            for i, contract in enumerate(main_contracts, 1):
                try:
                    exchange, product = self.parse_contract_info(contract)

                    if not exchange or not product:
                        self.logger.debug(f"跳过无效合约代码: {contract}")
                        continue

                    # 跳过非期货交易所
                    if exchange not in self.exchange_mapping:
                        self.logger.debug(f"跳过非支持交易所: {exchange}")
                        continue

                    self.logger.info(f"正在更新 {exchange}.{product} 主力合约数据... ({i}/{total_contracts})")

                    # 获取历史主力合约数据
                    hist_data = self.get_historical_main_contracts(contract, days)

                    if hist_data.empty:
                        self.logger.warning(f"未获取到 {contract} 的历史数据")
                        failed_count += 1
                        continue

                    # 更新该品种的主力合约数据
                    product_hots = self.update_product_hots(exchange, product, hist_data)

                    if product_hots:
                        # 确保交易所存在
                        if exchange not in hots_data:
                            hots_data[exchange] = {}

                        # 确保品种存在
                        if product not in hots_data[exchange]:
                            hots_data[exchange][product] = []

                        # 获取现有数据
                        existing_data = hots_data[exchange][product]

                        if not existing_data:
                            # 如果没有现有数据，直接使用新数据
                            hots_data[exchange][product] = product_hots
                            updated_count += 1
                            self.logger.info(f"{exchange}.{product} 新增 {len(product_hots)} 条主力合约记录")
                        else:
                            # 获取现有数据的最后一条记录
                            last_record = existing_data[-1]
                            last_from = last_record.get('from', '')
                            last_to = last_record.get('to', '')

                            # 在新数据中查找匹配的记录
                            start_index = -1
                            try:
                                for i, record in enumerate(product_hots):
                                    if record.get('from') == last_from and record.get('to') == last_to:
                                        start_index = i
                                        break

                                if start_index >= 0 and start_index + 1 < len(product_hots):
                                    # 从匹配记录的下一条开始追加
                                    new_records = product_hots[start_index + 1:]
                                    if new_records:
                                        hots_data[exchange][product].extend(new_records)
                                        updated_count += 1
                                        self.logger.info(f"{exchange}.{product} 追加 {len(new_records)} 条新的主力合约记录")
                                    else:
                                        self.logger.info(f"{exchange}.{product} 没有新的主力合约记录需要追加")
                                else:
                                    # 如果没找到匹配记录，使用日期去重的方式追加
                                    existing_dates = {record.get('date') for record in existing_data}
                                    new_records = [record for record in product_hots if record.get('date') not in existing_dates]

                                    if new_records:
                                        hots_data[exchange][product].extend(new_records)
                                        updated_count += 1
                                        self.logger.info(f"{exchange}.{product} 基于日期去重追加 {len(new_records)} 条主力合约记录")
                                    else:
                                        self.logger.info(f"{exchange}.{product} 没有新的主力合约记录需要追加")

                            except Exception as e:
                                self.logger.warning(f"处理 {exchange}.{product} 数据追加时出错: {e}，使用日期去重方式")
                                # 异常处理：使用日期去重的方式追加
                                existing_dates = {record.get('date') for record in existing_data}
                                new_records = [record for record in product_hots if record.get('date') not in existing_dates]

                                if new_records:
                                    hots_data[exchange][product].extend(new_records)
                                    updated_count += 1
                                    self.logger.info(f"{exchange}.{product} 异常恢复：基于日期去重追加 {len(new_records)} 条主力合约记录")
                                else:
                                    self.logger.info(f"{exchange}.{product} 异常恢复：没有新的主力合约记录需要追加")

                        self.logger.info(f"成功处理 {exchange}.{product}")
                    else:
                        failed_count += 1
                        self.logger.warning(f"更新 {exchange}.{product} 失败")

                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"处理合约 {contract} 时发生错误: {e}")
                    continue

            # 保存更新后的数据
            if updated_count > 0:
                self.save_hots_json(hots_data)
                self.logger.info(f"更新完成！成功: {updated_count} 个品种，失败: {failed_count} 个品种")
                return True
            else:
                self.logger.warning(f"没有更新任何数据。失败: {failed_count} 个品种")
                return False

        except Exception as e:
            self.logger.error(f"更新主力合约数据失败: {e}")
            return False
        finally:
            self.disconnect_api()

    def update_specific_products(self, products: List[str], days: int) -> bool:
        """更新指定品种的主力合约数据"""
        try:
            if not self.connect_api():
                return False

            hots_data = self.load_hots_json()
            updated_count = 0

            for product_code in products:
                # 产品代码格式: EXCHANGE.PRODUCT 如 DCE.a
                if '.' not in product_code:
                    self.logger.warning(f"产品代码格式错误: {product_code}，应为 EXCHANGE.PRODUCT")
                    continue

                exchange, product = product_code.split('.', 1)
                contract = f"KQ.m@{exchange}.{product}"

                self.logger.info(f"正在更新 {product_code} 主力合约数据...")

                hist_data = self.get_historical_main_contracts(contract, days)

                if hist_data.empty:
                    self.logger.warning(f"未获取到 {contract} 的历史数据")
                    continue

                product_hots = self.update_product_hots(exchange, product, hist_data)

                if product_hots:
                    if exchange not in hots_data:
                        hots_data[exchange] = {}

                    # 确保品种存在
                    if product not in hots_data[exchange]:
                        hots_data[exchange][product] = []

                    # 获取现有数据
                    existing_data = hots_data[exchange][product]

                    if not existing_data:
                        # 如果没有现有数据，直接使用新数据
                        hots_data[exchange][product] = product_hots
                        updated_count += 1
                        self.logger.info(f"{exchange}.{product} 新增 {len(product_hots)} 条主力合约记录")
                    else:
                        # 获取现有数据的最后一条记录
                        last_record = existing_data[-1]
                        last_from = last_record.get('from', '')
                        last_to = last_record.get('to', '')

                        # 在新数据中查找匹配的记录
                        start_index = -1
                        try:
                            for i, record in enumerate(product_hots):
                                if record.get('from') == last_from and record.get('to') == last_to:
                                    start_index = i
                                    break

                            if start_index >= 0 and start_index + 1 < len(product_hots):
                                # 从匹配记录的下一条开始追加
                                new_records = product_hots[start_index + 1:]
                                if new_records:
                                    hots_data[exchange][product].extend(new_records)
                                    updated_count += 1
                                    self.logger.info(f"{exchange}.{product} 追加 {len(new_records)} 条新的主力合约记录")
                                else:
                                    self.logger.info(f"{exchange}.{product} 没有新的主力合约记录需要追加")
                            else:
                                # 如果没找到匹配记录，使用日期去重的方式追加
                                existing_dates = {record.get('date') for record in existing_data}
                                new_records = [record for record in product_hots if record.get('date') not in existing_dates]

                                if new_records:
                                    hots_data[exchange][product].extend(new_records)
                                    updated_count += 1
                                    self.logger.info(f"{exchange}.{product} 基于日期去重追加 {len(new_records)} 条主力合约记录")
                                else:
                                    self.logger.info(f"{exchange}.{product} 没有新的主力合约记录需要追加")

                        except Exception as e:
                            self.logger.warning(f"处理 {exchange}.{product} 数据追加时出错: {e}，使用日期去重方式")
                            # 异常处理：使用日期去重的方式追加
                            existing_dates = {record.get('date') for record in existing_data}
                            new_records = [record for record in product_hots if record.get('date') not in existing_dates]

                            if new_records:
                                hots_data[exchange][product].extend(new_records)
                                updated_count += 1
                                self.logger.info(f"{exchange}.{product} 异常恢复：基于日期去重追加 {len(new_records)} 条主力合约记录")
                            else:
                                self.logger.info(f"{exchange}.{product} 异常恢复：没有新的主力合约记录需要追加")

            if updated_count > 0:
                self.save_hots_json(hots_data)
                self.logger.info(f"成功更新 {updated_count} 个指定品种的主力合约数据")
                return True
            else:
                self.logger.warning("没有更新任何指定品种的数据")
                return False

        except Exception as e:
            self.logger.error(f"更新指定品种主力合约数据失败: {e}")
            return False
        finally:
            self.disconnect_api()


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='天勤量化主力合约更新工具')
    parser.add_argument('--username', '-u', help='天勤账户用户名（也可通过环境变量TQ_USERNAME设置）')
    parser.add_argument('--password', '-p', help='天勤账户密码（也可通过环境变量TQ_PASSWORD设置）')
    parser.add_argument('--hots-file', '-f', help='hots.json文件路径')
    parser.add_argument('--days', '-d', type=int, default=200, help='获取历史数据的天数（默认200天）')
    parser.add_argument('--products', '-pr', nargs='+', help='指定要更新的品种列表，格式: EXCHANGE.PRODUCT 如 DCE.a SHFE.cu')

    args = parser.parse_args()

    try:
        # 创建更新器实例
        updater = TqHotContractUpdater(
            username=args.username,
            password=args.password,
            hots_file_path=args.hots_file
        )

        # 执行更新
        if args.products:
            success = updater.update_specific_products(args.products, args.days)
        else:
            success = updater.update_all_hots(args.days)

        if success:
            print("主力合约数据更新完成！")
            return 0
        else:
            print("主力合约数据更新失败！")
            return 1

    except Exception as e:
        print(f"程序执行失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
