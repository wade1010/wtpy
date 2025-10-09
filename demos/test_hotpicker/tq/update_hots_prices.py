#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
更新 hots.json 文件中的 newclose 和 oldclose 价格
通过回测获取前一交易日的收盘价来更新价格信息
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path

try:
    from tqsdk import TqApi, TqAuth, TqBacktest
    import pandas as pd
    from dotenv import load_dotenv
except ImportError as e:
    print(f"请安装必要的依赖包: pip install tqsdk pandas python-dotenv")
    print(f"错误详情: {e}")
    sys.exit(1)

# 加载环境变量
load_dotenv()


class HotsPriceUpdater:
    """hots.json价格更新器"""

    def __init__(self, username: str = None, password: str = None, hots_file_path: str = None):
        """
        初始化价格更新器
        
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
            # 默认路径为相对于当前脚本的../../common/hots.json
            current_dir = Path(__file__).parent
            self.hots_file_path = current_dir.parent.parent / "common" / "hots.json"

        self.logger = self._setup_logger()
        
        # 缓存系统
        self.cache_file = current_dir / "price_cache.json"
        self.price_cache = self._load_cache()

    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('HotsPriceUpdater')
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    def _load_cache(self) -> Dict:
        """加载价格缓存"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    self.logger.info(f"已加载价格缓存，包含 {len(cache)} 条记录")
                    return cache
        except Exception as e:
            self.logger.warning(f"加载缓存失败: {e}")
        return {}

    def _save_cache(self):
        """保存价格缓存"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.price_cache, f, ensure_ascii=False, indent=2)
            self.logger.debug(f"已保存价格缓存到 {self.cache_file}")
        except Exception as e:
            self.logger.error(f"保存缓存失败: {e}")

    def _get_cache_key_two_prices(self, exchange: str, to_contract: str, current_date: datetime, target_date: datetime) -> str:
        """生成成对价格获取的缓存key"""
        current_date_str = current_date.strftime('%Y%m%d')
        target_date_str = target_date.strftime('%Y%m%d')
        return f"two_prices:{exchange}:{to_contract}:{current_date_str}:{target_date_str}"

    def _get_cache_key_single_price(self, exchange: str, contract: str, date: datetime) -> str:
        """生成单个价格获取的缓存key"""
        date_str = date.strftime('%Y%m%d')
        return f"single_price:{exchange}:{contract}:{date_str}"

    def load_hots_json(self) -> Dict:
        """加载现有的hots.json文件"""
        try:
            if self.hots_file_path.exists():
                with open(self.hots_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self.logger.info(f"成功加载hots.json文件: {self.hots_file_path}")
                return data
            else:
                self.logger.error(f"hots.json文件不存在: {self.hots_file_path}")
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

    def date_to_datetime(self, date_int: int) -> datetime:
        """将整数日期转换为datetime对象"""
        date_str = str(date_int)
        return datetime.strptime(date_str, '%Y%m%d')

    def get_previous_trading_day(self, date: datetime) -> datetime:
        """获取前一个交易日（简单实现，向前推1天，实际应考虑节假日）"""
        return date - timedelta(days=1)

    def get_two_close_prices_by_backtest(self, exchange: str, contract: str, current_date: datetime,
                                         next_date: datetime, backtest_start_date: datetime) -> tuple[Optional[float], Optional[float]]:
        """
        通过一次回测获取同一合约在两个不同日期的收盘价
        
        Args:
            exchange: 交易所代码
            contract: 合约代码 (to_contract)
            current_date: 第i条记录的日期 (current_contract的date)
            next_date: 第i+1条记录的日期 (next_contract的date)
            backtest_start_date: 回测开始日期 (使用next_contract的date)
            
        Returns:
            (第i条的newclose, 第i+1条的oldclose)，获取失败返回None
        """
        try:
            # 计算回测结束时间
            end_date = backtest_start_date + timedelta(days=8)

            full_contract = f"{exchange}.{contract}"
            self.logger.info(f"开始回测获取价格，回测时间: {backtest_start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
            self.logger.info(f"  合约: {full_contract}")
            self.logger.info(f"  查找目标:")
            self.logger.info(f"    第i条newclose: < {current_date.strftime('%Y-%m-%d')}")
            self.logger.info(f"    第i+1条oldclose: < {next_date.strftime('%Y-%m-%d')}")

            # 创建回测API
            api = TqApi(
                auth=TqAuth(self.username, self.password),
                backtest=TqBacktest(start_dt=backtest_start_date, end_dt=end_date),
                disable_print=True
            )

            try:
                current_price = None  # 第i条的newclose
                next_price = None  # 第i+1条的oldclose

                # 获取K线数据
                klines = api.get_kline_serial(full_contract, duration_seconds=24 * 60 * 60)
                api.wait_update()

                if len(klines) > 0:
                    # 倒序遍历K线数据，同时查找两个目标价格
                    for i in range(len(klines) - 1, -1, -1):
                        kline_time = datetime.fromtimestamp(klines.iloc[i]['datetime'] / 1000000000)
                        
                        # 如果时间在1990年之前，停止查找
                        if kline_time.year < 1990:
                            self.logger.info(f"  遇到1990年之前的数据 ({kline_time.strftime('%Y-%m-%d')})，停止查找")
                            break
                            
                        close_price = klines.iloc[i]['close']

                        # 查找第i+1条的oldclose (第一个小于next_date的)
                        if next_price is None and kline_time.date() < next_date.date():
                            next_price = close_price
                            self.logger.info(f"  找到第i+1条oldclose: {next_price} (日期: {kline_time.strftime('%Y-%m-%d')})")
                            continue

                        # 查找第i条的newclose (第一个小于current_date的)
                        if current_price is None and kline_time.date() < current_date.date():
                            current_price = close_price
                            self.logger.info(f"  找到第i条newclose: {current_price} (日期: {kline_time.strftime('%Y-%m-%d')})")

                        # 如果两个价格都找到了，可以提前退出
                        if current_price is not None and next_price is not None:
                            break

                return current_price, next_price

            finally:
                api.close()

        except Exception as e:
            self.logger.error(f"回测获取价格失败: {e}")
            return 0, 0

    def update_contract_prices(self, exchange: str, product: str, contracts: List[Dict]) -> List[Dict]:
        """
        更新单个品种的合约价格
        
        根据您的需求：
        1. 如果from为空，则oldclose不需要更新（设为0.0）
        2. 如果是该合约的最新一个，就不需要更新newclose（设为0.0）
        3. 对于需要更新的价格，通过回测获取前一交易日的收盘价
        
        Args:
            exchange: 交易所代码
            product: 品种代码
            contracts: 合约列表
            
        Returns:
            更新后的合约列表
        """
        updated_contracts = []
        total_pairs = len(contracts) - 1  # 需要处理的合约对数量

        # 初始化所有合约记录
        for contract in contracts:
            updated_contracts.append(contract.copy())

        self.logger.info(f"开始处理 {exchange}.{product}，共 {len(contracts)} 条记录，需处理 {total_pairs} 对合约")

        # 第0条记录不需要更新oldclose
        if len(updated_contracts) > 0:
            updated_contracts[0]['oldclose'] = 0.0
            self.logger.info(f"第0条记录，oldclose设为0.0")

        # 最后一条记录不需要更新newclose
        if len(updated_contracts) > 0:
            updated_contracts[-1]['newclose'] = 0.0
            self.logger.info(f"最后一条记录，newclose设为0.0")

        # 成对处理中间的记录，减少回测次数
        for i in range(len(contracts) - 1):
            try:
                current_contract = contracts[i]
                next_contract = contracts[i + 1]

                # 获取下一条记录的日期作为回测基准时间
                next_date_int = next_contract['date']
                target_date = self.date_to_datetime(next_date_int)

                pair_progress = ((i + 1) / total_pairs * 100) if total_pairs > 0 else 0
                self.logger.info(f"  [{i + 1}/{total_pairs}] ({pair_progress:.1f}%) 处理第 {i}/{i + 1} 对记录，回测日期: {next_date_int}")
                self.logger.info(f"    第{i}条 to: '{current_contract['to']}'")
                self.logger.info(f"    第{i + 1}条 from: '{next_contract['from']}'")

                # 在一次回测中同时获取两个价格
                to_contract = current_contract['to']
                from_contract = next_contract['from']
                current_date = self.date_to_datetime(current_contract['date'])

                # 检查缓存
                cache_key = self._get_cache_key_two_prices(exchange, to_contract, current_date, target_date)
                if cache_key in self.price_cache:
                    newclose, oldclose = self.price_cache[cache_key]
                    self.logger.info(f"    从缓存获取价格: newclose={newclose}, oldclose={oldclose}")
                else:
                    # 使用新的合并回测方法，只传入to_contract
                    newclose, oldclose = self.get_two_close_prices_by_backtest(
                        exchange,
                        to_contract,  # 合约代码
                        current_date,  # 第i条的日期
                        target_date,  # 第i+1条的日期
                        target_date  # 回测开始时间使用第i+1条的日期
                    )
                    # 保存到缓存
                    if newclose is not None and oldclose is not None:
                        self.price_cache[cache_key] = [newclose, oldclose]
                        self._save_cache()
                        self.logger.info(f"    已缓存价格: newclose={newclose}, oldclose={oldclose}")

                # 更新第i条的newclose
                if to_contract != "":
                    if newclose is not None:
                        updated_contracts[i]['newclose'] = newclose
                        self.logger.info(f"    成功更新第{i}条newclose: {newclose}")
                    else:
                        self.logger.warning(f"    无法获取 {to_contract} 的收盘价，保持原值")

                # 更新第i+1条的oldclose
                if from_contract != "":
                    if oldclose is not None:
                        updated_contracts[i + 1]['oldclose'] = oldclose
                        self.logger.info(f"    成功更新第{i + 1}条oldclose: {oldclose}")
                    else:
                        self.logger.warning(f"    无法获取 {from_contract} 的收盘价，保持原值")
                else:
                    updated_contracts[i + 1]['oldclose'] = 0.0
                    self.logger.info(f"    第{i + 1}条from为空，oldclose设为0.0")

            except Exception as e:
                self.logger.error(f"处理第 {i}/{i + 1} 对记录失败: {e}")
                # 出错时保持原记录不变
                pass

        # 检查最后一条记录的newclose是否为空，如果为空则单独获取
        if len(updated_contracts) > 0:
            last_contract = updated_contracts[-1]
            if last_contract['newclose'] == 0:
                to_contract = last_contract.get('to')
                last_date = self.date_to_datetime(last_contract['date'])
                self.logger.info(f"  最后一条记录的newclose为空，单独获取 {to_contract} 的收盘价")

                # 检查缓存
                cache_key = self._get_cache_key_single_price(exchange, to_contract, last_date)
                if cache_key in self.price_cache:
                    newclose = self.price_cache[cache_key]
                    self.logger.info(f"  从缓存获取最后一条记录的价格: {newclose}")
                else:
                    newclose = self.get_close_price_by_backtest(exchange, to_contract, last_date)
                    # 保存到缓存
                    if newclose is not None:
                        self.price_cache[cache_key] = newclose
                        self._save_cache()
                        self.logger.info(f"  已缓存最后一条记录的价格: {newclose}")
                
                if newclose is not None:
                    updated_contracts[-1]['newclose'] = newclose
                    self.logger.info(f"  成功更新最后一条记录的newclose: {newclose}")
                else:
                    self.logger.warning(f"  无法获取最后一条记录 {to_contract} 的收盘价")

        self.logger.info(f"完成 {exchange}.{product} 价格更新，共处理 {len(contracts)} 条记录")
        return updated_contracts

    def get_close_price_by_backtest(self, exchange: str, contract: str, target_date: datetime) -> Optional[float]:
        """
        通过回测获取指定合约在指定日期的收盘价

        Args:
            exchange: 交易所代码
            contract: 合约代码
            target_date: 目标日期

        Returns:
            收盘价，如果获取失败返回None
        """
        try:
            # 计算回测开始和结束时间
            # 回测开始时间为目标日期，结束时间为开始时间加8天
            start_date = target_date
            end_date = target_date + timedelta(days=8)

            # 构造完整的合约代码
            full_contract = f"{exchange}.{contract}"

            self.logger.info(f"正在获取 {full_contract} 在 {target_date.strftime('%Y-%m-%d')} 的收盘价...")

            # 创建回测API
            api = TqApi(
                auth=TqAuth(self.username, self.password),
                backtest=TqBacktest(start_dt=start_date, end_dt=end_date),
                disable_print=True
            )

            try:
                # 获取K线数据，获取更多数据以确保包含目标日期
                klines = api.get_kline_serial(full_contract, duration_seconds=24 * 60 * 60)
                # 等待数据更新
                api.wait_update()
                # 查找目标日期的收盘价 - 倒序查找第一个时间小于target_date的收盘价
                target_date_str = target_date.strftime('%Y-%m-%d')
                # 转换K线数据为DataFrame以便处理
                if len(klines) > 0:
                    # 倒序遍历K线数据，查找第一个时间小于target_date的收盘价
                    for i in range(len(klines) - 1, -1, -1):
                        try:
                            kline_datetime = datetime.fromtimestamp(klines.iloc[i]['datetime'] / 1000000000)
                            
                            # 如果时间在1990年之前，停止查找
                            if kline_datetime.year < 1990:
                                self.logger.info(f"遇到1990年之前的数据 ({kline_datetime.strftime('%Y-%m-%d')})，停止查找")
                                break

                            # 如果K线时间小于目标日期，使用这个收盘价
                            if kline_datetime < target_date:
                                close_price = klines.iloc[i]['close']
                                if close_price > 0:  # 确保价格有效
                                    kline_date_str = kline_datetime.strftime('%Y-%m-%d')
                                    self.logger.info(f"成功获取 {full_contract} 在 {kline_date_str} 的收盘价: {close_price} (目标日期: {target_date_str})")
                                    return float(close_price)
                        except Exception as e:
                            self.logger.debug(f"处理K线数据第{i}条时出错: {e}")
                            continue

                self.logger.warning(f"未找到 {full_contract} 在 {target_date_str} 附近的有效数据")
                return 0

            finally:
                api.close()

        except Exception as e:
            self.logger.error(f"获取 {exchange}.{contract} 在 {target_date} 的收盘价失败: {e}")
            return 0

    def update_all_prices(self, exchanges: List[str] = None, products: List[str] = None) -> bool:
        """
        更新所有或指定的价格数据
        
        Args:
            exchanges: 要更新的交易所列表，None表示更新所有
            products: 要更新的品种列表，None表示更新所有
            
        Returns:
            是否成功
        """
        try:
            # 加载数据
            hots_data = self.load_hots_json()
            if not hots_data:
                self.logger.error("无法加载hots.json数据")
                return False

            updated_count = 0
            total_count = 0

            # 先统计总数
            for exchange, exchange_data in hots_data.items():
                if exchanges and exchange not in exchanges:
                    continue
                for product, contracts in exchange_data.items():
                    if products and product not in products:
                        continue
                    if contracts:
                        total_count += 1

            self.logger.info(f"开始更新价格数据，共需处理 {total_count} 个品种")

            # 遍历所有交易所和品种
            current_index = 0
            for exchange, exchange_data in hots_data.items():
                if exchanges and exchange not in exchanges:
                    continue

                for product, contracts in exchange_data.items():
                    if products and product not in products:
                        continue

                    if not contracts:
                        continue

                    current_index += 1
                    progress_percent = (current_index / total_count * 100) if total_count > 0 else 0
                    self.logger.info(f"[{current_index}/{total_count}] ({progress_percent:.1f}%) 正在更新 {exchange}.{product} 的价格数据...")

                    try:
                        updated_contracts = self.update_contract_prices(exchange, product, contracts)
                        hots_data[exchange][product] = updated_contracts
                        updated_count += 1
                        self.logger.info(f"[{current_index}/{total_count}] 成功更新 {exchange}.{product}")
                    except Exception as e:
                        self.logger.error(f"[{current_index}/{total_count}] 更新 {exchange}.{product} 失败: {e}")
                        continue

            # 保存更新后的数据
            if updated_count > 0:
                self.save_hots_json(hots_data)
                self.logger.info(f"价格更新完成！成功更新 {updated_count}/{total_count} 个品种")
                return True
            else:
                self.logger.warning("没有成功更新任何品种的价格")
                return False

        except Exception as e:
            self.logger.error(f"更新价格数据失败: {e}")
            return False

    def update_specific_product(self, exchange: str, product: str) -> bool:
        """
        更新指定品种的价格数据
        
        Args:
            exchange: 交易所代码
            product: 品种代码
            
        Returns:
            是否成功
        """
        try:
            # 加载数据
            hots_data = self.load_hots_json()
            if not hots_data:
                self.logger.error("无法加载hots.json数据")
                return False

            if exchange not in hots_data:
                self.logger.error(f"交易所 {exchange} 不存在")
                return False

            if product not in hots_data[exchange]:
                self.logger.error(f"品种 {exchange}.{product} 不存在")
                return False

            contracts = hots_data[exchange][product]
            if not contracts:
                self.logger.warning(f"品种 {exchange}.{product} 没有合约数据")
                return False

            self.logger.info(f"正在更新 {exchange}.{product} 的价格数据...")

            updated_contracts = self.update_contract_prices(exchange, product, contracts)
            hots_data[exchange][product] = updated_contracts

            # 保存更新后的数据
            self.save_hots_json(hots_data)
            self.logger.info(f"成功更新 {exchange}.{product} 的价格数据")
            return True

        except Exception as e:
            self.logger.error(f"更新 {exchange}.{product} 价格数据失败: {e}")
            return False


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='更新hots.json文件中的价格数据')
    parser.add_argument('--username', '-u', help='天勤账户用户名（也可通过环境变量TQ_USERNAME设置）')
    parser.add_argument('--password', '-p', help='天勤账户密码（也可通过环境变量TQ_PASSWORD设置）')
    parser.add_argument('--hots-file', '-f', help='hots.json文件路径')
    parser.add_argument('--exchange', '-e', help='指定要更新的交易所')
    parser.add_argument('--product', '-pr', help='指定要更新的品种（需要同时指定交易所）')
    parser.add_argument('--exchanges', '-es', nargs='+', help='指定要更新的交易所列表')
    parser.add_argument('--products', '-prs', nargs='+', help='指定要更新的品种列表')

    args = parser.parse_args()

    try:
        # 创建更新器实例
        updater = HotsPriceUpdater(
            username=args.username,
            password=args.password,
            hots_file_path=args.hots_file
        )

        # 执行更新
        if args.exchange and args.product:
            # 更新指定品种
            success = updater.update_specific_product(args.exchange, args.product)
        else:
            # 更新所有或指定的交易所/品种
            success = updater.update_all_prices(
                exchanges=args.exchanges,
                products=args.products
            )

        if success:
            print("价格数据更新完成！")
            return 0
        else:
            print("价格数据更新失败！")
            return 1

    except Exception as e:
        print(f"程序执行失败: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
