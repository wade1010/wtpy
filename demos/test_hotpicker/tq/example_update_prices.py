#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
使用示例：更新hots.json文件中的价格数据
"""

import sys
from pathlib import Path

# 添加当前目录到Python路径
sys.path.append(str(Path(__file__).parent))

from update_hots_prices import HotsPriceUpdater


def example_update_specific_product():
    """示例：更新指定品种的价格"""
    try:
        # 创建更新器（会自动从环境变量读取账户信息）
        updater = HotsPriceUpdater()

        # 更新指定品种，例如中金所的IC品种
        success = updater.update_specific_product('GFEX', 'lc')

        if success:
            print("成功更新CFFEX.IC的价格数据！")
        else:
            print("更新CFFEX.IC的价格数据失败！")

    except Exception as e:
        print(f"执行失败: {e}")


def example_update_specific_exchanges():
    """示例：更新指定交易所的所有品种"""
    try:
        updater = HotsPriceUpdater()

        # 只更新中金所的品种
        success = updater.update_all_prices(exchanges=['CFFEX'])

        if success:
            print("成功更新CFFEX交易所的价格数据！")
        else:
            print("更新CFFEX交易所的价格数据失败！")

    except Exception as e:
        print(f"执行失败: {e}")


def example_update_all():
    """示例：更新所有品种的价格"""
    try:
        updater = HotsPriceUpdater()

        # 更新所有品种（这可能需要很长时间）
        success = updater.update_all_prices()

        if success:
            print("成功更新所有品种的价格数据！")
        else:
            print("更新价格数据失败！")

    except Exception as e:
        print(f"执行失败: {e}")


def main():
    """主函数"""
    print("hots.json价格更新示例")
    print("=" * 50)

    print("\n选择要执行的示例：")
    print("1. 更新指定品种 (CFFEX.IC)")
    print("2. 更新指定交易所 (CFFEX)")
    print("3. 更新所有品种 (警告：耗时很长)")
    print("0. 退出")

    try:
        choice = input("\n请输入选择 (0-3): ").strip()

        if choice == '1':
            print("\n正在更新CFFEX.IC...")
            example_update_specific_product()
        elif choice == '2':
            print("\n正在更新CFFEX交易所...")
            example_update_specific_exchanges()
        elif choice == '3':
            confirm = input("\n更新所有品种可能需要很长时间，确定继续吗？(y/N): ").strip().lower()
            if confirm == 'y':
                print("\n正在更新所有品种...")
                example_update_all()
            else:
                print("已取消操作")
        elif choice == '0':
            print("退出程序")
        else:
            print("无效选择")

    except KeyboardInterrupt:
        print("\n\n用户中断操作")
    except Exception as e:
        print(f"\n执行出错: {e}")


if __name__ == "__main__":
    # example_update_specific_product()
    example_update_all()
