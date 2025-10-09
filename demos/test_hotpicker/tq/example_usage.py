#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
天勤量化主力合约更新工具使用示例
"""

from TqCacheMonExchg import TqHotContractUpdater
import os


def example_update_all():
    """示例：更新所有主力合约数据"""
    print("=== 更新所有主力合约数据示例 ===")

    updater = TqHotContractUpdater()

    success = updater.update_all_hots(days=1700)  # 获取最近1000天的数据

    if success:
        print("✅ 所有主力合约数据更新成功！")
    else:
        print("❌ 主力合约数据更新失败！")


def example_update_specific():
    """示例：更新指定品种的主力合约数据"""
    print("\n=== 更新指定品种主力合约数据示例 ===")

    updater = TqHotContractUpdater()

    # 指定要更新的品种列表
    products = [
        "DCE.a",  # 大商所豆一
        "SHFE.cu",  # 上期所铜
        "CFFEX.IC",  # 中金所中证500
        "CZCE.MA",  # 郑商所甲醇
    ]

    success = updater.update_specific_products(products, days=1000)

    if success:
        print("✅ 指定品种主力合约数据更新成功！")
    else:
        print("❌ 指定品种主力合约数据更新失败！")


def example_custom_config():
    """示例：使用自定义配置"""
    print("\n=== 使用自定义配置示例 ===")

    # 自定义hots.json文件路径
    custom_hots_path = "D:\\my_data\\custom_hots.json"

    updater = TqHotContractUpdater(hots_file_path=custom_hots_path)

    # 只更新几个主要品种
    major_products = ["SHFE.cu", "DCE.i", "CFFEX.IF"]

    success = updater.update_specific_products(major_products, days=1000)

    if success:
        print(f"✅ 自定义配置更新成功！数据已保存到: {custom_hots_path}")
    else:
        print("❌ 自定义配置更新失败！")


def check_environment():
    """检查环境配置"""
    print("=== 环境配置检查 ===")

    username = os.getenv('TQ_USERNAME')
    password = os.getenv('TQ_PASSWORD')

    if username and password:
        print(f"✅ 环境变量配置正确")
        print(f"   用户名: {username}")
        print(f"   密码: {'*' * len(password)}")
    else:
        print("❌ 环境变量未配置")
        print("   请确保已设置 TQ_USERNAME 和 TQ_PASSWORD 环境变量")
        print("   或者复制 env.example 为 .env 并填入您的账户信息")


if __name__ == "__main__":
    # 检查环境配置
    check_environment()

    try:
        # 运行示例
        # example_update_specific()  # 先更新指定品种，速度较快
        example_update_all()  # 更新所有品种，耗时较长
        # example_custom_config()  # 使用自定义配置

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
    except Exception as e:
        print(f"❌ 运行示例时发生错误: {e}")
        import traceback

        traceback.print_exc()
