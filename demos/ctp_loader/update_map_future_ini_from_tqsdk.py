#!/usr/bin/env python
# -*- coding: utf-8 -*-
from tqsdk import TqApi, TqAuth
import os
import re

# 合约信息缓存
CONTRACT_INFO = {}
# 期货交易所列表
EXCHANGES = ["SHFE", "CFFEX", "DCE", "CZCE", "INE", "GFEX"]

def get_contracts_from_tqsdk(username, password):
    """获取天勤SDK中的期货合约信息"""
    print("正在连接天勤API...")
    api = TqApi(auth=TqAuth(username, password))

    try:
        futures_contracts = {}

        # 获取期货合约信息
        for exchange in EXCHANGES:
            try:
                # 查询期货合约
                code_list = api.query_quotes(ins_class="FUTURE", exchange_id=exchange)
                if not code_list:
                    print(f"未从 {exchange} 交易所获取到合约")
                    continue

                # 获取合约详细信息
                code_list_info = api.query_symbol_info(code_list)

                for idx, row in code_list_info.iterrows():
                    product_id = row['product_id']
                    # 仅保存品种信息
                    if product_id not in futures_contracts:
                        # 去掉合约名称末尾的数字
                        name = row["instrument_name"]
                        # 移除末尾的数字和可能的空格
                        name = re.sub(r'\d+\s*$', '', name).strip()

                        futures_contracts[product_id] = {
                            "exchange": exchange,
                            "name": name,
                            "product": product_id,
                            "trading_time_day": row["trading_time_day"],
                            "trading_time_night": row["trading_time_night"],
                        }
                        # print(f'标的:{name},日盘时间段:{row["trading_time_day"]},夜盘时间:{row["trading_time_night"]}')
            except Exception as e:
                print(f"获取 {exchange} 合约时出错: {str(e)}")

        return futures_contracts

    finally:
        api.close()
        print("天勤API连接已关闭")


def update_map_future_ini(new_contracts):
    """更新map_future.ini文件，只追加新的合约信息，不修改现有配置"""
    ini_path = os.path.join(os.path.dirname(__file__), "map_future.ini")

    if not os.path.exists(ini_path):
        print(f"配置文件不存在: {ini_path}")
        return False

    # 读取现有配置
    with open(ini_path, "r", encoding="utf-8") as f:
        content = f.read()
        lines = content.splitlines()

    # 解析现有配置
    name_section = True
    existing_names = {}
    existing_sessions = {}
    name_lines = []
    session_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line == "[Name]":
            name_section = True
            name_lines.append(line)
            continue
        elif line == "[Session]":
            name_section = False
            session_lines.append(line)
            continue

        if name_section:
            name_lines.append(line)
            if "=" in line:
                parts = line.split("=")
                key = parts[0].strip()
                value = parts[1].strip()
                existing_names[key] = value
        else:
            session_lines.append(line)
            if "=" in line:
                parts = line.split("=")
                key = parts[0].strip()
                value = parts[1].strip()
                existing_sessions[key] = value

    # 准备新的名称配置
    new_name_lines = []
    for code, info in sorted(new_contracts.items()):
        if code not in existing_names:
            new_name_lines.append(f"{code}={info['name']}")

    # 准备新的会话配置
    new_session_lines = []
    for code, info in sorted(new_contracts.items()):
        exchange = info["exchange"]
        full_code = f"{exchange}.{code}"

        if full_code not in existing_sessions:
            # 根据交易时间来决定使用哪个交易时段代码
            session_code = "FD0900"  # 默认值
            
            # 检查交易时间
            day_time = info.get("trading_time_day")
            night_time = info.get("trading_time_night")
            
            if day_time and len(day_time) > 0:
                day_start = day_time[0][0]  # 第一个时间段的开始时间
                
                # 根据日盘开始时间分类
                if day_start == "09:30:00":
                    session_code = "SD0930"
                elif day_start == "09:15:00" or day_start == "09:00:00":
                    # 如果是09:15或09:00开始，还需要看夜盘情况
                    if night_time is None or len(night_time) == 0:
                        # 没有夜盘
                        if day_start == "09:15:00":
                            session_code = "FD0915"
                        else:
                            session_code = "FD0900"
                    else:
                        # 有夜盘，根据夜盘结束时间决定
                        night_end = night_time[0][1]  # 第一个夜盘时间段的结束时间
                        if night_end == "26:30:00":  # 02:30
                            session_code = "FN0230"
                        elif night_end == "25:00:00":  # 01:00
                            session_code = "FN0100"
                        else:  # 23:00
                            session_code = "FN2300"
            
            new_session_lines.append(f"{full_code}={session_code}")
    
    # 构建最终文件内容
    final_content = []
    
    # 添加原有的名称部分
    final_content.extend(name_lines)
    
    # 添加新的名称部分（如果有）
    if new_name_lines:
        final_content.extend(new_name_lines)
    
    # 添加空行分隔
    if final_content[-1].strip() != "":
        final_content.append("")
    
    # 添加原有的会话部分
    final_content.extend(session_lines)
    
    # 添加新的会话部分（如果有）
    if new_session_lines:
        final_content.extend(new_session_lines)
    
    # 写入文件
    with open(ini_path, "w", encoding="utf-8") as f:
        f.write("\n".join(final_content))
    
    print(f"新增合约{len(new_name_lines)}个")
    # 打印新增的标的名称
    if len(new_name_lines)>0:
        print(f"已更新配置文件: {ini_path}")
        print("新增的标的名称:")
        for line in new_name_lines:
            code, name = line.split("=", 1)
            print(f"  {code}: {name}")


def main():
    username = '15600097398'
    password = 'wade1010'
    
    try:
        # 获取合约信息
        contracts = get_contracts_from_tqsdk(username, password)
        
        if contracts:
            print(f"成功获取到 {len(contracts)} 个期货品种")
            update_map_future_ini(contracts)
        else:
            print("未获取到任何合约信息")
            
    except Exception as e:
        print(f"发生错误: {str(e)}")


if __name__ == "__main__":
    main() 