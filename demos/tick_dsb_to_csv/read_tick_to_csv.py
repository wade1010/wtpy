from wtpy.wrapper import WtDataHelper
import pandas as pd
import os

def read_tick_to_csv(tick_file, output_csv=None):
    """
    从dsb格式的tick文件中读取数据并保存为CSV
    
    参数:
        tick_file (str): tick数据文件路径
        output_csv (str, 可选): 输出CSV文件路径，如果为None则自动生成
    
    返回:
        str: 生成的CSV文件路径
    """
    # 检查文件是否存在
    if not os.path.exists(tick_file):
        raise FileNotFoundError(f"文件不存在: {tick_file}")
    
    # 初始化数据帮助器
    dt_helper = WtDataHelper()
    
    # 读取tick数据
    print(f"正在读取tick文件: {tick_file}")
    ticks = dt_helper.read_dsb_ticks(tick_file)
    print(f"成功读取到{len(ticks)}条tick记录")
    
    # 转换为DataFrame - 使用to_df()方法
    df = ticks.to_df()
    
    # 如果未指定输出文件，则根据输入文件名生成
    if output_csv is None:
        base_name = os.path.basename(tick_file).rsplit(".", 1)[0]
        output_csv = f"{base_name}.csv"
    
    # 保存为CSV
    df.to_csv(output_csv, index=False)
    print(f"数据已保存至: {output_csv}")
    
    return output_csv

if __name__ == "__main__":
    # 设置tick文件路径
    tick_file = "../storage/bin/ticks/CFFEX.IF.HOT_tick_20210104.dsb"
    
    # 执行转换
    output_file = read_tick_to_csv(tick_file)
    
    # 打印样例数据
    try:
        df = pd.read_csv(output_file)
        print("\n数据样例:")
        print(df.head())
    except Exception as e:
        print(f"读取生成的CSV文件时发生错误: {str(e)}") 