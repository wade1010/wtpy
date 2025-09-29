from wtpy.WtCoreDefs import WTSBarStruct, WTSTickStruct
from wtpy.wrapper import WtDataHelper
import pandas as pd
from tqdm import tqdm
import os
from tqsdk import TqApi, TqAuth
import datetime
class Ifeed(object):
    def __init__(self):
        self.dthelper = WtDataHelper()
        self.period_map = {"m1":"min1","m5":"min5","d":"day","tick":"ticks"}
        self.frequency_map = {
            "m1":"1m",
            "m5":"5m",
            "d":"1d",
        }
    
    def get_tick(self,symbol,start_date=None,end_date=None):
        return
    
    def get_bar(self,symbol,frequency,start_date=None,end_date=None):
        return
    
    def parse_code(self,code):
        items = code.split(".")
        return items[0],items[1],items[2]

    def code_std(self,stdCode:str):
        stdCode = stdCode.upper()
        items = stdCode.split(".")
        exchg = self.exchgStdToRQ(items[0])
        if len(items) == 2:
            # 简单股票代码，格式如SSE.600000
            return items[1] + "." + exchg
        elif items[1] in ["IDX","ETF","STK","OPT"]:
            # 标准股票代码，格式如SSE.IDX.000001
            return items[2] + "." + exchg
        elif len(items) == 3:
            # 标准期货代码，格式如CFFEX.IF.2103
            if items[2] != 'HOT':
                return ''.join(items[1:])
            else:
                return items[1] + "88"
            
    def cover_d_bar(self,df):
        count = len(df)
        BUFFER = WTSBarStruct * count
        buffer = BUFFER()
        for index, row in tqdm(df.iterrows()):
            curBar = buffer[index]
            curBar.date = int(row["date"])
            curBar.open = float(row["open"])
            curBar.high = float(row["high"])
            curBar.low = float(row["low"])
            curBar.close = float(row["close"])
            curBar.vol = float(row["vol"])
            curBar.money = float(row["money"])
            curBar.hold = float(row["hold"])
        return buffer
    
    def cover_m_bar(self,df):
        count = len(df)
        BUFFER = WTSBarStruct * count
        buffer = BUFFER()
        for index, row in tqdm(df.iterrows()):
            curBar = buffer[index]
            curBar.time = (int(row["date"])-19900000)*10000 + int(row["time"])
            curBar.open = float(row["open"])
            curBar.high = float(row["high"])
            curBar.low = float(row["low"])
            curBar.close = float(row["close"])
            curBar.vol = float(row["vol"])
            curBar.money = float(row["money"])
            curBar.hold = float(row["hold"])
        return buffer
        
    def cover_tick(self,df):
        count = len(df)
        BUFFER = WTSTickStruct * count
        buffer = BUFFER()
        for index, row in tqdm(df.iterrows()):
            curTick = buffer[index]
            curTick.exchg = bytes(row["exchg"],'utf-8')
            curTick.code = bytes(row["code"],'utf-8')
            curTick.price = float(row["price"])
            curTick.open = float(row["open"])
            curTick.high = float(row["high"])
            curTick.low = float(row["low"])
            curTick.settle_price = float(row["settle_price"])
            curTick.total_volume = float(row["total_volume"])
            curTick.volume = float(row["volume"])
            curTick.total_turnover = float(row["total_turnover"])
            curTick.turn_over = float(row["turn_over"])
            curTick.open_interest = float(row["open_interest"])
            curTick.diff_interest = float(row["diff_interest"])
            curTick.trading_date = int(row["trading_date"])
            curTick.action_date = int(row["action_date"])
            curTick.action_time = int(int(row["action_time"]) / 1000)
            curTick.pre_close = float(row["pre_close"])
            curTick.pre_settle = float(row["pre_settle"])
            curTick.pre_interest = float(0.0)
            for x in range(0,5):
                curTick.bid_prices[x] = float(row["bid_" + str(x+1)])
                curTick.bid_qty[x] = float(row["bid_qty_" + str(x+1)])
                curTick.ask_prices[x] = float(row["ask_" + str(x+1)])
                curTick.ask_qty[x] = float(row["ask_qty_" + str(x+1)])
        return buffer
        
    def bar_df_to_dsb(self,df,dsb_file,period):
        if "d" in period:
            buffer = self.cover_d_bar(df)
        elif "m" in period:
            buffer = self.cover_m_bar(df)      
        self.dthelper.store_bars(barFile=dsb_file,firstBar=buffer,count=len(buffer),period=period)

    def tick_df_to_dsb(self,df,dsb_file):
        buffer = self.cover_tick(df)
        self.dthelper.store_ticks(tickFile=dsb_file, firstTick=buffer, count=len(buffer))
        
    # 新下的数据会覆盖旧的数据
    def store_bin_bar(self,storage_path,code,start_date=None,end_date=None,frequency="1m",col_map=None):
        df = self.get_bar(code,start_date,end_date,frequency)
        period = self.period_map[frequency]
        save_path = os.path.join(storage_path,"bin",period)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        dsb_path = os.path.join(save_path,f"{code}_{frequency}.dsb")
        self.bar_df_to_dsb(df,dsb_path,frequency)
        
    def store_bin_tick(self,storage_path,code,start_date=None,end_date=None,col_map=None):
        df = self.get_tick(code,start_date,end_date)
        save_path = os.path.join(storage_path,"bin","ticks")
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        g = df.groupby("trading_date")
        for trading_date,g_df in g:
            g_df = g_df.reset_index()
            dsb_path = os.path.join(save_path,f"{code}_tick_{trading_date}.dsb")
            self.tick_df_to_dsb(g_df,dsb_path)
    
    # 除了转换为dsb格式，还会按照his的格式进行存储
    def store_his_bar(self,storage_path,code,start_date=None,end_date=None,frequency="1m",skip_saved=False):
        exchange,pid,month = self.parse_code(code)
        if frequency not in self.frequency_map.keys():
            print("周期只能为m1、m5或d,回测或实盘中会自动拼接")
        period = self.period_map[frequency]
        save_path = os.path.join(storage_path,"his",period,exchange)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        if exchange == "CZCE":
            month = month[-3:]
        dsb_name = f"{pid}{month}.dsb"
        dsb_path = os.path.join(save_path,dsb_name)
        if skip_saved:
            saved_list = os.listdir(save_path)
            if dsb_name in saved_list:
                print(f"重复数据，跳过{dsb_name}")
                return
        df = self.get_bar(code,start_date,end_date,frequency)
        self.bar_df_to_dsb(df,dsb_path,frequency)
        
    def store_his_tick(self,storage_path,code,start_date=None,end_date=None,skip_saved=False):
        exchange,pid,month = self.parse_code(code)
        # 分天下载，避免内存超出
        for date in pd.date_range(start_date,end_date):
            save_path = os.path.join(storage_path,"his","ticks",exchange,date.strftime('%Y%m%d'))
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            dsb_name = f"{pid}{month}.dsb"
            if skip_saved:
                saved_list = os.listdir(save_path)
                if dsb_name in saved_list:
                    print(f"重复数据，跳过{dsb_name}")
                    continue
            t_day = date.strftime('%Y.%m.%d')
            df = self.get_tick(code,t_day,t_day)
            if (df is None) or (df.empty):
                print(f"{date}:{code}没有数据")
                continue
            if exchange == "CZCE":
                month = month[-3:]
            dsb_path = os.path.join(save_path,f"{pid}{month}.dsb")
            self.tick_df_to_dsb(df,dsb_path)

class TqFeed(Ifeed):
    def __init__(self, user=None, passwd=None):
        super().__init__()
        self.tqauth = TqAuth(user, passwd)
        # 列映射关系
        self.bar_col_map = {
            "date": "date",
            "time": "time",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "vol",
            "amount": "money",
            "open_interest": "hold"
        }
        
        self.tick_col_map = {
            "code": "code",
            "exchg": "exchg",
            "last_price": "price",
            "open": "open",
            "highest": "high",
            "lowest": "low",
            "volume": "total_volume",
            "volume_delta": "volume",
            "amount": "total_turnover",
            "amount_delta": "turn_over",
            "open_interest": "open_interest",
            "open_interest_delta": "diff_interest",
            "trading_day": "trading_date",
            "datetime": "action_date",
            "time": "action_time",
            "pre_close": "pre_close",
            "settlement": "settle_price",
            "pre_settlement": "pre_settle"
        }
        
        # 添加买卖盘数据映射
        for i in range(1, 6):
            self.tick_col_map[f"ask_price{i}"] = f"ask_{i}"
            self.tick_col_map[f"bid_price{i}"] = f"bid_{i}"
            self.tick_col_map[f"ask_volume{i}"] = f"ask_qty_{i}"
            self.tick_col_map[f"bid_volume{i}"] = f"bid_qty_{i}"

    def get_tick(self, code, start_date=None, end_date=None):
        """获取Tick数据"""
        symbol = self.code_std(code)
        exchange, pid, month = self.parse_code(code)
        
        # 创建TqApi实例（每次调用创建新实例避免长连接问题）
        api = TqApi(auth=self.tqauth)
        try:
            # 从天勤获取tick数据
            ticks = api.get_tick_serial(symbol, start_dt=start_date, end_dt=end_date)
            if ticks is None or len(ticks) == 0:
                return None
                
            # 转换为DataFrame
            df = pd.DataFrame(ticks)
            
            # 添加交易所和合约代码列
            df["exchg"] = code.split(".")[0]
            df["code"] = code.split(".")[1] + code.split(".")[2]
            
            # 处理日期时间格式
            df["datetime"] = pd.to_datetime(df["datetime"] / 1e9, unit="s")
            df["date"] = df["datetime"].dt.strftime("%Y%m%d")
            df["time"] = df["datetime"].dt.strftime("%H%M%S%f")
            df["trading_day"] = df["trading_day"].apply(lambda x: x.replace("-", ""))
            
            # 计算增量值
            df["volume_delta"] = df["volume"].diff().fillna(0).astype(float)
            df["amount_delta"] = df["amount"].diff().fillna(0).astype(float)
            df["open_interest_delta"] = df["open_interest"].diff().fillna(0).astype(float)
            
            # 选择并重命名列
            df = df[[col for col in self.tick_col_map.keys() if col in df.columns]]
            df = df.rename(columns={k: v for k, v in self.tick_col_map.items() if k in df.columns})
            
            return df
        finally:
            # 确保关闭API连接
            api.close()
    
    def get_bar(self, code, start_date=None, end_date=None, frequency="1m"):
        """获取K线数据"""
        if frequency not in self.frequency_map.keys():
            print("周期只能为m1、m5或d，回测或实盘中会自动拼接")
            return None
            
        symbol = self.code_std(code)
        
        # 创建TqApi实例
        api = TqApi(auth=self.tqauth)
        try:
            # 转换周期格式为天勤接受的格式
            duration = frequency.replace("m", "min") if "m" in frequency else "1day"
            
            # 获取K线数据
            klines = api.get_kline_serial(symbol, duration=duration, start_dt=start_date, end_dt=end_date)
            if klines is None or len(klines) == 0:
                return None
                
            # 转换为DataFrame
            df = pd.DataFrame(klines)
            
            # 处理日期时间格式
            df["datetime"] = pd.to_datetime(df["datetime"] / 1e9, unit="s")
            df["date"] = df["datetime"].dt.strftime("%Y%m%d")
            if "m" in frequency:
                df["time"] = df["datetime"].dt.strftime("%H%M")
            else:
                df["time"] = "0000"
                
            # 计算成交额（如果没有提供）
            if "amount" not in df.columns and "volume" in df.columns and "close" in df.columns:
                df["amount"] = df["volume"] * df["close"]
                
            # 选择并重命名列
            df = df[[col for col in self.bar_col_map.keys() if col in df.columns]]
            df = df.rename(columns={k: v for k, v in self.bar_col_map.items() if k in df.columns})
            
            return df
        finally:
            # 确保关闭API连接
            api.close()
            
    def code_std(self, stdCode: str):
        """转换标准代码到天勤代码格式"""
        stdCode = stdCode.upper()
        items = stdCode.split(".")
        
        if len(items) == 2:
            # 简单股票代码，格式如SSE.600000
            if items[0] == "SSE":
                return f"SHFE.{items[1]}"
            elif items[0] == "SZSE":
                return f"SZSE.{items[1]}"
            return stdCode
        elif items[1] in ["IDX", "ETF", "STK", "OPT"]:
            # 标准股票代码，格式如SSE.IDX.000001
            if items[0] == "SSE":
                return f"SHFE.{items[2]}"
            elif items[0] == "SZSE":
                return f"SZSE.{items[2]}"
            return f"{items[0]}.{items[2]}"
        elif len(items) == 3:
            # 标准期货代码，格式如CFFEX.IF.2103
            if items[2] == 'HOT':
                return f"{items[0]}.{items[1]}"  # 主力合约
            return f"{items[0]}.{items[1]}{items[2]}"  # 具体合约

if __name__ == '__main__':
    # 从天勤下载数据
    feed = TqFeed("你的天勤账号", "你的天勤密码")
    
    # 数据存储的目录
    storage_path = "../storage"
    
    # 下载期货数据示例
    feed.store_his_bar(storage_path, "SHFE.ni.2201", start_date="20211225", end_date="20220101", frequency="m1", skip_saved=False)
    feed.store_his_tick(storage_path, "SHFE.ni.2201", start_date="20211225", end_date="20220101", skip_saved=False)
    
    # 读取dsb数据
    dtHelper = WtDataHelper()
    dtHelper.dump_bars(binFolder="../storage/his/min1/SHFE/", csvFolder="min1_csv")
    dtHelper.dump_ticks(binFolder="../storage/his/ticks/SHFE/20211227/", csvFolder="ticks_csv")
