from wtpy import WtDtServo

dtServo = WtDtServo()
dtServo.setBasefiles(folder="../common/")
dtServo.setStorage(path='../storage/')

# 读取IF主力合约的前复权数据
bars = dtServo.get_bars("CFFEX.IF.HOT", "m5", fromTime=201909100930, endTime=201912011500).to_df()
bars.to_csv("CFFEX.IF.HOT.csv")

# # 读取IF主力合约的后复权数据
# bars = dtServo.get_bars("CFFEX.IF.HOT+", "m5", fromTime=202205010930, endTime=202205281500).to_df()
# bars.to_csv("CFFEX.IF.HOT+.csv")
#
# # 读取IF主力合约的原始拼接数据
# bars = dtServo.get_bars("CFFEX.IF.HOT", "m5", fromTime=202205010930, endTime=202205281500).to_df()
# bars.to_csv("CFFEX.IF.HOT.csv")
#
# # 读取IF主力合约的tick数据
# bars = dtServo.get_ticks("CFFEX.IF.HOT", fromTime=202212210930, endTime=202212281500).to_df()
# bars.to_csv("CFFEX.IF.HOT_ticks.csv")



# bars = dtServo.get_bars("DCE.jm.HOT", "m1", fromTime=202501020900, endTime=0).to_df()
# bars.to_csv("DCE.jm.HOT.csv")