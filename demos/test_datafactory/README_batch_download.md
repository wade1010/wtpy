# 期货主连K线数据批量下载工具

## 功能说明

这个工具可以通过天勤量化API批量下载所有期货主连合约的K线数据，并自动保存为dsb格式。

## 主要特性

- 🚀 **自动获取主连合约**: 通过天勤API自动获取所有交易所的期货主连合约列表
- 📊 **多周期支持**: 支持min1、min5、day等多种K线周期
- 🏢 **全交易所覆盖**: 支持上期所、中金所、大商所、郑商所、上海能源、广期所
- 🛡️ **错误处理**: 完善的异常处理和重试机制
- 📝 **详细日志**: 完整的下载进度和结果统计
- ⏱️ **频率控制**: 可配置下载间隔，避免请求过于频繁

## 使用方法

### 1. 基本用法

```python
from batch_download_futures import FuturesBatchDownloader
import datetime

# 创建下载器（请替换为你的天勤账号）
downloader = FuturesBatchDownloader()

# 初始化
downloader.init_helper()

# 设置时间范围
start_date = datetime.datetime(2023, 1, 1)
end_date = datetime.datetime(2025, 10, 11)

# 下载所有主连合约
downloader.batch_download_all_contracts(
    start_date=start_date,
    end_date=end_date,
    periods=["min1", "min5", "day"]
)
```

### 2. 下载指定合约

```python
# 只下载指定的几个合约
specific_contracts = [
    "DCE.jm.HOT",      # 焦煤主连
    "CFFEX.IF.HOT",    # 沪深300股指期货主连
    "CFFEX.IC.HOT",    # 中证500股指期货主连
    "SHFE.rb.HOT"      # 螺纹钢主连
]

downloader.download_specific_contracts(
    contract_codes=specific_contracts,
    start_date=start_date,
    end_date=end_date,
    periods=["min1", "day"]
)
```

### 3. 按交易所下载

```python
# 只下载特定交易所的合约
downloader.batch_download_all_contracts(
    start_date=start_date,
    end_date=end_date,
    exchanges=["DCE", "CFFEX"],  # 只下载大商所和中金所
    periods=["min5"]
)
```

## 支持的交易所

| 代码 | 交易所名称 |
|------|------------|
| SHFE | 上海期货交易所 |
| CFFEX | 中国金融期货交易所 |
| DCE | 大连商品交易所 |
| CZCE | 郑州商品交易所 |
| INE | 上海国际能源交易中心 |
| GFEX | 广州期货交易所 |

## 支持的K线周期

- `min1`: 1分钟K线
- `min5`: 5分钟K线  
- `day`: 日K线

## 配置参数

### FuturesBatchDownloader 参数

- `username`: 天勤账号用户名
- `password`: 天勤账号密码

### batch_download_all_contracts 参数

- `start_date`: 开始日期 (datetime对象)
- `end_date`: 结束日期 (datetime对象)
- `periods`: K线周期列表，默认 ["min1", "min5", "day"]
- `exchanges`: 交易所列表，默认所有交易所

### download_specific_contracts 参数

- `contract_codes`: 合约代码列表，如 ["DCE.jm.HOT"]
- `start_date`: 开始日期
- `end_date`: 结束日期
- `periods`: K线周期列表

## 数据存储

下载的数据会自动保存为dsb格式，存储路径由wtpy的配置决定。通常保存在：
- `../storage/his/min1/` - 1分钟数据
- `../storage/his/min5/` - 5分钟数据  
- `../storage/his/day/` - 日线数据

## 日志记录

程序会生成详细的日志记录：
- 控制台输出：实时显示下载进度
- 文件日志：保存到 `batch_download.log`

日志包含：
- 每个合约的下载状态
- 成功/失败统计
- 错误详情
- 总体完成情况

## 注意事项

1. **账号权限**: 需要有效的天勤量化账号
2. **网络稳定**: 确保网络连接稳定，下载大量数据需要时间
3. **存储空间**: 确保有足够的磁盘空间存储数据
4. **请求频率**: 建议设置适当的延时间隔，避免请求过于频繁
5. **数据范围**: 历史数据的可获取范围取决于天勤的数据权限

## 错误处理

程序包含完善的错误处理机制：
- 单个合约下载失败不会影响其他合约
- 自动记录失败原因
- 提供详细的成功率统计
- 支持断点续传（重新运行会跳过已存在的数据）

## 示例输出

```
2025-10-11 10:00:00 - INFO - 天勤SDK数据助手初始化成功
2025-10-11 10:00:01 - INFO - 开始获取期货主连合约列表...
2025-10-11 10:00:02 - INFO - DCE: 获取到 23 个主连合约
2025-10-11 10:00:03 - INFO - CFFEX: 获取到 6 个主连合约
...
2025-10-11 10:05:00 - INFO - ==================================================
2025-10-11 10:05:00 - INFO - 批量下载完成!
2025-10-11 10:05:00 - INFO - 总任务数: 174
2025-10-11 10:05:00 - INFO - 成功: 168
2025-10-11 10:05:00 - INFO - 失败: 6
2025-10-11 10:05:00 - INFO - 成功率: 96.55%
2025-10-11 10:05:00 - INFO - ==================================================
```
