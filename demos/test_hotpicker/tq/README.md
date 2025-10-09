# 天勤量化主力合约更新工具

这个工具用于通过天勤量化API更新主力合约文件（hots.json）。

## 功能特点

- 🔄 自动获取所有主连合约的历史数据
- 📊 更新主力合约切换记录
- 🎯 支持指定品种更新
- 📝 完整的日志记录
- ⚙️ 灵活的配置选项

## 安装依赖

```cmd
pip install tqsdk pandas python-dotenv
```

## 配置设置

1. 复制环境配置模板：
```cmd
copy env.example .env
```

2. 编辑 `.env` 文件，填入您的天勤账户信息：
```
TQ_USERNAME=your_tq_username
TQ_PASSWORD=your_tq_password
```

## 使用方法

### 1. 更新所有主力合约数据

```cmd
python TqCacheMonExchg.py
```

### 2. 更新指定品种

```cmd
python TqCacheMonExchg.py --products DCE.a SHFE.cu CFFEX.IC
```

### 3. 指定历史数据天数

```cmd
python TqCacheMonExchg.py --days 300
```

### 4. 指定hots.json文件路径

```cmd
python TqCacheMonExchg.py --hots-file "D:\path\to\your\hots.json"
```

### 5. 直接传入账户信息（不推荐）

```cmd
python TqCacheMonExchg.py --username your_username --password your_password
```

## 命令行参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--username` | `-u` | 天勤账户用户名 | 从环境变量获取 |
| `--password` | `-p` | 天勤账户密码 | 从环境变量获取 |
| `--hots-file` | `-f` | hots.json文件路径 | `../common/hots.json` |
| `--days` | `-d` | 获取历史数据天数 | 200 |
| `--products` | `-pr` | 指定品种列表 | 所有品种 |

## 支持的交易所

- **CFFEX**: 中金所
- **SHFE**: 上期所
- **DCE**: 大商所
- **CZCE**: 郑商所
- **INE**: 能源交易所
- **GFEX**: 广州期货交易所

## 输出格式

更新后的hots.json文件格式：

```json
{
    "CFFEX": {
        "IC": [
            {
                "date": 20190102,
                "from": "",
                "newclose": 4096.8,
                "oldclose": 0.0,
                "to": "IC1901"
            },
            {
                "date": 20190117,
                "from": "IC1901",
                "newclose": 4325.2,
                "oldclose": 4341.0,
                "to": "IC1902"
            }
        ]
    }
}
```

## 注意事项

1. **账户安全**: 请妥善保管您的天勤账户信息，不要将 `.env` 文件提交到版本控制系统
2. **API限制**: 天勤API可能有调用频率限制，大量数据更新时请注意
3. **数据准确性**: 工具会尽力获取准确的主力合约切换数据，但建议定期验证
4. **备份数据**: 在大规模更新前，建议备份原有的hots.json文件

## 错误处理

程序包含完整的错误处理机制：

- API连接失败时会自动重试
- 数据解析错误会记录详细日志
- 文件操作异常会给出明确提示

## 日志输出

程序运行时会输出详细的日志信息，包括：

- API连接状态
- 数据获取进度
- 更新结果统计
- 错误和警告信息

## 示例输出

```
2024-01-15 10:30:00 - TqHotContractUpdater - INFO - 天勤API连接成功
2024-01-15 10:30:01 - TqHotContractUpdater - INFO - 成功加载hots.json文件
2024-01-15 10:30:02 - TqHotContractUpdater - INFO - 获取到 156 个主连合约
2024-01-15 10:30:03 - TqHotContractUpdater - INFO - 正在更新 DCE.a 主力合约数据...
2024-01-15 10:30:04 - TqHotContractUpdater - INFO - 更新 DCE.a 主力合约数据，共 12 条记录
...
2024-01-15 10:35:00 - TqHotContractUpdater - INFO - 成功更新 45 个品种的主力合约数据
2024-01-15 10:35:01 - TqHotContractUpdater - INFO - 成功保存hots.json文件
```

## 技术支持

如遇到问题，请检查：

1. 天勤账户是否有效
2. 网络连接是否正常
3. 依赖包是否正确安装
4. 配置文件是否正确设置
