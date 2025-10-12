from datetime import datetime, timedelta, time
import time as time_lib
import calendar
import pytz
from typing import Optional, Union, List, Tuple, Dict


class DatetimeUtils:
    """
    时间工具类，提供各种常用的时间处理方法
    """

    # 常用时区
    TIMEZONE_CN = pytz.timezone("Asia/Shanghai")  # 中国时区

    # 常用时间格式
    FORMAT_FULL = "%Y-%m-%d %H:%M:%S"  # 完整时间格式
    FORMAT_DATE = "%Y-%m-%d"  # 日期格式
    FORMAT_TIME = "%H:%M:%S"  # 时间格式
    FORMAT_COMPACT = "%Y%m%d%H%M%S"  # 紧凑格式
    FORMAT_ISO = "%Y-%m-%dT%H:%M:%S"  # ISO格式
    FORMAT_FULL_TICK = "%Y-%m-%d %H:%M:%S.%f"

    @staticmethod
    def now(tz: Optional[pytz.timezone] = TIMEZONE_CN) -> datetime:
        """
        获取当前时间
        
        参数:
            tz: 时区，默认为None，使用系统时区
            
        返回:
            datetime: 当前时间
        """
        if tz:
            return datetime.now(tz)
        return datetime.now()

    @staticmethod
    def utc_now() -> datetime:
        """
        获取当前UTC时间
        
        返回:
            datetime: 当前UTC时间
        """
        return datetime.now(pytz.UTC)

    @staticmethod
    def china_now() -> datetime:
        """
        获取当前中国时间
        
        返回:
            datetime: 当前中国时间
        """
        return datetime.now(DatetimeUtils.TIMEZONE_CN)

    @staticmethod
    def timestamp_to_datetime(timestamp: Union[int, float], tz: Optional[pytz.timezone] = TIMEZONE_CN) -> datetime:
        """
        时间戳转换为datetime对象
        
        参数:
            timestamp: 时间戳（秒）
            tz: 时区，默认为None，使用系统时区
            
        返回:
            datetime: 转换后的datetime对象
        """
        dt = datetime.fromtimestamp(timestamp)
        if tz:
            dt = dt.replace(tzinfo=tz)
        return dt

    @staticmethod
    def datetime_to_timestamp(dt: datetime) -> float:
        """
        datetime对象转换为时间戳
        
        参数:
            dt: datetime对象
            
        返回:
            float: 时间戳（秒）
        """
        return dt.timestamp()

    @staticmethod
    def millisecond_timestamp_to_datetime(ms_timestamp: Union[int, float],
                                          tz: Optional[pytz.timezone] = TIMEZONE_CN) -> datetime:
        """
        毫秒级时间戳转换为datetime对象
        
        参数:
            ms_timestamp: 毫秒级时间戳
            tz: 时区，默认为None，使用系统时区
            
        返回:
            datetime: 转换后的datetime对象
        """
        dt = datetime.fromtimestamp(ms_timestamp / 1000)
        if tz:
            dt = dt.replace(tzinfo=tz)
        return dt

    @staticmethod
    def datetime_to_millisecond_timestamp(dt: datetime) -> int:
        """
        datetime对象转换为毫秒级时间戳
        
        参数:
            dt: datetime对象
            
        返回:
            int: 毫秒级时间戳
        """
        return int(dt.timestamp() * 1000)

    @staticmethod
    def str_to_datetime(date_str: str, fmt: str = FORMAT_FULL, tz: Optional[pytz.timezone] = TIMEZONE_CN) -> datetime:
        """
        字符串转换为datetime对象
        
        参数:
            date_str: 日期字符串
            fmt: 日期格式，默认为"%Y-%m-%d %H:%M:%S"
            tz: 时区，默认为None
            
        返回:
            datetime: 转换后的datetime对象
        """
        dt = datetime.strptime(date_str, fmt)
        if tz:
            dt = dt.replace(tzinfo=tz)
        return dt

    @staticmethod
    def datetime_to_str(dt: datetime, fmt: str = FORMAT_FULL) -> str:
        """
        datetime对象转换为字符串
        
        参数:
            dt: datetime对象
            fmt: 日期格式，默认为"%Y-%m-%d %H:%M:%S"
            
        返回:
            str: 转换后的字符串
        """
        return dt.strftime(fmt)

    def nanosecond_to_str(ns_timestamp: Union[int, float], fmt: str = FORMAT_FULL,
                          tz: Optional[pytz.timezone] = TIMEZONE_CN) -> str:
        """
        纳秒级时间戳转换为datetime对象

        参数:
            ns_timestamp: 纳秒级时间戳
            tz: 时区，默认为TIMEZONE_CN（中国时区）

        返回:
            datetime: 转换后的datetime对象
        """
        # 将纳秒转换为秒（除以1e9）
        dt = datetime.fromtimestamp(ns_timestamp / 1e9)
        if tz:
            dt = dt.replace(tzinfo=tz)
        return dt.strftime(fmt)

    @staticmethod
    def convert_timezone(dt: datetime, target_tz: pytz.timezone) -> datetime:
        """
        转换时区
        
        参数:
            dt: datetime对象
            target_tz: 目标时区
            
        返回:
            datetime: 转换后的datetime对象
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        return dt.astimezone(target_tz)

    @staticmethod
    def is_same_day(dt1: datetime, dt2: datetime) -> bool:
        """
        判断两个datetime对象是否为同一天
        
        参数:
            dt1: 第一个datetime对象
            dt2: 第二个datetime对象
            
        返回:
            bool: 是否为同一天
        """
        return dt1.date() == dt2.date()

    @staticmethod
    def add_days(dt: datetime, days: int) -> datetime:
        """
        添加天数
        
        参数:
            dt: datetime对象
            days: 天数，可为负数
            
        返回:
            datetime: 添加天数后的datetime对象
        """
        return dt + timedelta(days=days)

    @staticmethod
    def add_hours(dt: datetime, hours: int) -> datetime:
        """
        添加小时
        
        参数:
            dt: datetime对象
            hours: 小时数，可为负数
            
        返回:
            datetime: 添加小时后的datetime对象
        """
        return dt + timedelta(hours=hours)

    @staticmethod
    def add_minutes(dt: datetime, minutes: int) -> datetime:
        """
        添加分钟
        
        参数:
            dt: datetime对象
            minutes: 分钟数，可为负数
            
        返回:
            datetime: 添加分钟后的datetime对象
        """
        return dt + timedelta(minutes=minutes)

    @staticmethod
    def add_seconds(dt: datetime, seconds: int) -> datetime:
        """
        添加秒数
        
        参数:
            dt: datetime对象
            seconds: 秒数，可为负数
            
        返回:
            datetime: 添加秒数后的datetime对象
        """
        return dt + timedelta(seconds=seconds)

    @staticmethod
    def get_date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
        """
        获取日期范围
        
        参数:
            start_date: 开始日期
            end_date: 结束日期
            
        返回:
            List[datetime]: 日期列表
        """
        date_list = []
        current_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        while current_date <= end_date:
            date_list.append(current_date)
            current_date = DatetimeUtils.add_days(current_date, 1)

        return date_list

    @staticmethod
    def get_today_start_end(tz: Optional[pytz.timezone] = TIMEZONE_CN) -> Tuple[datetime, datetime]:
        """
        获取今天的开始和结束时间
        
        参数:
            tz: 时区，默认为None，使用系统时区
            
        返回:
            Tuple[datetime, datetime]: 今天的开始和结束时间
        """
        now = DatetimeUtils.now(tz)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end

    @staticmethod
    def get_yesterday_start_end(tz: Optional[pytz.timezone] = TIMEZONE_CN) -> Tuple[datetime, datetime]:
        """
        获取昨天的开始和结束时间
        
        参数:
            tz: 时区，默认为None，使用系统时区
            
        返回:
            Tuple[datetime, datetime]: 昨天的开始和结束时间
        """
        now = DatetimeUtils.now(tz)
        yesterday = DatetimeUtils.add_days(now, -1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end

    @staticmethod
    def get_week_start_end(dt: Optional[datetime] = None, tz: Optional[pytz.timezone] = TIMEZONE_CN) -> Tuple[
        datetime, datetime]:
        """
        获取指定日期所在周的开始和结束时间（周一为一周的开始）
        
        参数:
            dt: datetime对象，默认为None，表示当前时间
            tz: 时区，默认为None，使用系统时区
            
        返回:
            Tuple[datetime, datetime]: 周的开始和结束时间
        """
        if dt is None:
            dt = DatetimeUtils.now(tz)

        start = dt - timedelta(days=dt.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)

        end = start + timedelta(days=6)
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)

        return start, end

    @staticmethod
    def get_month_start_end(dt: Optional[datetime] = None, tz: Optional[pytz.timezone] = TIMEZONE_CN) -> Tuple[
        datetime, datetime]:
        """
        获取指定日期所在月的开始和结束时间
        
        参数:
            dt: datetime对象，默认为None，表示当前时间
            tz: 时区，默认为None，使用系统时区
            
        返回:
            Tuple[datetime, datetime]: 月的开始和结束时间
        """
        if dt is None:
            dt = DatetimeUtils.now(tz)

        start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # 获取当月的最后一天
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        end = dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)

        return start, end

    @staticmethod
    def get_quarter_start_end(dt: Optional[datetime] = None, tz: Optional[pytz.timezone] = TIMEZONE_CN) -> Tuple[
        datetime, datetime]:
        """
        获取指定日期所在季度的开始和结束时间
        
        参数:
            dt: datetime对象，默认为None，表示当前时间
            tz: 时区，默认为None，使用系统时区
            
        返回:
            Tuple[datetime, datetime]: 季度的开始和结束时间
        """
        if dt is None:
            dt = DatetimeUtils.now(tz)

        quarter = (dt.month - 1) // 3 + 1
        start_month = (quarter - 1) * 3 + 1
        end_month = quarter * 3

        start = dt.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)

        # 获取季度最后一个月的最后一天
        last_day = calendar.monthrange(dt.year, end_month)[1]
        end = dt.replace(month=end_month, day=last_day, hour=23, minute=59, second=59, microsecond=999999)

        return start, end

    @staticmethod
    def get_year_start_end(dt: Optional[datetime] = None, tz: Optional[pytz.timezone] = TIMEZONE_CN) -> Tuple[
        datetime, datetime]:
        """
        获取指定日期所在年的开始和结束时间
        
        参数:
            dt: datetime对象，默认为None，表示当前时间
            tz: 时区，默认为None，使用系统时区
            
        返回:
            Tuple[datetime, datetime]: 年的开始和结束时间
        """
        if dt is None:
            dt = DatetimeUtils.now(tz)

        start = dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = dt.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)

        return start, end

    @staticmethod
    def is_weekend(dt: datetime) -> bool:
        """
        判断是否为周末（周六或周日）
        
        参数:
            dt: datetime对象
            
        返回:
            bool: 是否为周末
        """
        return dt.weekday() >= 5  # 5=周六，6=周日

    @staticmethod
    def is_workday(dt: datetime) -> bool:
        """
        判断是否为工作日（周一至周五）
        
        参数:
            dt: datetime对象
            
        返回:
            bool: 是否为工作日
        """
        return dt.weekday() < 5  # 0-4为周一至周五

    @staticmethod
    def get_days_diff(dt1: datetime, dt2: datetime) -> int:
        """
        计算两个日期之间的天数差
        
        参数:
            dt1: 第一个datetime对象
            dt2: 第二个datetime对象
            
        返回:
            int: 天数差
        """
        delta = dt2.date() - dt1.date()
        return delta.days

    @staticmethod
    def get_seconds_diff(dt1: datetime, dt2: datetime) -> float:
        """
        计算两个时间之间的秒数差
        
        参数:
            dt1: 第一个datetime对象
            dt2: 第二个datetime对象
            
        返回:
            float: 秒数差
        """
        delta = dt2 - dt1
        return delta.total_seconds()

    @staticmethod
    def get_age(birth_date: datetime, reference_date: Optional[datetime] = None) -> int:
        """
        计算年龄
        
        参数:
            birth_date: 出生日期
            reference_date: 参考日期，默认为当前日期
            
        返回:
            int: 年龄
        """
        if reference_date is None:
            reference_date = DatetimeUtils.now()

        age = reference_date.year - birth_date.year

        # 检查生日是否已过
        if (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day):
            age -= 1

        return age

    @staticmethod
    def is_leap_year(year: int) -> bool:
        """
        判断是否为闰年
        
        参数:
            year: 年份
            
        返回:
            bool: 是否为闰年
        """
        return calendar.isleap(year)

    @staticmethod
    def get_days_in_month(year: int, month: int) -> int:
        """
        获取指定年月的天数
        
        参数:
            year: 年份
            month: 月份
            
        返回:
            int: 天数
        """
        return calendar.monthrange(year, month)[1]

    @staticmethod
    def get_day_of_year(dt: datetime) -> int:
        """
        获取指定日期是一年中的第几天
        
        参数:
            dt: datetime对象
            
        返回:
            int: 天数
        """
        return dt.timetuple().tm_yday

    @staticmethod
    def get_week_of_year(dt: datetime) -> int:
        """
        获取指定日期是一年中的第几周
        
        参数:
            dt: datetime对象
            
        返回:
            int: 周数
        """
        return dt.isocalendar()[1]

    @staticmethod
    def format_time_delta(seconds: float) -> str:
        """
        格式化时间差
        
        参数:
            seconds: 秒数
            
        返回:
            str: 格式化后的时间差，如"2小时3分钟5秒"
        """
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if hours:
            parts.append(f"{int(hours)}小时")
        if minutes:
            parts.append(f"{int(minutes)}分钟")
        if seconds or not parts:
            parts.append(f"{int(seconds)}秒")

        return "".join(parts)

    @staticmethod
    def sleep(seconds: float) -> None:
        """
        线程休眠
        
        参数:
            seconds: 休眠秒数
        """
        time_lib.sleep(seconds)

    @staticmethod
    def round_datetime(dt: datetime, round_to: str = "minute") -> datetime:
        """
        对时间进行舍入
        
        参数:
            dt: datetime对象
            round_to: 舍入单位，可选值为"day", "hour", "minute", "second"
            
        返回:
            datetime: 舍入后的datetime对象
        """
        if round_to == "day":
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif round_to == "hour":
            return dt.replace(minute=0, second=0, microsecond=0)
        elif round_to == "minute":
            return dt.replace(second=0, microsecond=0)
        elif round_to == "second":
            return dt.replace(microsecond=0)
        else:
            raise ValueError("round_to must be one of 'day', 'hour', 'minute', 'second'")

    @staticmethod
    def is_trading_day(dt: datetime) -> bool:
        """
        判断是否为交易日（非周末且非法定假日）
        注意：此方法仅判断是否为周末，不包含法定假日的判断，需要扩展
        
        参数:
            dt: datetime对象
            
        返回:
            bool: 是否为交易日
        """
        # 这里只判断是否为周末，实际使用时需要加入法定假日的判断
        return not DatetimeUtils.is_weekend(dt)

    @staticmethod
    def get_trading_days(start_date: datetime, end_date: datetime) -> List[datetime]:
        """
        获取交易日列表（非周末且非法定假日）
        注意：此方法仅排除周末，不包含法定假日的判断，需要扩展
        
        参数:
            start_date: 开始日期
            end_date: 结束日期
            
        返回:
            List[datetime]: 交易日列表
        """
        all_days = DatetimeUtils.get_date_range(start_date, end_date)
        trading_days = [day for day in all_days if DatetimeUtils.is_trading_day(day)]
        return trading_days

    @staticmethod
    def get_next_trading_day(dt: datetime) -> datetime:
        """
        获取下一个交易日
        注意：此方法仅考虑周末，不包含法定假日的判断，需要扩展
        
        参数:
            dt: datetime对象
            
        返回:
            datetime: 下一个交易日
        """
        next_day = DatetimeUtils.add_days(dt, 1)
        while not DatetimeUtils.is_trading_day(next_day):
            next_day = DatetimeUtils.add_days(next_day, 1)
        return next_day

    @staticmethod
    def get_previous_trading_day(dt: datetime) -> datetime:
        """
        获取上一个交易日
        注意：此方法仅考虑周末，不包含法定假日的判断，需要扩展
        
        参数:
            dt: datetime对象
            
        返回:
            datetime: 上一个交易日
        """
        prev_day = DatetimeUtils.add_days(dt, -1)
        while not DatetimeUtils.is_trading_day(prev_day):
            prev_day = DatetimeUtils.add_days(prev_day, -1)
        return prev_day

    @staticmethod
    def get_trading_periods() -> Dict[str, Tuple[time, time]]:
        """
        获取交易时段
        注意：这里仅提供中国A股的交易时段示例，实际使用时需要根据不同市场进行调整
        
        返回:
            Dict[str, Tuple[time, time]]: 交易时段字典，键为时段名称，值为(开始时间, 结束时间)
        """
        # 中国A股交易时段
        return {
            "morning": (time(9, 30), time(11, 30)),
            "afternoon": (time(13, 0), time(15, 0))
        }

    @staticmethod
    def is_in_trading_period(dt: datetime, periods: Optional[Dict[str, Tuple[time, time]]] = None) -> bool:
        """
        判断是否在交易时段内
        
        参数:
            dt: datetime对象
            periods: 交易时段字典，默认为None，使用get_trading_periods()返回的时段
            
        返回:
            bool: 是否在交易时段内
        """
        if not DatetimeUtils.is_trading_day(dt):
            return False

        if periods is None:
            periods = DatetimeUtils.get_trading_periods()

        current_time = dt.time()

        for period_name, (start_time, end_time) in periods.items():
            if start_time <= current_time <= end_time:
                return True

        return False

    @staticmethod
    def get_current_trading_period(dt: datetime, periods: Optional[Dict[str, Tuple[time, time]]] = None) -> Optional[
        str]:
        """
        获取当前所在的交易时段名称
        
        参数:
            dt: datetime对象
            periods: 交易时段字典，默认为None，使用get_trading_periods()返回的时段
            
        返回:
            Optional[str]: 交易时段名称，如果不在交易时段内则返回None
        """
        if not DatetimeUtils.is_trading_day(dt):
            return None

        if periods is None:
            periods = DatetimeUtils.get_trading_periods()

        current_time = dt.time()

        for period_name, (start_time, end_time) in periods.items():
            if start_time <= current_time <= end_time:
                return period_name

        return None

    @staticmethod
    def get_time_to_next_trading_period(dt: datetime, periods: Optional[Dict[str, Tuple[time, time]]] = None) -> \
            Optional[timedelta]:
        """
        获取距离下一个交易时段的时间
        
        参数:
            dt: datetime对象
            periods: 交易时段字典，默认为None，使用get_trading_periods()返回的时段
            
        返回:
            Optional[timedelta]: 距离下一个交易时段的时间，如果当前已在交易时段内则返回None
        """
        if periods is None:
            periods = DatetimeUtils.get_trading_periods()

        if DatetimeUtils.is_in_trading_period(dt, periods):
            return None

        current_time = dt.time()
        current_date = dt.date()

        # 按时间顺序排序交易时段
        sorted_periods = sorted(periods.items(), key=lambda x: x[1][0])

        # 检查当天的剩余交易时段
        for period_name, (start_time, end_time) in sorted_periods:
            if current_time < start_time:
                next_period_start = datetime.combine(current_date, start_time)
                if dt.tzinfo:
                    next_period_start = next_period_start.replace(tzinfo=dt.tzinfo)
                return next_period_start - dt

        # 如果当天没有剩余交易时段，则查找下一个交易日的第一个时段
        next_trading_day = DatetimeUtils.get_next_trading_day(dt)
        next_period_start = datetime.combine(next_trading_day.date(), sorted_periods[0][1][0])
        if dt.tzinfo:
            next_period_start = next_period_start.replace(tzinfo=dt.tzinfo)
        return next_period_start - dt

    @staticmethod
    def wt_make_time(date_num: int, time_num: int):
        """
        将wondertrader的时间转成datetime\n
        @date   日期，格式如20200723\n
        @time   时间，精确到分，格式如0935\n
        """
        return datetime(year=int(date_num / 10000), month=int(date_num % 10000 / 100), day=date_num % 100, hour=int(time_num / 100), minute=time_num % 100)
