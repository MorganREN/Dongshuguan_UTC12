import polars as pl
import datetime as dt
import re


def convert_offset(offset_str):
    '''
    将offset字符串转换为datetime.time对象
    '''
    match = re.match(r"(?:(\d+)h)?(?:(\d+)m)?", offset_str)
    if match:
        hour = int(match.group(1)) if match.group(1) else 0
        minute = int(match.group(2)) if match.group(2) else 0
        return dt.time(hour, minute)
    else:
        raise ValueError(f"Invalid offset format: {offset_str}")


def desample_kline(data, interval='1d', offset='0'):
    '''
    降采样聚合kline数据
    '''
    desampled_kline = data.group_by_dynamic(
        index_column='open_time',
        every=interval,
        period=interval,
        offset=offset,
        closed='left',
        group_by='jj_code'
    ).agg([
        # pl.col('open_time').first().alias('open_time'),
        pl.col('close_time').last().alias('close_time'),
        pl.col('open').first().alias('open'),
        pl.col('high').max().alias('high'),
        pl.col('low').min().alias('low'),
        pl.col('close').last().alias('close'),
        pl.col('volume').sum().alias('volume'),
        pl.col('quote_volume').sum().alias('quote_volume'),
        pl.col('count').sum().alias('count'),
        pl.col('taker_buy_volume').sum().alias('taker_buy_volume'),
        pl.col('taker_buy_quote_volume').sum().alias('taker_buy_quote_volume')
    ])
    return desampled_kline


def kline_simulation(
        start_date:dt.date, # 指数据回放的起始日期
        db:pl.DataFrame, # 原始数据库
        offset:str = "5m", # 指数据回放的偏移量
        sub_interval_list:list = ["1d"], # 需要聚合出哪些颗粒度的kline
        time:str = "close_time",
        symbol:str = "jj_code"
):
    
    db = db.with_columns(pl.col('open_time').dt.date().alias('date'))

    # 转换成datetime.time对象
    start_time = convert_offset(offset)
    
    # 默认是必然会输出5m的原始颗粒度数据 迭代回放频率是1d ｜ 获取的数据是从start_date+偏移量开始的
    data = db.filter(
        pl.col(time) >= pl.lit(dt.datetime.combine(start_date,start_time))
    )
    # 这里要给data一列可以用以迭代的东西 比如offset的date
    for date,kline in data.group_by("date",maintain_order=True):  #  修改内容：对'open_time'进行分组，不知道是否会有问题
        res = {
            "5m":kline
        }

        for interval in sub_interval_list:
            res[interval] = desample_kline(kline,interval=interval, offset=offset)

        yield res