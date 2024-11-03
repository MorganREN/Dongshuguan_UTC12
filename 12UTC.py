import polars as pl
import datetime as dt

def kline_simulation(
        start_date:dt.date, # 指数据回放的起始日期
        start_time:dt.time, # 新添加内容：指数据回放的起始时间
        db:pl.DataFrame, # 原始数据库
        sub_interval_list:list = ["1d"], # 需要聚合出哪些颗粒度的kline
        time:str = "close_time",
        symbol:str = "jj_code"
):
    date_time = dt.datetime.combine(start_date, start_time)  # 新添加内容：将日期和时间合并为datetime
    # 默认是必然会输出5m的原始颗粒度数据 迭代回放频率是1d
    data = db.filter(
        (pl.col(time) >= date_time) & (pl.col(time) <= date_time + dt.timedelta(days=1))  # 修改内容：不确定回放频率
        # pl.col(time) >= start_date
    )
    # 这里要给data一列可以用以迭代的东西 比如offset的date
    for date,kline in data.group_by("open_time",maintain_order=True):  #  修改内容：对'open_time'进行分组，不知道是否会有问题
        res = {
            "5m":kline
        }
        # 以下内容目的不清晰
        if "1d" in sub_interval_list:
            res["1d"] = None
        if "1h" in sub_interval_list:
            res["5m"] = None
        yield res