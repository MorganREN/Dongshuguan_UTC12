import polars as pl
import datetime as dt

def kline_simulation(
        start_date:dt.date, # 指数据回放的起始时间
        db:pl.DataFrame, # 原始数据库
        sub_interval_list:list = ["1d"], # 需要聚合出哪些颗粒度的kline
        time:str = "close_time",
        symbol:str = "jj_code"
):
    # 默认是必然会输出5m的原始颗粒度数据 迭代回放频率是1d
    data = db.filter(
        pl.col(time) >= start_date
    )
    # 这里要给data一列可以用以迭代的东西 比如offset的date
    for date,kline in data.group_by("date",maintain_order=True):
        res = {
            "5m":kline
        }
        if "1d" in sub_interval_list:
            res["1d"] = None
        if "1h" in sub_interval_list:
            res["5m"] = None
        yield res