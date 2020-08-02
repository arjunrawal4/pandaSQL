import pandas as pd
import subprocess
import time
import gc

from pandasql.core import DataFrame, SQL_CON, DB_FILE
from pandasql.utils import _new_name


def read_csv(*args, name=None, **kwargs):
    chunksize = 10 ** 5
    name = name or _new_name()

    start = time.time()
    kwargs['chunksize'] = None
    kwargs['nrows'] = 100
    df = pd.read_csv(*args, **kwargs)
    df.to_sql(name=name, con=SQL_CON, index=False, if_exists='append')
    subprocess.call(["sqlite3", DB_FILE, ".mode csv", f".import {args[0]} {name}"])
    t0 = time.time() - start

    kwargs['chunksize'] = chunksize
    kwargs['nrows'] = None
    for chunk in pd.read_csv(*args, **kwargs):
        chunk.to_sql(name=name+'_1', con=SQL_CON, index=False, if_exists='append')
    t1 = time.time() - start
    gc.collect()


    start = time.time()
    kwargs['chunksize'] = None
    kwargs['nrows'] = None
    df = pd.read_csv(*args, **kwargs)
    df.to_sql(name=name+'_2', con=SQL_CON, index=False, if_exists='append')
    t2 = time.time() - start

    print(t0,t1,t2)
    return DataFrame(df, name=name, offload_needed=False)


def read_json(*args, name=None, **kwargs):
    return DataFrame(pd.read_json(*args, **kwargs), name=name)


def read_numpy(*args, name=None, **kwargs):
    return DataFrame(pd.read_numpy(*args, **kwargs), name=name)


def read_pickle(*args, name=None, **kwargs):
    return DataFrame(pd.read_pickle(*args, **kwargs), name=name)
