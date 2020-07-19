import time
import json
import argparse

import pandas
import pandasql
import gc

def make_registrar():
    registry = []
    def registrar(func):
        registry.append(func.__name__)
        return func
    registrar.all = registry
    return registrar

reg = make_registrar()


@reg
def join(module, df_a, df_b, n=None, **kwargs):
    assert(isinstance(df_a, module.DataFrame))
    assert(isinstance(df_b, module.DataFrame))

    selection = module.merge(df_a, df_b, on=['c1'])

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection


def run_join_benchmark(benchmark, nrows, file_base, runs, limit=None,):

    func = globals()[benchmark]

    print(f"Reading {nrows} CSV rows, running {benchmark} with limit={limit}")

    stats = {"pandas": {}, "pandaSQL": {"sql": {}},
             "nrows": nrows, "benchmark": benchmark, "limit": limit}

    start = time.time()
    df_a = pandas.read_csv(file_base + '_a.csv',  nrows=nrows)
    df_b = pandas.read_csv(file_base + '_b.csv',  nrows=nrows)

    time_taken = time.time() - start
    stats['pandas']['read_time'] = time_taken
    stats['pandas']['run_time'] = []

    for _ in range(0,runs):
        start = time.time()
        res = func(pandas, df_a, df_b, n=limit,)
        time_taken = time.time() - start
        del res
        gc.collect()
        stats['pandas']['run_time'].append(time_taken)

    start = time.time()

    ps_a = pandasql.DataFrame(df_a)
    ps_b = pandasql.DataFrame(df_a)

    del df_a
    del df_b
    gc.collect()

    time_taken = time.time() - start
    stats['pandaSQL']['read_time'] = time_taken
    stats['pandaSQL']['run_time'] = []
    stats['pandaSQL']['sql']['compute'] = []
    stats['pandaSQL']['sql']['read_out'] = []

    for _ in range(0,runs):
        start = time.time()
        res = func(pandasql, ps_a, ps_b, n=limit,)
        time_taken = time.time() - start
        stats['pandaSQL']['run_time'].append(time_taken)

        if hasattr(res, '_sql_timings'):
            stats['pandaSQL']['sql']['compute'].append(res._sql_timings['compute'])
            stats['pandaSQL']['sql']['read_out'].append(res._sql_timings['read'])
        del res
        gc.collect()

    print(json.dumps({str(benchmark):stats}, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--nrows', type=int, default=10**3, required=True)
    parser.add_argument('--benchmark', type=str, required=True)
    parser.add_argument('--data', type=str, required=False, default='int_join')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--runs', type=int, default=10)
    args = parser.parse_args()

    if args.benchmark == 'ALL':
        for f in reg.all:
            run_join_benchmark(f, args.nrows, args.data, args.runs, args.limit)
    else:
        run_join_benchmark(args.benchmark, args.nrows, args.data, args.runs, args.limit)
