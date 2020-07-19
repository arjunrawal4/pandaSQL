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
def agg_add(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    selection = type_df.sum()

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection

@reg
def agg_max(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    selection = type_df.max()

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection

@reg
def group(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    # modded = type_df['c1'] % 10
    # type_df['c1'] = modded
    selection = type_df.groupby('c0').mean()['c1']
    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection

@reg
def add(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))

    selection = type_df['c0'] + type_df['c1']
    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection

@reg
def multiplication(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    selection = type_df['c0'] * type_df['c1']
    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection

@reg
def division(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    selection = type_df['c0'] / type_df['c1']

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection


@reg
def filter(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    selection = type_df[type_df['c1'] == type_df['c2']]

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection


# @reg
# def statistics(module, type_df, n=None, **kwargs):
#     assert(isinstance(type_df, module.DataFrame))
#     if isinstance(type_df, pandas.DataFrame):
#         str(type_df.describe())
#     if isinstance(type_df, pandasql.DataFrame):
#         str(type_df.compute().describe())
#     return type_df

# @reg
# def selection(module, type_df, n=None, **kwargs):
#     assert(isinstance(type_df, module.DataFrame))


#     selection = type_df[type_df['c1'] >= 0 * 10**7]
#     selection = selection[selection['c1'] >= 1 * 10**7]
#     selection = selection[selection['c1'] >= 2 * 10**7]
#     selection = selection[selection['c1'] >= 3 * 10**7]
#     selection = selection[selection['c1'] >= 4 * 10**7]
#     selection = selection[selection['c1'] >= 5 * 10**7]
#     selection = selection[selection['c1'] >= 6 * 10**7]
#     selection = selection[selection['c1'] >= 7 * 10**7]
#     selection = selection[selection['c1'] >= 8 * 10**7]
#     selection = selection[selection['c1'] >= 9 * 10**7]
#     selection = selection[selection['c1'] >= 10 * 10**7]

#     if n is not None:
#         selection = selection.head(n=n)
#     str(selection)
#     return selection


def run_type_benchmark(benchmark, nrows, data_file, runs, limit=None,):

    func = globals()[benchmark]

    print(f"Reading {nrows} CSV rows, running {benchmark} with limit={limit}")

    stats = {"pandas": {}, "pandaSQL": {"sql": {}},
             "nrows": nrows, "benchmark": benchmark, "limit": limit}

    start = time.time()
    type_df = pandas.read_csv(data_file + '.csv',  nrows=nrows)

    time_taken = time.time() - start
    stats['pandas']['read_time'] = time_taken
    stats['pandas']['run_time'] = []

    for _ in range(0,runs):
        start = time.time()
        res = func(pandas, type_df, n=limit,)
        time_taken = time.time() - start
        del res
        gc.collect()
        stats['pandas']['run_time'].append(time_taken)

    start = time.time()

    typ = pandasql.DataFrame(type_df)

    del type_df
    gc.collect()

    time_taken = time.time() - start
    stats['pandaSQL']['read_time'] = time_taken
    stats['pandaSQL']['run_time'] = []
    stats['pandaSQL']['sql']['compute'] = []
    stats['pandaSQL']['sql']['read_out'] = []

    for _ in range(0,runs):
        start = time.time()
        res = func(pandasql, typ, n=limit,)
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
    parser.add_argument('--data', type=str, required=False, default='integer')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--runs', type=int, default=10)
    args = parser.parse_args()

    if args.benchmark == 'ALL':
        for f in reg.all:
            run_type_benchmark(f, args.nrows, args.data, args.runs, args.limit)
    else:
        run_type_benchmark(args.benchmark, args.nrows, args.data, args.runs, args.limit)
