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

def merge_size(left_frame, right_frame, group_by):
    left_groups = left_frame.groupby(group_by).size()
    right_groups = right_frame.groupby(group_by).size()
    left_keys = set(left_groups.index)
    right_keys = set(right_groups.index)
    intersection = right_keys & left_keys
    left_diff = left_keys - intersection
    right_diff = right_keys - intersection
    
    # if the joining column contains np.nan values, these get missed by the intersections
    # but are present in the merge. These need to be added separately.
    left_nan = len(left_frame.query('{0} != {0}'.format(group_by)))
    right_nan = len(right_frame.query('{0} != {0}'.format(group_by)))
    left_nan = 1 if left_nan == 0 and right_nan != 0 else left_nan
    right_nan = 1 if right_nan == 0 and left_nan != 0 else right_nan
    
    sizes = [(left_groups[group_name] * right_groups[group_name]) for group_name in intersection]
    sizes += [left_groups[group_name] for group_name in left_diff]
    sizes += [right_groups[group_name] for group_name in right_diff]
    sizes += [left_nan * right_nan]
    return sum(sizes)

@reg
def join(module, df_a, df_b, n=None, **kwargs):
    assert(isinstance(df_a, module.DataFrame))
    assert(isinstance(df_b, module.DataFrame))

    selection = module.merge(df_a, df_b, on=['c0'])

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection


def run_join_benchmark(benchmark, nrows, file_base, runs, percent=100, limit=None,):

    func = globals()[benchmark]

    print(f"Reading {nrows} CSV rows, running {benchmark} with limit={limit}")

    stats = {"pandas": {}, "pandaSQL": {"sql": {}},
             "nrows": nrows, "benchmark": benchmark, "limit": limit}

    start = time.time()
    df_a = pandas.read_csv(file_base + '_a.csv',  nrows=nrows)
    df_b = pandas.read_csv(file_base + '_b.csv',  nrows=nrows)
    
    # testing string join
    df_a['c0'] = df_a['c0'].astype(str)
    df_b['c0'] = df_b['c0'].astype(str)
    
    df_b = df_b[df_b['c1'] <= 100_000_000 * percent/100.0]
   
    time_taken = time.time() - start
    stats['pandas']['read_time'] = time_taken
    stats['pandas']['run_time'] = []

    start = time.time()
    print(merge_size(df_a, df_b, 'c0'))
    time_taken = time.time() - start
    print(time_taken) 
    
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

    ps_b = ps_b[ps_b['c1'] <= 100_000_000 * percent/100.0]
    str(ps_b)

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
    parser.add_argument('--percent', type=int, default=100)
    parser.add_argument('--runs', type=int, default=10)
    args = parser.parse_args()

    if args.benchmark == 'ALL':
        for f in reg.all:
            run_join_benchmark(f, args.nrows, args.data, args.runs, args.percent, args.limit)
    else:
        run_join_benchmark(args.benchmark, args.nrows, args.data, args.runs, args.percent, args.limit)
