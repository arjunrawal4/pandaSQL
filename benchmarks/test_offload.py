import time
import json
import argparse

import pandas
import pandasql

def agg_add(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    selection = type_df.sum()

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection

def agg_max(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    selection = type_df.max()

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection

def add(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    type_df['c1'] += type_df['c2'] + type_df['c3']

    if n is not None:
        type_df = type_df.head(n=n)
    str(type_df)
    return type_df

def multiplication(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    type_df['c1'] /= type_df['c2']

    if n is not None:
        type_df = type_df.head(n=n)
    str(type_df)
    return type_df

def division(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    type_df['c1'] /= type_df['c2']

    if n is not None:
        type_df = type_df.head(n=n)
    str(type_df)
    return type_df

def statistics(module, type_df, n=None, **kwargs):
    assert(isinstance(type_df, module.DataFrame))
    if isinstance(type_df, pandas.DataFrame):
        print(type_df.describe())
    if isinstance(type_df, pandasql.DataFrame):
        print(type_df.compute().describe())

def selection(module, authors, books, n=None, **kwargs):
    assert(isinstance(authors, module.DataFrame))
    assert(isinstance(books, module.DataFrame))

    selection = books[books['publication_year'] > 2000]
    selection = selection[selection['publication_year'] >= 2001]
    selection = selection[selection['publication_year'] >= 2002]
    selection = selection[selection['publication_year'] >= 2003]
    selection = selection[selection['publication_year'] >= 2004]
    selection = selection[selection['publication_year'] >= 2005]
    selection = selection[selection['publication_year'] >= 2006]
    selection = selection[selection['publication_year'] >= 2007]
    selection = selection[selection['publication_year'] >= 2008]
    selection = selection[selection['publication_year'] >= 2009]
    selection = selection[selection['publication_year'] >= 2010]
    selection = selection[selection['publication_year'] >= 2011]
    selection = selection[selection['publication_year'] >= 2012]
    selection = selection[selection['publication_year'] >= 2013]
    selection = selection[selection['publication_year'] >= 2014]
    # selection = selection[selection['publication_year'] >= 2015]
    # selection = selection[selection['publication_year'] >= 2016]
    # selection = selection[selection['publication_year'] >= 2017]
    # selection = selection[selection['publication_year'] >= 2018]
    # selection = selection[selection['publication_year'] >= 2019]
    # selection = selection[selection['publication_year'] >= 2020]

    if n is not None:
        selection = selection.head(n=n)
    str(selection)
    return selection


def join(module, authors, books, n=None, **kwargs):
    assert(isinstance(authors, module.DataFrame))
    assert(isinstance(books, module.DataFrame))

    merged = module.merge(books, authors, on=['first_name', 'last_name'])

    if n is not None:
        merged = merged.head(n=n)

    str(merged)
    return merged


def run_benchmark(benchmark, nrows, limit=None):

    func = globals()[benchmark]

    print(f"Reading {nrows} CSV rows, running {benchmark} with limit={limit}")

    stats = {"pandas": {}, "pandaSQL": {}, "dask": {},
             "nrows": nrows, "benchmark": benchmark, "limit": limit}

    start = time.time()
    authors_df = pandas.read_csv('authors.csv')
    books_df = pandas.read_csv('books.csv', nrows=nrows)

    time_taken = time.time() - start
    stats['pandas']['read_time'] = time_taken
    print("[Pandas]   Time taken to read: {:0.3f} seconds".format(time_taken))

    start = time.time()
    func(pandas, authors_df, books_df, n=limit,)
    time_taken = time.time() - start
    stats['pandas']['run_time'] = time_taken
    print("[Pandas]   Time taken to run:  {:0.3f} seconds".format(time_taken))

    start = time.time()
    # authors = pandasql.read_csv('authors.csv')
    # books = pandasql.read_csv('books.csv', nrows=nrows)

    authors = pandasql.DataFrame(authors_df)
    books = pandasql.DataFrame(books_df)

    time_taken = time.time() - start
    stats['pandaSQL']['read_time'] = time_taken
    print("[PandaSQL] Time taken to read: {:0.3f} seconds".format(time_taken))

    start = time.time()
    func(pandasql, authors, books, n=limit,)
    time_taken = time.time() - start
    stats['pandaSQL']['run_time'] = time_taken
    print("[PandaSQL] Time taken to run:  {:0.3f} seconds".format(time_taken))


    print(json.dumps(stats, indent=4))



def run_type_benchmark(benchmark, nrows, data_file, limit=None,):

    func = globals()[benchmark]

    print(f"Reading {nrows} CSV rows, running {benchmark} with limit={limit}")

    stats = {"pandas": {}, "pandaSQL": {"sql": {}}, "dask": {},
             "nrows": nrows, "benchmark": benchmark, "limit": limit}

    start = time.time()
    type_df = pandas.read_csv(data_file + '.csv',  nrows=nrows)

    time_taken = time.time() - start
    stats['pandas']['read_time'] = time_taken

    start = time.time()
    func(pandas, type_df, n=limit,)
    time_taken = time.time() - start
    stats['pandas']['run_time'] = time_taken

    start = time.time()

    typ = pandasql.DataFrame(type_df)

    time_taken = time.time() - start
    stats['pandaSQL']['read_time'] = time_taken

    start = time.time()
    res = func(pandasql, typ, n=limit,)
    time_taken = time.time() - start
    stats['pandaSQL']['run_time'] = time_taken

    if hasattr(res, '_sql_timings'):
        stats['pandaSQL']['sql']['compute'] = res._sql_timings['compute']
        stats['pandaSQL']['sql']['read_out'] = res._sql_timings['read']

    print(json.dumps(stats, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--nrows', type=int, default=10**3, required=True)
    parser.add_argument('--benchmark', type=str, required=True)
    parser.add_argument('--data', type=str, required=False, default='integer')
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()

    # run_benchmark(args.benchmark, args.nrows, args.limit)
    run_type_benchmark(args.benchmark, args.nrows, args.data, args.limit)