import os
from typing import List
from tempfile import mkstemp
import pandas as pd

from pandasql.graph_utils import _get_dependency_graph, _topological_sort, \
    _filter_ancestors
from pandasql.sql_utils import get_sqlite_connection
from pandasql.cost_model import CostModel
from pandasql.memory_utils import _free_memory, \
    _estimate_pandas_memory_from_sqlite
from pandasql.api_status import SUPPORTED_VIA_FALLBACK

DB_FILE = mkstemp("_pandasql.db")[1]
SQL_CON = get_sqlite_connection(DB_FILE)
OFFLOADING_STRATEGY = None
SQLITE_CHUNK_SIZE = 10000
COST_MODEL = CostModel()


def require_result(func):
    '''Decorator for functions that require results to be ready'''

    def result_ensured(self, *args, **kwargs):
        self.compute()
        return func(self, *args, **kwargs)

    return result_ensured


class BaseFrame(object):
    def __init__(self, name=None, sources=None, pandas_sources=None):
        self.name = name or _new_name()
        # TODO: deduplicate sources
        self.sources = sources or []
        self.pandas_sources = pandas_sources or []
        self.dependents = []
        self._cached_result = None
        self._cached_on_sqlite = False
        self._computed_on_pandas = False
        self._out_of_memory = False
        self.update = None
        self.columns = pd.Index([])
        self._sql_query = None
        self._memory_usage = None
        self.stats = None

        # For each source, add this as a dependent
        for source in self.sources:
            source.dependents.append(self)

    @property
    def result(self):
        if self._cached_result is None:
            if self._out_of_memory:
                raise MemoryError('The result of this dataframe is too big to '
                                  'fit in memory. Please try accessing a '
                                  'smaller subset of the data you need, '
                                  'e.g., using df.head().')
            else:
                return None
        elif hasattr(self, 'process_result') and not self._computed_on_pandas:
            # If result was not computed on Pandas, post-process result
            return self.process_result(self._cached_result)
        else:
            return self._cached_result

    @property
    def memory_usage(self):
        if self._memory_usage is None:
            if isinstance(self._cached_result, pd.DataFrame):
                self._memory_usage = self._cached_result.memory_usage(
                    deep=True, index=True).sum()
            elif isinstance(self._cached_result, pd.Series):
                self._memory_usage = self._cached_result.memory_usage(
                    deep=True, index=True)
            else:
                # TODO: handle Aggregator, GroupByDataFrame, GroupByProjection
                self._memory_usage = None
        return self._memory_usage

    def compute(self):
        # TODO: explore if there are situations when some part of the
        # computation should be offloaded, whereas others should not.

        if self._cached_result is None:
            # TODO: If A is computed on Pandas, and B depends on A and
            # is about to be computed on SQLite, should A be computed as
            # part of B's SQL query, or should A's result be first transferred
            # to SQLite? Probably not one-size-fits-all. Currently, the former
            # occurs every time.
            if should_offload_computation(self):
                _ensure_computable(self, on='sqlite')
                self._compute_sqlite()
            else:
                _ensure_computable(self, on='pandas')
                self._compute_pandas()

            # Compute stats about result, if it exists on Pandas
            if isinstance(self._cached_result, pd.DataFrame):
                self.stats = collect_stats(self._cached_result)

        return self.result

    def _compute_pandas(self, offload=False):
        if self._cached_result is None:
            graph = _get_dependency_graph(self, on='pandas')
            ordered_deps = _topological_sort(graph)

            # Compute all dependencies in order (via Pandas)
            for t in ordered_deps:
                if t is not self and t.result is None \
                        and isinstance(t, BaseFrame):
                    t.compute()

            # Finally, compute this object's result
            if self.update is None:
                result = self._pandas()
            else:   # Trigger the lazy write that is pending for this object
                result = self.update.source.result.copy(deep=True)
                result[self.update.col] = self.update.value.result

            # TODO(important): when should this result be offloaded to SQLite?
            self._cached_result = result
            self._computed_on_pandas = True

            if offload:
                self._cached_on_sqlite = True
                result.to_sql(name=self.name, con=SQL_CON, index=False)

        return self.result

    def _compute_sqlite(self):

        if not self._cached_on_sqlite:
            # Compute result and store in SQLite table
            query = self.sql(dependencies=True)
            compute_query = 'CREATE TABLE {} AS {}'.format(self.name, query)
            SQL_CON.execute(compute_query)
            self._cached_on_sqlite = True

        if self._cached_result is None:

            estimated = _estimate_pandas_memory_from_sqlite(self.name)
            if estimated > _free_memory():
                # Cannot bring back result because it's too big for memory
                self._out_of_memory = True
            else:
                # Read table as Pandas DataFrame
                read_query = 'SELECT * FROM {}'.format(self.name)
                self._cached_result = pd.read_sql_query(
                    read_query, con=SQL_CON)
                self.columns = self._cached_result.columns

        return self.result

    def _pandas(self):
        raise NotImplementedError("To be implemented by subclasses")

    def sql(self, dependencies=True):
        query = []

        if dependencies:
            common_table_expr = _define_dependencies(self)
            if common_table_expr is not None:
                query.append(common_table_expr)

        if self.update is None:
            query.append(self._sql_query)
        else:
            query.append(self.update._sql_query)

        return ' '.join(query)

    def sum(self):
        return Aggregator('sum', self)

    def mean(self):
        return Aggregator('mean', self)

    def count(self):
        return Aggregator('count', self)

    def min(self):
        return Aggregator('min', self)

    def max(self):
        return Aggregator('max', self)

    def prod(self):
        return Aggregator('prod', self)

    def any(self):
        return Aggregator('any', self)

    def all(self):
        return Aggregator('all', self)

    def __getattr__(self, attr):
        if attr in SUPPORTED_VIA_FALLBACK:
            def wrapped_op(*args, **kwargs):
                return FallbackOperation(*args, source=self, op=attr, **kwargs)
            return wrapped_op

        elif attr in self.columns:
            return self[attr]

        else:
            return self.__getattribute__(attr)

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.name)


class Constant(BaseFrame):
    def __init__(self, value, name=None):
        super().__init__(name=name)
        if not _is_supported_constant(value):
            raise TypeError('Unsupported type {}'.format(type(value)))
        self._cached_result = value

    def __str__(self):
        if isinstance(self._cached_result, str):
            return "'{}'".format(self._cached_result)
        elif isinstance(self._cached_result, list):
            values = [str(Constant(x)) for x in self._cached_result]
            return '({})'.format(', '.join(values))
        else:
            return str(self._cached_result)


class Criterion(BaseFrame):
    def __init__(self, operation, pandas_func, source_1, source_2=None,
                 name=None, simple=True):
        # Simple criteria depend on comparisons between columns and/or
        # constants, whereas compound criteria depend on smaller criteria
        if simple:
            assert(isinstance(source_1, ArithmeticMixin) or
                   isinstance(source_1, Constant))
            if source_2 is not None:
                assert(isinstance(source_2, ArithmeticMixin) or
                       isinstance(source_2, Constant))
        else:
            assert(isinstance(source_1, Criterion))
            if source_2 is not None:
                assert(isinstance(source_2, Criterion))

        assert(callable(pandas_func))
        self._pandas_func = pandas_func

        self.operation = operation
        sources = [source_1]
        if source_2 is not None:
            sources.append(source_2)

        super().__init__(name=name, sources=sources)

    def __str__(self):
        '''Default for binary Criterion objects'''
        return '{} {} {}'.format(self._source_to_str(self.sources[0]),
                                 self.operation,
                                 self._source_to_str(self.sources[1]))

    def _pandas(self):
        results = [s.result[s.columns[0]]
                   if isinstance(s, Projection) and len(s.columns) == 1
                   else s.result for s in self.sources]
        return self._pandas_func(*results)

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __invert__(self):
        return Not(self)

    @staticmethod
    def _source_to_str(source):
        if isinstance(source, Projection):
            attrs = ', '.join('{}.{}'.format(source.sources[0].name, col)
                              for col in source.columns)
            if len(source.columns) > 1:
                attrs = '({})'.format(attrs)
            return attrs
        elif isinstance(source, Arithmetic):
            return source._operation_as_str()
        elif isinstance(source, Constant) or isinstance(source, Criterion):
            return str(source)
        else:
            raise TypeError("Unexpected source type {}".format(type(source)))


class ArithmeticMixin(object):
    def __add__(self, other):
        return Add(self, other)

    def __sub__(self, other):
        return Subtract(self, other)

    def __mul__(self, other):
        return Multiply(self, other)

    def __truediv__(self, other):
        return Divide(self, other)

    def __floordiv__(self, other):
        return FloorDivide(self, other)

    def __mod__(self, other):
        return Modulo(self, other)

    def __pow__(self, other):
        return Power(self, other)

    def __and__(self, other):
        return BitAnd(self, other)

    def __or__(self, other):
        return BitOr(self, other)

    def __xor__(self, other):
        return BitXor(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __rsub__(self, other):
        return Subtract(other, self)

    def __rmul__(self, other):
        return Multiply(other, self)

    def __rtruediv__(self, other):
        return Divide(other, self)

    def __rfloordiv__(self, other):
        return FloorDivide(other, self)

    def __rmod__(self, other):
        return Modulo(other, self)

    def __rpow__(self, other):
        return Power(other, self)

    def __rand__(self, other):
        return BitAnd(other, self)

    def __ror__(self, other):
        return BitOr(other, self)

    def __rxor__(self, other):
        return BitXor(other, self)

    def __invert__(self):
        return Invert(self)

    def __neg__(self):
        return Multiply(self, -1)

    def __abs__(self):
        return Abs(self)

    def __eq__(self, other):
        return self.__comparison(other, Equal)

    def __ne__(self, other):
        return self.__comparison(other, NotEqual)

    def __lt__(self, other):
        return self.__comparison(other, LessThan)

    def __le__(self, other):
        return self.__comparison(other, LessThanOrEqual)

    def __gt__(self, other):
        return self.__comparison(other, GreaterThan)

    def __ge__(self, other):
        return self.__comparison(other, GreaterThanOrEqual)

    def __comparison(self, other, how_class):
        # TODO: move the _make call into Criterion.__init__
        return how_class(self, _make_projection_or_constant(other))

    def isin(self, other: List[str]):
        return IsIn(self, Constant(other))

    @property
    def str(self):
        return StringOperator(self)


class StringOperator(object):
    def __init__(self, source):
        self.source = source

    def contains(self, pat: str, regex=False):
        # TODO: support all of Pandas str.contains args
        if regex:
            raise ValueError('Regex support has not been added')

        return Contains(self.source, Constant(pat), regex=regex)

    def startswith(self, pat: str):
        return StartsWith(self.source, Constant(pat))

    def endswith(self, pat: str):
        return EndsWith(self.source, Constant(pat))


class DataFrame(BaseFrame):
    def __init__(self, data=None, name=None, sources=None, pandas_sources=None,
                 deep_copy=False, offload=True, loaded_on_sqlite=False):
        super().__init__(name=name, sources=sources,
                         pandas_sources=pandas_sources)
        df = None

        # If data provided, result is already ready
        if isinstance(data, dict) or isinstance(data, list):
            df = pd.DataFrame(data)
            self._cached_result = df
        elif isinstance(data, pd.DataFrame):
            df = data
            self._cached_result = df.copy(deep=deep_copy)
        elif isinstance(data, DataFrame):
            df = data.compute()
            self._cached_result = df.copy(deep=deep_copy)
        elif data is None:
            pass
        else:
            raise TypeError('Cannot create table from object of type {}'
                            .format(type(data)))

        if df is not None and len(df) > 0:
            if offload:  # Offload dataframe to SQLite
                df.to_sql(name=self.name, con=SQL_CON,
                          index=False, chunksize=SQLITE_CHUNK_SIZE)
                self._cached_on_sqlite = True

            # Store columns
            self.columns = df.columns

        elif loaded_on_sqlite:
            self._cached_on_sqlite = True

        # Compute stats about data, if it exists on Pandas
        if isinstance(self._cached_result, pd.DataFrame):
            self.stats = collect_stats(self._cached_result)

    def __getitem__(self, x):
        if isinstance(x, str) or isinstance(x, list):
            return Projection(self, x)
        elif isinstance(x, Criterion):
            return Selection(self, x)
        elif isinstance(x, slice):
            if x.start is not None or x.step is not None:
                raise ValueError('Only slices of the form df[:n] are accepted')
            return self if x.stop is None else Limit(self, n=x.stop)
        else:
            raise TypeError('Unsupported indexing type {}'.format(type(x)))

    def __setitem__(self, col, value):
        # Make a copy of self which will act as the source of all DataFrames
        # which already depend on self
        old = DataFrame()
        old.__dict__.update(self.__dict__)

        # Make a copy of the _pandas method so old._pandas() works correctly
        old._pandas = lambda: self.__class__._pandas(old)

        # For all dependents, replace self as a source, and add old instead
        for dependent in old.dependents:
            dependent.sources.remove(self)
            dependent.sources.append(old)

        # Ensure value is another column or a constant
        value = _make_projection_or_constant(value, simple=True)

        # Create a new DataFrame (which will become self), and add old
        # as a source, tracking the update that is being done
        # Add value as a Pandas-only source so in the case of Pandas execution,
        # it is computed before executing this write
        new = DataFrame(sources=[old], pandas_sources=[value])
        new.update = Update(old, new, col, value)

        # If column is new, add it to columns
        new.columns = old.columns
        if col not in old.columns:
            new.columns = old.columns.insert(len(old.columns), col)

        # Update self to new object
        self.__dict__.update(new.__dict__)

    def rename(self, columns):
        if not isinstance(columns, dict):
            raise TypeError('Column names must be of type dict, but found {}'
                            .format(type(columns)))

        old_cols = list(columns.keys())
        new_cols = list(columns.values())

        new = DataFrame(sources=[self])
        new.update = UpdateNames(self, new, old_cols, new_names=new_cols)

        # If column is new, add it to columns
        new.columns = self.columns
        for old_col, new_col in zip(old_cols, new_cols):
            col_index = new.columns.get_loc(old_col)
            new.columns = new.columns.drop(old_col)
            new.columns = new.columns.insert(col_index, new_col)

        return new

    def head(self, n=5):
        return self[:n]

    def sort_values(self, by, ascending=True):
        return OrderBy(self, cols=by, ascending=ascending)

    def merge(self, other, on=None, left_on=None, right_on=None):
        """TODO: support other pandas join arguments"""
        return Join(self, other, on, left_on, right_on)

    def groupby(self, by, as_index=False):
        """TODO: support other pandas groupby arguments"""
        return GroupByDataFrame(self, by=by, as_index=as_index)

    def drop_duplicates(self, subset=None, keep='first', inplace=False,
                        ignore_index=False):
        if subset is not None:
            raise ValueError('Subset support has not been added')
        if inplace:
            raise ValueError('In place support has not been added')
        if ignore_index:
            raise ValueError('Index support has not been added')
        if keep != 'first':
            raise ValueError('Keep support has not been added')
        return Projection(self, self.columns.tolist(), drop_duplicates=True)

    @require_result
    def __str__(self):
        return str(self.result)

    @require_result
    def __len__(self):
        return len(self.result)

    @require_result
    def to_csv(self, *args, **kwargs):
        return self.result.to_csv(*args, **kwargs)

    @require_result
    def to_json(self, *args, **kwargs):
        return self.result.to_json(*args, **kwargs)

    @require_result
    def to_numpy(self, *args, **kwargs):
        return self.result.to_numpy(*args, **kwargs)

    @require_result
    def to_pickle(self, *args, **kwargs):
        return self.result.to_pickle(*args, **kwargs)

    @require_result
    def _repr_html_(self, *args, **kwargs):
        '''For being pretty in Jupyter noteboks'''
        return self.result._repr_html_(*args, **kwargs)

    @require_result
    def _repr_latex_(self, *args, **kwargs):
        return self.result._repr_latex_(*args, **kwargs)

    @require_result
    def _repr_data_resource_(self, *args, **kwargs):
        return self.result._repr_data_resource_(*args, **kwargs)

    @require_result
    def _repr_fits_horizontal_(self, *args, **kwargs):
        return self.result._repr_fits_horizontal_(*args, **kwargs)

    @require_result
    def _repr_fits_vertical_(self, *args, **kwargs):
        return self.result._repr_fits_vertical_(*args, **kwargs)

    def __hash__(self):
        return super().__hash__()


class Update(object):
    def __init__(self, source: DataFrame, dest: DataFrame, col: str, value):
        # TODO: check if projection is from the same df?

        self.source = source
        self.dest = dest
        self.col = col
        self.value = value

        columns = self.source.columns
        col_index = columns.get_loc(col) if self.col in columns \
            else len(columns)
        columns = columns.drop(self.col) if self.col in columns else columns

        val = self.value
        if isinstance(val, Projection):
            val = val.columns[0]
        elif isinstance(val, Arithmetic):
            val = val._operation_as_str()

        new_column = '{} AS {}'.format(val, self.col)
        columns = columns.insert(col_index, new_column)

        self._sql_query = 'SELECT {} FROM {}'.format(', '.join(columns),
                                                     self.source.name)

    def __str__(self):
        val = self.value
        if isinstance(val, Projection):
            val = val.columns[0]
        return 'Update({} to {}, {} <- {})'.format(self.source.name,
                                                   self.dest.name, self.col,
                                                   val)


class UpdateNames(object):
    def __init__(self, source: DataFrame, dest: DataFrame, cols: str,
                 new_names: list):
        self.source = source
        self.dest = dest
        self.cols = cols

        columns = self.source.columns

        if len(cols) != len(new_names):
            raise ValueError('Length of columns and new names do not match')

        for col, new_name in zip(cols, new_names):
            col_index = columns.get_loc(
                col) if col in columns else len(columns)
            columns = columns.drop(col) if col in columns else columns

            new_column = '{} AS {}'.format(col, new_name)
            columns = columns.insert(col_index, new_column)

        self._sql_query = 'SELECT {} FROM {}'.format(', '.join(columns),
                                                     self.source.name)


class Projection(DataFrame, ArithmeticMixin):
    def __init__(self, source: DataFrame, col, name=None,
                 drop_duplicates=False):

        if isinstance(col, str):
            cols = [col]
        elif isinstance(col, list):
            cols = col
        else:
            raise TypeError('col must be of type str or list, but found {}'
                            .format(type(col)))

        super().__init__(name=name, sources=[source])

        if len(pd.Index(cols).difference(source.columns)) > 0:
            raise ValueError("Projection columns {} are not a subset of {}"
                             .format(cols, source.columns))

        self.dedup = drop_duplicates
        self.columns = source.columns[source.columns.isin(cols)]
        dist = "DISTINCT " if self.dedup else ""
        self._sql_query = 'SELECT {}{} FROM {}'.format(dist,
                                                       ', '.join(self.columns),
                                                       self.sources[0].name)

    def _pandas(self):
        if self.dedup:
            return self.sources[0].result[self.columns].drop_duplicates()
        return self.sources[0].result[self.columns]

    def __hash__(self):
        return super().__hash__()


class Selection(DataFrame):
    def __init__(self, source: DataFrame, criterion: Criterion, name=None):
        super().__init__(name=name, sources=[source],
                         pandas_sources=[criterion])
        self.columns = source.columns

        self._sql_query = 'SELECT * FROM {} WHERE {}'.format(
            self.sources[0].name, self.pandas_sources[0])

    def _pandas(self):
        return self.sources[0].result[self.pandas_sources[0].result]


class OrderBy(DataFrame):
    def __init__(self, source: DataFrame, cols, ascending=True, name=None):

        super().__init__(name=name, sources=[source])
        if isinstance(cols, str):
            cols = [cols]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(cols)
        if len(cols) != len(ascending):
            raise ValueError("cols and ascending must be equal lengths, "
                             "but found len(cols)={} and len(ascending)={}"
                             .format(len(cols), len(ascending)))

        self.order_cols = cols
        self.ascending = ascending
        self.columns = source.columns

        order_by = [
            '{}.{} {}'.format(self.sources[0].name, col,
                              'ASC' if asc else 'DESC')
            for col, asc in zip(self.order_cols, self.ascending)
        ]

        self._sql_query = 'SELECT * FROM {} ORDER BY {}' \
            .format(self.sources[0].name, ', '.join(order_by))

    def _pandas(self):
        return self.sources[0].result.sort_values(self.order_cols,
                                                  ascending=self.ascending)


class Join(DataFrame):
    def __init__(self, source_1: DataFrame, source_2: DataFrame,
                 join_keys=None, left_keys=None, right_keys=None, name=None):
        super().__init__(name=name, sources=[source_1, source_2])

        if join_keys is None:
            if left_keys is None or right_keys is None:
                raise ValueError('No join keys provided!')
        else:
            left_keys = right_keys = join_keys
        if isinstance(left_keys, str):
            left_keys = [left_keys]
        if isinstance(right_keys, str):
            right_keys = [right_keys]

        if len(left_keys) != len(right_keys):
            raise ValueError('Got conflicting number of keys for merge: '
                             f'{len(left_keys)} and {len(right_keys)}')

        self.left_keys = left_keys
        self.right_keys = right_keys

        for source, keys in zip(self.sources, [left_keys, right_keys]):
            join_index = pd.Index(keys)
            if len(join_index.difference(source.columns)) > 0:
                raise ValueError("Source {} does not contain all join keys {}"
                                 .format(source.name, join_keys))

        self.columns = source_1.columns.union(source_2.columns)

        join_cols = [f'({source_1.name}.{l} = {source_2.name}.{r})'
                     for l, r in zip(left_keys, right_keys)]
        output_cols = [f'{source_1.name}.{c} AS {c}' if c in source_1.columns
                       else f'{source_2.name}.{c} AS {c}'
                       for c in self.columns]
        self._sql_query = 'SELECT {} FROM {} JOIN {} ON {}' \
            .format(', '.join(output_cols), source_1.name, source_2.name,
                    ' AND '.join(join_cols))

    def _pandas(self):
        return pd.merge(self.sources[0].result, self.sources[1].result,
                        left_on=self.left_keys, right_on=self.right_keys)


class Union(DataFrame):
    def __init__(self, sources: List[DataFrame], name=None):
        super().__init__(name=name, sources=sources)

        self.columns = sources[0].columns
        for source in sources:
            if len(source.columns.symmetric_difference(self.columns)) > 0:
                raise ValueError("Cannot union sources with different schemas")

        self._sql_query = ' UNION ALL '.join('SELECT * FROM {}'
                                             .format(source.name)
                                             for source in self.sources)

    def _pandas(self):
        return pd.concat([s.result for s in self.sources])


class Limit(DataFrame):
    def __init__(self, source: DataFrame, n: int, name=None):

        super().__init__(name=name, sources=[source])
        self.n = n
        self.columns = source.columns

        self._sql_query = 'SELECT * FROM {} LIMIT {}'.format(
            self.sources[0].name, self.n)

    def _pandas(self):
        return self.sources[0].result[:self.n]


##############################################################################
#                           GroupBy Operations
##############################################################################


class GroupByDataFrame(BaseFrame):
    def __init__(self, source: DataFrame, by, as_index=False, name=None):

        if isinstance(by, str):
            self.groupby_cols = [by]
        elif isinstance(by, list):
            self.groupby_cols = by
        else:
            raise TypeError('by must be of type str or list, but found {}'
                            .format(type(by)))

        super().__init__(name=name, sources=[source])
        self.base_name = source.name
        self.columns = source.columns.union(pd.Index(self.groupby_cols))
        self.as_index = as_index

    def __getitem__(self, x):
        if isinstance(x, str) or isinstance(x, list):
            return GroupByProjection(self, x)
        elif isinstance(x, slice):
            raise TypeError('Slicing not support for GroupBy objects')
        else:
            raise TypeError('Unsupported indexing type {}'.format(type(x)))

    def __str__(self):
        return 'GroupBy({}, by={})'.format(self.sources[0].name,
                                           self.groupby_cols)

    def _pandas(self):
        grouped = self.sources[0].result.groupby(self.groupby_cols,
                                                 as_index=self.as_index)
        # When as_index=False, Pandas does not like it if you select a
        # column that is being grouped by
        return grouped[self.columns[~self.columns.isin(self.groupby_cols)]]


class GroupByProjection(GroupByDataFrame):
    def __init__(self, source: GroupByDataFrame, col, name=None):

        if isinstance(col, str):
            cols = [col]
        elif isinstance(col, list):
            cols = col
        else:
            raise TypeError('col must be of type str or list, but found {}'
                            .format(type(col)))

        super().__init__(source.sources[0], by=source.groupby_cols,
                         as_index=source.as_index, name=name)

        if len(pd.Index(cols).difference(source.columns)) > 0:
            raise ValueError("Projection columns {} are not a subset of {}"
                             .format(cols, source.columns))
        self.columns = self.columns[self.columns.isin(cols)
                                    | self.columns.isin(self.groupby_cols)]

    def __str__(self):
        return 'GroupByProjection({}, by={}, cols={})' \
            .format(self.sources[0].name, self.groupby_cols,
                    self.columns.to_list())


##############################################################################
#                                Aggregators
##############################################################################


class Aggregator(DataFrame):
    AGGREGATORS = {
        'sum': 'SUM',
        'count': 'COUNT',
        'mean': 'AVG',
        'min': 'MIN',
        'max': 'MAX',
        'prod': 'PROD',
        'any': 'AGG_ANY',
        'all': 'AGG_ALL'
    }

    def __init__(self, agg, source: BaseFrame, name=None):
        super().__init__(sources=[source], name=name)

        self.agg = agg
        self.agg_sql = self.AGGREGATORS[agg]
        self.grouped = isinstance(source, GroupByDataFrame)

        # TODO: only use valid columns for aggregation operation
        cols = ['{}({}) AS {}'.format(self.agg_sql, c, c)
                for c in source.columns]

        if not self.grouped:
            self._sql_query = 'SELECT {} FROM {}'.format(', '.join(cols),
                                                         source.name)
        else:
            cols = list(source.groupby_cols)
            cols += ['{}({}) AS {}'.format(self.agg_sql, c, c)
                     for c in source.columns
                     if c not in source.groupby_cols]

            self._sql_query = 'SELECT {} FROM {} GROUP BY {}' \
                .format(', '.join(cols), source.base_name,
                        ', '.join(source.groupby_cols))

        self.columns = source.columns

        # Set return types in cases where explicit conversion is needed,
        # e.g., bool because SQLite doesn't have bools
        if self.agg_sql in ['AGG_ANY', 'AGG_ALL']:
            self.final_type = bool
        else:
            self.final_type = None

    def _pandas(self):
        if self.grouped:
            result = getattr(self.sources[0].result, self.agg)()
            if len(result.columns) == 1:
                result = result[result.columns[0]]

        else:
            source = self.sources[0].result
            if len(self.columns) == 1:
                source = source[source.columns[0]]
            result = getattr(source, self.agg)()

        return result

    def process_result(self, result):
        '''This function will be called by BaseFrame.compute'''
        if len(result) == 1:
            series = result.iloc[0]
            if len(series) == 1:    # Single numerical value
                ret = series[0]
            else:                   # Multiple numerical values
                series.name = None
                ret = series
        else:
            ret = result

        # Type cast if necessary
        if self.final_type is not None:
            if isinstance(ret, pd.DataFrame) or isinstance(ret, pd.Series):
                ret = ret.astype(self.final_type)
            else:
                ret = self.final_type(ret)

        return ret


##############################################################################
#                       Catch-all for Pandas API Functions
##############################################################################


class FallbackOperation(DataFrame):
    def __init__(self, *args, source: DataFrame, op: str, name=None, **kwargs):
        super().__init__(name=name, sources=[source])
        self.op = op
        self.args = args
        self.kwargs = kwargs
        self.columns = source.columns

    def _pandas(self):
        source_result = self.sources[0].result
        func = getattr(source_result, self.op)
        return func(*self.args, **self.kwargs)


##############################################################################
#                           Misc. API Functions
##############################################################################


def merge(left: DataFrame, right: DataFrame, on=None,
          left_on=None, right_on=None):
    """TODO: support other pandas join arguments"""
    return Join(left, right, on, left_on, right_on)


def concat(objs: List[DataFrame]):
    return Union(objs)


##############################################################################
#                           Criterion Classes
##############################################################################


class Equal(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('=', lambda x, y: x == y,
                         source_1, source_2, name=name)


class NotEqual(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('<>', lambda x, y: x != y,
                         source_1, source_2, name=name)


class LessThan(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('<', lambda x, y: x < y,
                         source_1, source_2, name=name)


class LessThanOrEqual(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('<=', lambda x, y: x <= y,
                         source_1, source_2, name=name)


class GreaterThan(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('>', lambda x, y: x > y,
                         source_1, source_2, name=name)


class GreaterThanOrEqual(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('>=', lambda x, y: x >= y,
                         source_1, source_2, name=name)


class And(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('AND', lambda x, y: x & y,
                         source_1, source_2, name=name, simple=False)


class Or(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('OR', lambda x, y: x | y,
                         source_1, source_2, name=name, simple=False)


class Not(Criterion):
    def __init__(self, source, name=None):
        super().__init__('NOT', lambda x: ~x,
                         source, name=name, simple=False)

    def __str__(self):
        return 'NOT ({})'.format(self.sources[0])


class IsIn(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('IN', lambda x, y: x.isin(y),
                         source_1, source_2, name=name)


class Contains(Criterion):
    def __init__(self, source_1, source_2, regex=False, name=None):
        super().__init__('LIKE', lambda x, y: x.str.contains(y, regex=regex),
                         source_1, source_2, name=name)

    def __str__(self):
        return "{} LIKE '%{}%'".format(self._source_to_str(self.sources[0]),
                                       self.sources[1].result)


class StartsWith(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('LIKE', lambda x, y: x.str.startswith(y),
                         source_1, source_2, name=name)

    def __str__(self):
        return "{} LIKE '{}%'".format(self._source_to_str(self.sources[0]),
                                      self.sources[1].result)


class EndsWith(Criterion):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('LIKE', lambda x, y: x.str.endswith(y),
                         source_1, source_2, name=name)

    def __str__(self):
        return "{} LIKE '%{}'".format(self._source_to_str(self.sources[0]),
                                      self.sources[1].result)


##############################################################################
#                           Arithmetic Classes
##############################################################################


class Arithmetic(DataFrame, ArithmeticMixin):
    def __init__(self, operation, pandas_func, operand_1, operand_2=None,
                 inline=False, name=None):

        sources = []

        self.operands = [_make_projection_or_constant(operand_1, simple=True)]
        if isinstance(self.operands[0], DataFrame):
            sources += self.operands[0].sources

        if operand_2 is not None:
            self.operands.append(_make_projection_or_constant(operand_2,
                                                              simple=True))
            if isinstance(self.operands[1], DataFrame):
                sources += self.operands[1].sources

        super().__init__(name=name, sources=sources)

        assert(callable(pandas_func))
        self._pandas_func = pandas_func

        self.operation = operation
        self.inline = inline

        self._sql_query = 'SELECT {} AS res FROM {}'.format(
            self._operation_as_str(), self.sources[0].name)

    def _pandas(self):
        # Operands might not be already computed since they are not technically
        # "sources" (dependencies). So, explicitly compute them.
        for operand in self.operands:
            operand.compute()

        results = [op.result[op.columns[0]]
                   if isinstance(op, Projection) and len(op.columns) == 1
                   else op.result for op in self.operands]

        return self._pandas_func(*results)

    def _operation_as_str(self):
        if len(self.operands) == 1:
            fmt = '{op}{x}' if self.inline else'{op}({x})'

            return fmt.format(x=self._operand_to_str(self.operands[0]),
                              op=self.operation)
        else:
            fmt = '{x} {op} {y}' if self.inline else'{op}({x}, {y})'

            return fmt.format(x=self._operand_to_str(self.operands[0]),
                              y=self._operand_to_str(self.operands[1]),
                              op=self.operation)

    @staticmethod
    def _operand_to_str(source):
        if isinstance(source, Projection):
            return '{}.{}'.format(source.sources[0].name, source.columns[0])
        elif isinstance(source, Constant):
            return str(source)
        elif isinstance(source, Arithmetic):
            if source.inline:
                return '({})'.format(source._operation_as_str())
            else:
                return source._operation_as_str()
        else:
            raise TypeError("Unexpected source type {}".format(type(source)))


class Add(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('+', lambda x, y: x + y, source_1, source_2,
                         name=name, inline=True)


class Subtract(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('-', lambda x, y: x - y, source_1, source_2,
                         name=name, inline=True)


class Multiply(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('*', lambda x, y: x * y, source_1, source_2,
                         name=name, inline=True)


class Divide(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('DIV', lambda x, y: x / y, source_1, source_2,
                         name=name)


class FloorDivide(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('FLOORDIV', lambda x, y: x // y, source_1, source_2,
                         name=name)


class Modulo(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('MOD', lambda x, y: x % y, source_1, source_2,
                         name=name)


class Power(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('POW', lambda x, y: x ** y, source_1, source_2,
                         name=name)


class BitAnd(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('BITAND', lambda x, y: x & y, source_1, source_2,
                         name=name)


class BitOr(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('BITOR', lambda x, y: x | y, source_1, source_2,
                         name=name)


class BitXor(Arithmetic):
    def __init__(self, source_1, source_2, name=None):
        super().__init__('BITXOR', lambda x, y: x ^ y, source_1, source_2,
                         name=name)


class Invert(Arithmetic):
    def __init__(self, source, name=None):
        super().__init__('INV', lambda x: ~x, source, name=name)


class Abs(Arithmetic):
    def __init__(self, source, name=None):
        super().__init__('abs', abs, source, name=name)


##############################################################################
#                       Offloading Decision Functions
##############################################################################


def offloading_strategy(name=None):
    if name is not None:
        name = name.upper()
        if name not in ['ALWAYS', 'NEVER', 'BEST']:
            raise ValueError(f'Unsupported offloading strategy: {name}')

        global OFFLOADING_STRATEGY
        OFFLOADING_STRATEGY = name

    else:
        return OFFLOADING_STRATEGY


def should_offload_computation(df: BaseFrame):
    if OFFLOADING_STRATEGY == 'ALWAYS':
        return True
    elif OFFLOADING_STRATEGY == 'NEVER':
        return False
    elif OFFLOADING_STRATEGY == 'BEST':
        return COST_MODEL.should_offload(df)
        # TODO: put smart offloading logic here
    else:
        raise NotImplementedError('Unsupported offloading strategy: {}'
                                  .format(OFFLOADING_STRATEGY))


def _ensure_computable(df, on):
    if on == 'pandas':
        def is_cached(x): return x._cached_result is not None
    elif on == 'sqlite':
        def is_cached(x): return x._cached_on_sqlite
    else:
        raise ValueError(f'Unknown computation mechanism: {on}')

    graph = _get_dependency_graph(df, on=on)
    ordered_deps = _topological_sort(graph)

    computable = {}
    faults = 0
    for dep in ordered_deps:
        # A DataFrame is computable if it is already cached,
        # or all of its dependencies are computable
        if is_cached(dep):
            computable[dep] = True
        elif len(dep.sources) > 0 and \
                all(computable[source] for source in dep.sources):
            computable[dep] = True
        else:
            computable[dep] = False
            faults += 1

    if faults > 0:
        raise RuntimeError(f'The given computation cannot be run on {on} '
                           f'because {faults} dependencies cannot be computed')

    # If there are any pending operations that are Pandas-only
    # (i.e., supported via fallbacks), then we cannot run on SQLite.
    if on == 'sqlite':
        fallbacks = _filter_ancestors(df, graph, lambda x: not is_cached(x) and
                                      isinstance(x, FallbackOperation),
                                      max_depth=None)
        if len(fallbacks) > 0:
            raise RuntimeError(f'The given computation cannot be run on {on} '
                               f'because {len(fallbacks)} pending operations '
                               'are only supported via Pandas.')


##############################################################################
#                           Utility Functions
##############################################################################


# TODO: add more supported types
SUPPORTED_TYPES = [int, float, str, list]

# TODO: switch to uuids when done testing
COUNT = 0


def _is_supported_constant(x):
    return any(isinstance(x, t) for t in SUPPORTED_TYPES)


def _new_name():
    # name = uuid.uuid4().hex
    global COUNT
    name = 'T' + str(COUNT)
    COUNT += 1
    return name


def _define_dependencies(df: DataFrame):
    graph = _get_dependency_graph(df, on='sqlite')
    ordered_deps = _topological_sort(graph)

    # Do NOT define a dependency t if any of the following is true:
    #   (1) t is the current df
    #   (2) t is already cached on SQLite
    #   (3) t is a GroupByDataFrame object
    #   (4) t is a Criterion object (won't be in graph)
    common_table_exprs = [
        '{} AS ({})'.format(t.name, t.sql(dependencies=False))
        for t in ordered_deps
        if t is not df and not t._cached_on_sqlite and isinstance(t, DataFrame)
    ]

    if len(common_table_exprs) > 0:
        return 'WITH ' + ', '.join(common_table_exprs)
    else:
        return None


def _make_projection_or_constant(x, simple=False, arithmetic=True):
    if isinstance(x, Projection):
        if simple and len(x.columns) != 1:
            raise ValueError("Projections must have exactly 1 column"
                             "but found columns {}".format(x.columns))
        return x
    elif _is_supported_constant(x):
        return Constant(x)
    elif arithmetic and isinstance(x, Arithmetic):
        return x
    else:
        raise TypeError('Only constants and Projections are accepted')


def collect_stats(df: pd.DataFrame):
    # TODO: also collect stats for str and datetime columns
    return df.describe([q / 100 for q in range(5, 100, 5)])


##############################################################################
#                           Public SQLite Functions
##############################################################################


def get_database_file():
    return DB_FILE


def set_database_file(file_name, delete=False):
    # Close previous connection
    stop(delete=delete)

    # Start new connection with new file
    global DB_FILE
    global SQL_CON
    DB_FILE = file_name
    SQL_CON = get_sqlite_connection(DB_FILE)


def stop(delete=False):
    SQL_CON.close()
    if delete:
        os.remove(DB_FILE)
