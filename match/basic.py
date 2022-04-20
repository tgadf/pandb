""" Basic Matching Classes """

__all__ = ["MatchString", "MatchSeries"]

from pandas import DataFrame, Series, concat
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from .matchlev import getLevenshtein

class MatchString:
    def __init__(self, base, compare, nPart=3):
        assert isinstance(base, str), "Base is not a Series"
        assert isinstance(compare, Series), "Compare is not a Series"
    
        self.match = compare.apply(getLevenshtein, x2=base)
        self.start = compare.str.startswith(base)
        
    def get(self, cutoff=None):
        retval = self.match if cutoff is None else self.match[(self.match >= cutoff) | (self.start)].to_dict()
        return retval

class MatchSeries:
    def __init__(self, base, compare, nPart=3):
        assert isinstance(base, Series), "Base is not a Series"
        assert isinstance(compare, Series), "Compare is not a Series"
        
        pbar = ProgressBar()
        pbar.register()
        
        daskDF = dd.from_pandas(base, npartitions=nPart)
        self.match  = daskDF.map_partitions(lambda df: df.apply(lambda artistName: compare.apply(getLevenshtein, x2=artistName))).compute(scheduler='processes')
        
    def get(self, cutoff=None):
        retval = self.match if cutoff is None else self.match.apply(lambda values: values[values >= cutoff].to_dict(), axis=1)
        return retval