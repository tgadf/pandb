""" Simple wrapper for Levenshtein ratio calculation """

__all__ = ["getLevenshtein", "getLevPair", "getLev"]

from pandas import Series
from numpy import vectorize
import Levenshtein

def getLevPair(pair):
    if isinstance(pair, (Series,list,tuple)):
        assert len(pair) == 2, f"Can't compute Lev value since {pair} is not a pair of entries"
        return getLevenshtein(pair[0],pair[1])
    else:
        raise TypeError(f"Can't compute Lev value since {pair} is type {type(pair)}")
        
def getLev(x1, x2, verbose=False):
    return getLevenshtein(x1, x2, verbose=False)
        
def getLevenshtein(x1, x2, verbose=False):
    try:
        return Levenshtein.ratio(x1, x2)
    except:
        if verbose: print("ERROR With Levenshtein.ratio({0}, {1})".format(x1,x2))
        return -1.0
    
vLev = vectorize(getLevenshtein)