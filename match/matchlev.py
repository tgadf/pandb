""" Simple wrapper for Levenshtein ratio calculation """

__all__ = ["getLevenshtein"]

from numpy import vectorize
import Levenshtein
def getLevenshtein(x1, x2):
    try:
        return Levenshtein.ratio(x1, x2)
    except:
        print("ERROR With Levenshtein.ratio({0}, {1})".format(x1,x2))
        return 0.0
    
vLev = vectorize(getLevenshtein)