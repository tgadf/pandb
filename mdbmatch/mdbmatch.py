""" Music DB Match """

__all__ = ["MusicDBMatch"]

import Levenshtein
from pandas import Series
#from strsimpy.cosine import Cosine
#from thefuzz import fuzz

class MusicDBMatch:
    def __init__(self, base, **kwargs):
        self.debug = kwargs.get('debug', kwargs.get('verbose', False))
        self.mtype = "Levenshtein"
        self.base  = base
        if not isinstance(base, Series):
            raise ValueError("Must pass base Series")
        
        self.prepare = kwargs.get('prepare', True)
        try:
            self.base    = self.base.apply(str.upper) if self.prepare is True else self.base
        except:
            raise ValueError("Could not prepare base Series")
            
        
    ##################################################################################################################
    # Matching Algorithms
    ##################################################################################################################
    def getLevenshtein(self, x1, x2):
        try:
            return Levenshtein.ratio(x1, x2)
        except:
            print("ERROR With Levenshtein.ratio({0}, {1})".format(x1,x2))
            return 0.0
        
    def getCosine(self, x1,x2):
        try:
            p1 = self.cosine.get_profile(x1)
            p2 = self.cosine.get_profile(x2)
            return self.cosine.similarity_profiles(p1, p2)
        except:
            print("ERROR With cosine.similarity_profiles({0}, {1})  [{2} , {3}]".format(p1,p2,x1,x2))
            return 0.0
        
        
    ##################################################################################################################
    # Matching I/O
    ##################################################################################################################
    def matchNULL(self):
        return Series([0.0]*len(self.base), index=self.base.index)
    
    def match(self, value):
        if not isinstance(value, str):
            print("Input Value is NONE ==> Returning 0.0")
            return self.matchNULL()
        
        value = value.upper() if self.prepare else value
        try:
            retval = self.base.apply(self.getLevenshtein, x2=value)
        except:
            print("Could not apply getLevenshtein to all elements")
            return self.matchNULL()
        
        return retval