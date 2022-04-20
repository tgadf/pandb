""" Match RateYourMusic List Names and Refs """

__all__ = ["MatchListDataNames", "MatchListDataRefs"]

import Levenshtein
from pandas import Series
from timeutils import Timestat
from .mdbmatch import MusicDBMatch

class MatchListDataNames:
    def __init__(self, listDataToGet, artistsToMatch, cutoff=0.9):
        self.listDataToGet  = listDataToGet
        self.artistsToMatch = artistsToMatch.unique()
        self.mdbm           = MusicDBMatch(base=self.listDataToGet["Name"])
        self.cutoff         = cutoff
                
    def getNearMatches(self, x):
        artistNameUpper = x.upper()
        searchResults = remainingArtists.apply(mm.getLevenshtein, x2=artistNameUpper)
        return len(searchResults[searchResults > cutoff])        
        
    def match(self):
        retval = {}
        ts = Timestat("Matching Master {0} Artists Against {1} List Artists".format(len(self.artistsToMatch), self.listDataToGet.shape[0]))
        if len(self.artistsToMatch) > 5000:
            modVal = 2500
        elif len(self.artistsToMatch) > 1000:
            modVal = 500
        else:
            modVal = 250
        for n,artistName in enumerate(self.artistsToMatch):
            if (n+1) % modVal == 0 or (n+1) == modVal/2:
                ts.update(n=n+1,N=self.artistsToMatch.shape[0])
            results = self.mdbm.match(value=artistName)
            idx = results[results >= self.cutoff]
            if len(idx) > 0:
                retval[artistName] = idx.index
        ts.stop()
        return retval
        

class MatchListDataRefs:
    def __init__(self, listDataToGet, artistsToMatch, cutoff=0.9, maxCutoff=1.0):
        self.listDataToGet  = listDataToGet
        self.artistsToMatch = artistsToMatch.unique()
        self.mdbm           = MusicDBMatch(base=self.listDataToGet["Ref"])
        self.cutoff         = cutoff
        self.maxCutoff      = maxCutoff
                
    def getNearMatches(self, x):
        artistNameUpper = x.upper()
        searchResults = remainingArtists.apply(mm.getLevenshtein, x2=artistRefUpper)
        return len(searchResults[(searchResults > cutoff) & (searchResults <= maxCutoff)])
        
    def match(self):
        retval = {}
        ts = Timestat("Matching Master {0} Artists Against {1} List Artists".format(len(self.artistsToMatch), self.listDataToGet.shape[0]))
        if len(self.artistsToMatch) > 5000:
            modVal = 2500
        elif len(self.artistsToMatch) > 1000:
            modVal = 500
        else:
            modVal = 250
        for n,artistRef in enumerate(self.artistsToMatch):
            if (n+1) % modVal == 0 or (n+1) == modVal/2:
                ts.update(n=n+1,N=self.artistsToMatch.shape[0])
            results = self.mdbm.match(value=artistRef)
            idx = results[(results >= self.cutoff) & (results < self.maxCutoff)]
            if len(idx) > 0:
                retval[artistRef] = idx.index
        ts.stop()
        return retval