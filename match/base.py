""" Match Base Class """

__all__ = ["MatchDBBase"]

from .params import MatchDBParams
from .utils import printIntro, write
from .pool import poolMatchAlbums
from .results import MatchQualityNames
from pandas import DataFrame, Series, concat
from typing import Union

class MatchDBBase:
    def __init__(self, reqs, **kwargs):
        self.verbose  = kwargs.get('verbose', True)
        self.params   = MatchDBParams(reqs, verbose=self.verbose)
        self.write    = write
        self.__name__ = type(self).__name__            
        if self.verbose: printIntro(self.__name__)
        
        self.validDB  = self.params.validDB
        self.getDBReq = self.params.getDBReq
        self.getMatchNameReq = self.params.getMatchNameReq
        self.getPart = self.params.getPart
        self.mqnames = MatchQualityNames()
        

    ################################################################################################################################################
    ## Select Artists For Media Match
    ################################################################################################################################################
    def selectArtistsForMediaMatch(self, artistMatchResults: Union[DataFrame,Series]) -> 'Series':
        artistNameCutoff = self.getMatchNameReq()
        if isinstance(artistMatchResults,DataFrame):
            nearArtistNameMatches = artistMatchResults.apply(lambda values: values[values >= artistNameCutoff].to_dict(), axis=1)
        elif isinstance(artistMatchResults,Series):
            nearArtistNameMatches = artistMatchResults.apply(lambda values: values[values >= artistNameCutoff].to_dict())
        if self.verbose: self.write(4, "Found {0} Name Results", nearArtistNameMatches.shape[0])
        artistNameMatches = nearArtistNameMatches[nearArtistNameMatches.apply(len) > 0]
        if self.verbose: self.write(4, "Found {0} Artists With One Or More Matches", artistNameMatches.shape[0])
        if self.verbose: self.write(4, "Found {0} Possible Matches", artistNameMatches.apply(len).sum())
        return artistNameMatches
        
        
    ################################################################################################################################################
    ## Match Media Data Using Pool (instead of Dask)
    ################################################################################################################################################
    def matchMediaDataPool(self, mediaData: Series) -> 'Series':
        albumMatchResults = poolMatchAlbums(mediaData, verbose=True)
        
        mediaResults = {}
        #rankValues   = {"Loose": 0.7, "Medium": 0.8, "Tight": 0.9, "Exact": 0.95}
        for baseID,compareResults in albumMatchResults.groupby(level=0):
            mediaResults[baseID] = {}
            for (_,compareID),compareIDResult in compareResults.iteritems():
                df  = compareIDResult.apply(Series)
                key = (baseID,compareID)
                bestBaseMatch    = Series(df.max(axis=0).values, index=mediaData[key]["Compare"])
                bestCompareMatch = Series(df.max(axis=1).values, index=mediaData[key]["Base"])

                baseRankResult = {rank: bestBaseMatch[bestBaseMatch >= value].count() for rank,value in self.mqnames.mediaMatchValues.items()}
                compareRankResult = {rank: bestCompareMatch[bestCompareMatch >= value].count() for rank,value in self.mqnames.mediaMatchValues.items()}

                rankData = concat([Series(baseRankResult, name="Base"), Series(compareRankResult, name="Compare")], axis=1)

                mediaResults[baseID][compareID] = {"Rank": rankData,  "Raw": {"BestBaseMatch": bestBaseMatch, "BestCompareMatch": bestCompareMatch}}

        retval = Series(mediaResults)
        return retval