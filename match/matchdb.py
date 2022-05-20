""" Main Matching Class """

__all__ = ["MatchDB"]

from timeutils import Timestat
from listUtils import getFlatList
from .matchlev import getLevenshtein
from .dataio import MatchDBDataIO
from .req import MatchReq
from .results import PrimaryMatchResults
from .utils import write
from .pool import poolMatchNames, poolMatchAlbums
from pandas import DataFrame, Series, concat
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from typing import Union

class MatchDB:
    def __init__(self, baseDB: str, compareDBs: list, reqs: dict, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        self.write = write
        if self.verbose:
            print("*"*175)
            print("*{0}  {1}  {2} *".format(" "*69, "MatchDB()", " "*(175-71-5-len("MatchDB()"))))
            print("*"*175)

        self.useDask = kwargs.get('dask', False)
        self.usePool = kwargs.get('pool', True)
        assert self.useDask | self.usePool, "dask or pool must be set"
            
        mask = reqs.get("Mask")
        assert isinstance(mask,(bool,str)), "Mask Req is not set."
        
        mediaTypes = reqs.get("Media")
        assert isinstance(mediaTypes,list) or mediaTypes is None, "Media Req is not set."
        self.mediaTypes = mediaTypes
        
        mreqs = reqs.get("Reqs")
        assert isinstance(mreqs,dict), "Match Reqs is not set."
        self.mreqs = mreqs
        
        self.nPart = reqs.get("NPart", 2)
        assert isinstance(self.nPart,int), "NPart Req is not set."
        
        self.matchReqs = reqs.get("Match")
        assert isinstance(self.matchReqs,dict), "Match Req is not set."
        assert isinstance(self.matchReqs.get('Artist'),float), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Medium'),int), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Tight'),int), "Artist match req is not set"
        
        self.baseIO = MatchDBDataIO(db=baseDB, mediaTypes=mediaTypes, mask=mask, verbose=False, base=True)
        assert isinstance(self.mreqs.get(self.baseIO.db),MatchReq), "Match Reqs does not have BaseDB [{0}]".format(self.baseIO.db)
        
        self.compareIOs = {}
        for compareDB in compareDBs:
            compareIO = MatchDBDataIO(db=compareDB, mediaTypes=mediaTypes, mask=mask, verbose=False, base=False)
            assert isinstance(self.mreqs.get(compareIO.db),MatchReq), "Match Reqs does not have CompareDB [{0}]".format(compareIO.db)
            self.compareIOs[compareDB] = compareIO
        
        self.diagnostics = {}
        self.mres = MatchResults(self.baseIO)
        self.save = self.mres.save
        self.results = {}
        
        pbar = ProgressBar()
        pbar.register()
        

    def match(self, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)                
        tsMatch = Timestat("Matching [{0}] Against {1}".format(self.baseIO.db, list(self.compareIOs.keys())), ind=0)
        
        print("")
        print("-"*150)
        print("{0} {1} {2}".format("-"*70, self.baseIO.db, "-"*(150-70-2-len(self.baseIO.db))))
        baseIO = self.baseIO
        if verbose: ts = Timestat("Loading Artist Names", ind=2)
        baseIO.loadNames()
        baseIO.setAvailableNames(self.mreqs[baseIO.db])
        if verbose: ts.stop()
        self.results[baseIO.db] = {}
        
        results = {}
        for compareDB in self.compareIOs.keys():
            compareIO = self.compareIOs[compareDB]
            
            print("")
            print("-"*150)
            print("{0} {1} {2}".format("-"*70, compareDB, "-"*(150-70-2-len(compareDB))))
            
            if verbose: ts = Timestat("Loading Artist Names", ind=2)
            compareIO.loadNames()
            compareIO.setAvailableNames(self.mreqs[compareIO.db])
            if verbose: ts.stop()
        
            ########################################################################################################################################
            ## 1) Match Artist Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] x {2} [{3}] Artist Names".format(baseIO.getNumNames(), baseIO.db, compareIO.getNumNames(), compareIO.db), ind=2)
            nCores = self.nPart if compareIO.getNumNames() < 200000 else 3
            artistMatchResults = poolMatchNames(baseNames=baseIO.getAvailableNames(), compNames=compareIO.getAvailableNames(), nCores=nCores, progress=True, cutoff=self.matchReqs['Artist'])
            if verbose: ts.stop()
                
            artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults, self.matchReqs['Artist'])
            if artistNameMatches.shape[0] == 0:
                del artistMatchResults
                self.compareIOs[compareDB] = None
                continue                
            mediaData          = self.prepareMediaData(artistNameMatches, baseIO, compareIO)
            del artistMatchResults
            del artistNameMatches
            
            
            ########################################################################################################################################
            ## 2) Match Artist Albums Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] Album Names".format(mediaData.shape[0], baseIO.db), ind=2)
            albumMatchResults = self.matchMediaDataPool(mediaData)
            self.mres.addResult(compareDB, compareIO, albumMatchResults)
            del albumMatchResults
            del mediaData
            if verbose: ts.stop()
                
            self.compareIOs[compareDB] = None
        
        tsMatch.stop()
        
        
    def matchMediaDataPool(self, mediaData: Series) -> 'Series':
        albumMatchResults = poolMatchAlbums(mediaData, nCores=3, verbose=True)
        
        mediaResults = {}
        rankValues   = {"Loose": 0.7, "Medium": 0.8, "Tight": 0.9, "Exact": 0.95}
        for baseID,compareResults in albumMatchResults.groupby(level=0):
            mediaResults[baseID] = {}
            for (_,compareID),compareIDResult in compareResults.iteritems():
                df  = compareIDResult.apply(Series)
                key = (baseID,compareID)
                bestBaseMatch    = Series(df.max(axis=0).values, index=mediaData[key]["Compare"])
                bestCompareMatch = Series(df.max(axis=1).values, index=mediaData[key]["Base"])

                baseRankResult = {rank: bestBaseMatch[bestBaseMatch >= value].count() for rank,value in rankValues.items()}
                compareRankResult = {rank: bestCompareMatch[bestCompareMatch >= value].count() for rank,value in rankValues.items()}

                rankData = concat([Series(baseRankResult, name="Base"), Series(compareRankResult, name="Compare")], axis=1)

                mediaResults[baseID][compareID] = {"Rank": rankData,  "Raw": {"BestBaseMatch": bestBaseMatch, "BestCompareMatch": bestCompareMatch}}

        retval = Series(mediaResults)
        return retval

    def matchMediaData(self, artistNameMatches: DataFrame) -> 'Series':
        mediaResults = {}
        rankValues   = {"Loose": 0.7, "Medium": 0.8, "Tight": 0.9, "Exact": 0.95}
        for n,(baseID,nameMatchData) in enumerate(artistNameMatches.iterrows()):
            mediaResults[baseID] = {}

            baseNameMedia    = nameMatchData["BaseMedia"]
            baseMedia        = Series(baseNameMedia["Media"])
            compareNameMedia = Series(nameMatchData["CompareMedia"])

            ###############################################################################################
            ## Master Media String Comparisons
            ###############################################################################################
            compareResults = compareNameMedia.apply(lambda cNameMedia: Series(cNameMedia["Media"]).apply(lambda cMediaValue: baseMedia.apply(getLevenshtein, x2=cMediaValue)))
            for compareID,compareIDResult in compareResults.iteritems():
                bestBaseMatch    = Series(compareIDResult.max(axis=1).values, index=compareNameMedia[compareID]["Media"])
                bestCompareMatch = Series(compareIDResult.max(axis=0).values, index=baseNameMedia["Media"])

                baseRankResult = {rank: bestBaseMatch[bestBaseMatch >= value].count() for rank,value in rankValues.items()}
                compareRankResult = {rank: bestCompareMatch[bestCompareMatch >= value].count() for rank,value in rankValues.items()}

                rankData = concat([Series(baseRankResult, name="Base"), Series(compareRankResult, name="Compare")], axis=1)

                mediaResults[baseID][compareID] = {"Names": {"Base": baseNameMedia["Name"], "Compare": compareNameMedia[compareID]["Name"]},
                                                   "Rank": rankData, 
                                                   "Raw": {"BestBaseMatch": bestBaseMatch, "BestCompareMatch": bestCompareMatch}}

        retval = Series(mediaResults)
        return retval



    ################################################################################################################################################
    ## Utility Functions
    ################################################################################################################################################
    def selectArtistsForMediaMatch(self, artistMatchResults: Union[DataFrame,Series], artistNameCutoff: float) -> 'Series':
        if isinstance(artistMatchResults,DataFrame):
            nearArtistNameMatches = artistMatchResults.apply(lambda values: values[values >= artistNameCutoff].to_dict(), axis=1)
        elif isinstance(artistMatchResults,Series):
            nearArtistNameMatches = artistMatchResults.apply(lambda values: values[values >= artistNameCutoff].to_dict())
        if self.verbose: self.write(4, "Found {0} Name Results", nearArtistNameMatches.shape[0])
        artistNameMatches = nearArtistNameMatches[nearArtistNameMatches.apply(len) > 0]
        if self.verbose: self.write(4, "Found {0} Artists With One Or More Matches", artistNameMatches.shape[0])
        if self.verbose: self.write(4, "Found {0} Possible Matches", artistNameMatches.apply(len).sum())
        return artistNameMatches
    
    
    
    def prepareMediaData(self, artistNameMatches: Series, baseIO: MatchDBDataIO, compareIO: MatchDBDataIO) -> 'DataFrame':
        nameMatchValues = {}
        for baseid,compareValues in artistNameMatches.iteritems():
            for compareid,value in compareValues.items():
                key   = (baseid,compareid)
                nameMatchValues[key] = value
        baseids = [baseid for baseid,_ in Series(nameMatchValues).groupby(level=0)]
        compids = [compid for compid,_ in Series(nameMatchValues).groupby(level=1)]

        if self.verbose: ts = Timestat("Loading {0} Media Data".format(baseIO.db), ind=4)
        baseIO.loadMedia()
        baseMediaData = baseIO.getAvailableMedia()
        if self.verbose: ts.stop()
            
        if self.verbose: ts = Timestat("Loading {0} Media Data".format(compareIO.db), ind=4)
        compareIO.loadMedia(ids=compids)
        compareMediaData = compareIO.getAvailableMedia()
        if self.verbose: ts.stop()
        
        mediaData = {}
        for key in nameMatchValues.keys():
            baseid,compid = key
            mediaData[key] = {"Base": Series(baseMediaData[baseid]), "Compare": Series(compareMediaData[compid])}
        mediaData = Series(mediaData)
        return mediaData
    
    
    def selectMatches(self, albumMatchResults: Series) -> 'dict':
        rankResults = albumMatchResults.apply(lambda x: DataFrame({compareID: compareIDResult["Rank"].min(axis=1) for compareID,compareIDResult in x.items()}).T)
        nearResults = rankResults.apply(lambda df: df[((df["Medium"] <  self.matchReqs["Medium"]) & (df["Tight"] >= self.matchReqs["Tight"])) |  
                                                      ((df["Medium"] >=  self.matchReqs["Medium"]) & (df["Tight"] < self.matchReqs["Tight"]))])
        goodResults = rankResults.apply(lambda df: df[(df["Medium"] >= self.matchReqs["Medium"]) & (df["Tight"] >= self.matchReqs["Tight"])])

        multipleMatches = goodResults[goodResults.apply(lambda df: df.shape[0]) > 1]
        singleMatches   = goodResults[goodResults.apply(lambda df: df.shape[0]) == 1]
        nearMatches     = nearResults[nearResults.apply(lambda df: df.shape[0]) >= 1]
        self.write(4, "Found [{0} / {1} / {2}] Multi/Good/Near Matches", (multipleMatches.shape[0], singleMatches.shape[0], nearMatches.shape[0]))

        retval = {"Multiple": multipleMatches, "Single": singleMatches, "Near": nearMatches}
        return retval