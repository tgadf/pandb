""" Main Matching Class """

__all__ = ["CrossMatchDB"]

from timeutils import Timestat
from listUtils import getFlatList
from .matchlev import getLevenshtein
from .dataio import MatchDBDataIO
from .albumreq import MatchReq
from .results import MatchResults, CrossMatchResults
from .utils import write
from .pool import poolMatchNames, poolMatchAlbums
from pandas import DataFrame, Series, concat
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from typing import Union
        
    
class CrossMatchDB:
    def __init__(self, compareDB, mres: DataFrame, reqs: dict, **kwargs):
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
        assert isinstance(mreqs,dict), "Albums Req is not set."
        self.mreqs = mreqs
        
        self.nPart = reqs.get("NPart", 2)
        assert isinstance(self.nPart,int), "NPart Req is not set."
        
        self.matchReqs = reqs.get("Match")
        assert isinstance(self.matchReqs,dict), "Match Req is not set."
        assert isinstance(self.matchReqs.get('Artist'),float), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Medium'),int), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Tight'),int), "Artist match req is not set"
        
        self.compareIO = MatchDBDataIO(db=compareDB, mediaTypes=mediaTypes, mask=mask, verbose=False, base=False)
        assert isinstance(self.mreqs.get(self.compareIO.db),MatchReq), "Reqs does not have BaseDB [{0}]".format(compareIO.db)
        
        self.mres = mres
        self.cmres = CrossMatchResults()
        self.save = self.cmres.save
        self.results = {}
        

    def match(self, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)
        baseDBs = list(self.mres["DB"].unique())
        tsMatch = Timestat("Cross Matching [{0}] Against {1}".format(self.compareIO.db, baseDBs), ind=0)
        
        compareIO = self.compareIO        
        compareIO.loadNames()
        compareIO.setAvailableNames(self.mreqs[compareIO.db])
                
        index = self.mres.apply(lambda row: (row["BaseID"],row["DB"],row["CompareID"]), axis=1)
        baseNames = self.mres['Match'].apply(lambda x: x["Info"]["Name"])
        baseMediaData = self.mres["Match"].apply(lambda x: x["Media"])
        baseNames.index = index
        baseNames.name = "Name"
        baseMediaData.index = index
        baseMediaData.name = "Media"
        self.cmres.setBaseNames(baseNames)

        ########################################################################################################################################
        ## 1) Match Artist Names
        ########################################################################################################################################
        if verbose: ts = Timestat("String Matching {0} {1} x {2} [{3}] Artist Names".format(len(baseNames), baseDBs, compareIO.getNumNames(), compareIO.db), ind=2)
        artistMatchResults = poolMatchNames(baseNames=baseNames, compNames=compareIO.getAvailableNames(), nCores=self.nPart, progress=True)
        artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults, self.matchReqs['Artist'])
        mediaData          = self.prepareMediaData(artistNameMatches, baseMediaData, compareIO)
        del artistMatchResults
        del artistNameMatches
        if verbose: ts.stop()
            
            
        ########################################################################################################################################
        ## 2) Match Artist Albums Names
        ########################################################################################################################################
        if verbose: ts = Timestat("String Matching {0} {1} Album Names".format(mediaData.shape[0], baseDBs), ind=2)
        albumMatchResults = self.matchMediaDataPool(mediaData)
        self.cmres.addResult(compareIO.db, compareIO, albumMatchResults)
        del albumMatchResults
        del mediaData
        if verbose: ts.stop()
                
        compareIO = None
        
        
    def matchMediaDataPool(self, mediaData: Series) -> 'Series':
        albumMatchResults = poolMatchAlbums(mediaData, verbose=True)
        
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
    
    
    
    def prepareMediaData(self, artistNameMatches: Series, baseMediaData: Series, compareIO: MatchDBDataIO) -> 'DataFrame':
        nameMatchValues = {}
        for baseid,compareValues in artistNameMatches.iteritems():
            for compareid,value in compareValues.items():
                key   = (baseid,compareid)
                nameMatchValues[key] = value
        compids = [compid for compid,_ in Series(nameMatchValues).groupby(level=1)]        
        compareIO.loadMedia(ids=compids)
        compareMediaData = compareIO.getAvailableMedia()        

        mediaData = {}
        for key in nameMatchValues.keys():
            baseid,compid = key
            mediaData[key] = {"Base": Series(baseMediaData[baseid]), "Compare": Series(compareMediaData[compid])}
        mediaData = Series(mediaData)
        return mediaData