""" Main Matching Class """

__all__ = ["MatchDB"]

from timeutils import Timestat
from listUtils import getFlatList
from .matchlev import getLevenshtein
from .dataio import MatchDBDataIO
from .albumreq import AlbumReq
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
        
        albumReqs = reqs.get("Albums")
        assert isinstance(albumReqs,dict), "Albums Req is not set."
        self.albumReqs = albumReqs
        
        self.nPart = reqs.get("NPart", 2)
        assert isinstance(self.nPart,int), "NPart Req is not set."
        
        self.matchReqs = reqs.get("Match")
        assert isinstance(self.matchReqs,dict), "Match Req is not set."
        assert isinstance(self.matchReqs.get('Artist'),float), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Medium'),int), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Tight'),int), "Artist match req is not set"
        
        self.baseIO = MatchDBDataIO(db=baseDB, mediaTypes=mediaTypes, mask=mask, verbose=False, base=True)
        assert isinstance(self.albumReqs.get(self.baseIO.db),AlbumReq), "Reqs does not have BaseDB [{0}]".format(baseIO.db)
        
        self.compareIOs = {}
        for compareDB in compareDBs:
            compareIO = MatchDBDataIO(db=compareDB, mediaTypes=mediaTypes, mask=mask, verbose=False, base=False)
            assert isinstance(self.albumReqs.get(compareIO.db),AlbumReq), "Reqs does not have CompareDB [{0}]".format(compareIO.db)
            self.compareIOs[compareDB] = compareIO
        
        self.diagnostics = {}
        
        pbar = ProgressBar()
        pbar.register()
        

    def match(self, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)                
        tsMatch = Timestat("Matching [{0}] Against {1}".format(self.baseIO.db, list(self.compareIOs.keys())), ind=0)
        
        print("")
        print("-"*150)
        print("{0} {1} {2}".format("-"*70, self.baseIO.db, "-"*(150-70-2-len(self.baseIO.db))))
        baseIO = self.baseIO
        baseIO.loadNames()
        baseIO.setAvailableNames(self.albumReqs[baseIO.db])
        #baseMediaMatchData = baseIO.getData(albums=)
        
        results = {}
        for compareDB,compareIO in self.compareIOs.items():
            print("")
            print("-"*150)
            print("{0} {1} {2}".format("-"*70, compareDB, "-"*(150-70-2-len(compareDB))))
            compareIO.loadNames()
            compareIO.setAvailableNames(self.albumReqs[compareIO.db])
            #compareMediaMatchData = compareIO.getData(albums=self.albumReqs[compareIO.db])
            self.diagnostics[compareDB] = {}
        
            ########################################################################################################################################
            ## 1) Match Artist Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] x {2} [{3}] Artist Names".format(baseIO.getNumNames(), baseIO.db, compareIO.getNumNames(), compareIO.db), ind=2)
            if self.usePool:
                artistMatchResults = poolMatchNames(baseNames=baseIO.getAvailableNames(), compNames=compareIO.getAvailableNames(), nCores=self.nPart, progress=True)
                #artistMatchResults = poolMatchNames(baseNames=baseMediaMatchData["Name"], compNames=compareMediaMatchData["Name"], nCores=self.nPart, progress=True)
            elif self.useDask:
                daskDF = dd.from_pandas(baseIO.getAvailableNames(), npartitions=self.nPart)
                artistMatchResults = daskDF.map_partitions(lambda df: df.apply(lambda artistName: compareIO.getAvailableNames().apply(getLevenshtein, x2=artistName))).compute(scheduler='processes')
            artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults, self.matchReqs['Artist'])
            mediaData          = self.prepareMediaData(artistNameMatches, baseIO, compareIO)
            del artistMatchResults
            del artistNameMatches
            if verbose: ts.stop()
            
            
            ########################################################################################################################################
            ## 2) Match Artist Albums Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] Album Names".format(mediaData.shape[0], baseIO.db), ind=2)
            if self.usePool is True:
                albumMatchResults = self.matchMediaDataPool(mediaData)
            elif self.useDask is True:
                albumMatchResults = self.matchMediaData(mediaData)
            matchResults      = self.selectMatches(albumMatchResults)
            del albumMatchResults
            del mediaData
            if verbose: ts.stop()
                

            ########################################################################################################################################
            ## 3) Cross Match Data
            ########################################################################################################################################
            compareCrossMatchIDs = getFlatList(matchResults["Single"].apply(lambda x: x.index).values)
            if len(compareCrossMatchIDs) == 0:
                self.write(2, "Did not find any potential matches. Stopping process")
                tsMatch.update()
                continue
            print(compareCrossMatchIDs)
            
            compareIO.setAvailableNames(compareCrossMatchIDs) ## This will be the 'base'
            baseIO.setAvailableNames(AlbumReq(min=2))
            
            
            #return
            #baseMediaCrossMatchData    = compareIO.getData(ids=compareCrossMatchIDs)
            #compareMediaCrossMatchData = baseIO.getData(albums=AlbumReq(min=2))
            
        
            ########################################################################################################################################
            ## 4) Cross Match Artist Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] x {2} [{3}] Artist Names".format(compareIO.getNumNames(), compareIO.db, baseIO.getNumNames(), baseIO.db), ind=2)
            artistMatchResults = poolMatchNames(baseNames=compareIO.getAvailableNames(), compNames=baseIO.getAvailableNames(), nCores=self.nPart, progress=True)
            artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults, self.matchReqs["Artist"])
            mediaData          = self.prepareMediaData(artistNameMatches, compareIO, baseIO)
            del artistMatchResults
            del artistNameMatches
            if verbose: ts.stop()
            return
            
            
            ########################################################################################################################################
            ## 5) Cross Match Artist Albums Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] Album Names".format(mediaData.shape[0], compareIO.db), ind=2)
            if self.usePool is True:
                albumCrossMatchResults = self.matchMediaDataPool(mediaData)
            elif self.useDask is True:
                albumCrossMatchResults = self.matchMediaData(mediaData)
            crossMatchResults      = self.selectMatches(albumCrossMatchResults)
            del mediaData
            del albumCrossMatchResults
            if verbose: ts.stop()
            
            
            ########################################################################################################################################
            ## 6) Get Final Matches
            ########################################################################################################################################
            baseCrossMatchIDs = crossMatchResults["Single"].apply(lambda matchResult: list(matchResult.index)[0])
            crossMatchDF = DataFrame(matchResults["Single"].apply(lambda matchResult: list(matchResult.index)[0]), columns=["CompareID"])
            crossMatchDF["BaseIDMap"] = crossMatchDF["CompareID"].map(baseCrossMatchIDs)
            correctMatches = crossMatchDF[crossMatchDF.index == crossMatchDF["BaseIDMap"]]
            results[compareDB] = correctMatches
            del baseCrossMatchIDs
            del crossMatchDF
            
            tsMatch.update()
        
        self.results = results
        tsMatch.stop()
        
        
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
            
        
        try:
            baseMediaValues = baseMediaMatchData.loc[artistNameMatches.index]
        except:
            print("artistNameMatches:")
            print(artistNameMatches.head())
            print(artistNameMatches.index)
            print("baseMediaMatchData:")
            print(baseMediaMatchData.head())
            raise ValueError("Error subselecting artists for media gathering")

            
        try:
            baseMediaValues = baseMediaValues.apply(lambda row: getMatchMediaData(row), axis=1)
        except:
            print("self.mediaTypes:")
            print(self.mediaTypes)
            print("baseMediaValues:")
            print(baseMediaValues.head())
            raise ValueError("Error calling baseMediaValues = baseMediaValues.apply(lambda row: getMatchMediaData(row), axis=1)")
        #print("")
        #print(type(baseMediaValues))
        #print(baseMediaValues.head())
        compareMediaValues = artistNameMatches.apply(lambda compareIDMatches: {compareID: getMatchMediaData(compareMediaMatchData.loc[compareID]) for compareID in compareIDMatches.keys()})
        #print("")
        #print(type(compareMediaValues))
        #print(compareMediaValues.head())

        try:
            baseMediaValues.name = "BaseMedia"
            compareMediaValues.name = "CompareMedia"
            retval = DataFrame(baseMediaValues).join(compareMediaValues)
            #retval = concat([baseMediaValues,compareMediaValues], axis=1)
            #retval.columns = ["BaseMedia", "CompareMedia"]    
        except:
            self.diagnostics["ConcatError"] = {"Base": baseMediaValues, "Compare": compareMediaValues}
            raise ValueError("Something went really wrong when running retval = concat([baseMediaValues,compareMediaValues], axis=1) in prepareMediaData(). See diagnotic for data")
        return retval
    
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