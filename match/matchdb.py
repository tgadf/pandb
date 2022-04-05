""" Main Matching Class """

__all__ = ["MatchDB"]

from timeutils import Timestat
from listUtils import getFlatList
from .matchlev import getLevenshtein
from .dataio import MatchDBDataIO
from .albumreq import AlbumReq
from pandas import DataFrame, Series, concat
import dask.dataframe as dd

class MatchDB:
    def __init__(self, baseDB: str, compareDBs: list, reqs: dict, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        if self.verbose:
            print("#"*175)
            print("{0}  {1}  {2}".format("#"*70, "MatchDB()", "#"*(175-70-4-len("MatchDB()"))))
            print("#"*175)
            
        mask = reqs.get("Mask")
        assert isinstance(mask,(bool,str)), "Mask Req is not set."
        
        mediaTypes = reqs.get("Media")
        assert isinstance(mediaTypes,list), "Media Req is not set."
        self.mediaTypes = mediaTypes
        
        albumReqs = reqs.get("Albums")
        assert isinstance(albumReqs,dict), "Albums Req is not set."
        self.albumReqs = albumReqs
        
        self.nPart = reqs.get("NPart", 3)
        assert isinstance(self.nPart,int), "NPart Req is not set."
        
        self.matchReqs = reqs.get("Match")
        assert isinstance(self.matchReqs,dict), "Match Req is not set."
        assert isinstance(self.matchReqs.get('Artist'),float), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Medium'),int), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Tight'),int), "Artist match req is not set"
        
        self.baseIO = MatchDBDataIO(db=baseDB, mediaTypes=mediaTypes, mask=mask, verbose=False)
        assert isinstance(self.albumReqs.get(self.baseIO.db),AlbumReq), "Reqs does not have BaseDB [{0}]".format(baseIO.db)
        
        self.compareIOs = {}
        for compareDB in compareDBs:
            compareIO = MatchDBDataIO(db=compareDB, mediaTypes=mediaTypes, mask=mask, verbose=False)
            assert isinstance(self.albumReqs.get(compareIO.db),AlbumReq), "Reqs does not have CompareDB [{0}]".format(compareIO.db)
            self.compareIOs[compareDB] = compareIO
        

    def match(self, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)                
        tsMatch = Timestat("Matching [{0}] Against {1}".format(self.baseIO.db, list(self.compareIOs.keys())))
        
        print("")
        print("-"*150)
        print("{0} {1} {2}".format("-"*70, self.baseIO.db, "-"*(150-70-2-len(self.baseIO.db))))
        print("-"*150)
        baseIO = self.baseIO
        baseMediaMatchData = baseIO.getData(albums=self.albumReqs[baseIO.db])
        
        results = {}
        for compareDB,compareIO in self.compareIOs.items():
            print("")
            print("-"*150)
            print("{0} {1} {2}".format("-"*70, compareDB, "-"*(150-70-2-len(compareDB))))
            print("-"*150)
            compareMediaMatchData = compareIO.getData(albums=self.albumReqs[compareIO.db])
        
            ########################################################################################################################################
            ## 1) Match Artist Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] x {2} [{3}] Artist Names".format(baseMediaMatchData.shape[0], baseIO.db, compareMediaMatchData.shape[0], compareIO.db))
            daskDF = dd.from_pandas(baseMediaMatchData["Name"], npartitions=self.nPart)
            artistMatchResults = daskDF.map_partitions(lambda df: df.apply(lambda artistName: compareMediaMatchData["Name"].apply(getLevenshtein, x2=artistName))).compute(scheduler='processes')
            artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults, self.matchReqs['Artist'])
            mediaData          = self.prepareMediaData(artistNameMatches, baseMediaMatchData, compareMediaMatchData)
            if verbose: ts.stop()
            
            ########################################################################################################################################
            ## 2) Match Artist Albums Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] Album Names".format(mediaData.shape[0], baseIO.db))
            albumMatchResults = self.matchMediaData(mediaData)
            matchResults      = self.selectMatches(albumMatchResults)
            if verbose: ts.stop()

            ########################################################################################################################################
            ## 3) Cross Match Data
            ########################################################################################################################################
            compareCrossMatchIDs = getFlatList(matchResults["Single"].apply(lambda x: x.index).values)
            if len(compareCrossMatchIDs) == 0:
                print("Did not find any potential matches. Stopping process")
                tsMatch.update()
                continue
            if verbose: print("Found {0: >4} [{1}] Artists To Match Against {2}".format(len(compareCrossMatchIDs), compareIO.db, baseIO.db))
            baseMediaCrossMatchData    = compareIO.getData(ids=compareCrossMatchIDs)
            compareMediaCrossMatchData = baseIO.getData(albums=AlbumReq(min=2))
            
        
            ########################################################################################################################################
            ## 4) Cross Match Artist Names
            ########################################################################################################################################            
            if verbose: ts = Timestat("String Matching {0} [{1}] x {2} [{3}] Artist Names".format(baseMediaCrossMatchData.shape[0], compareIO.db, compareMediaCrossMatchData.shape[0], baseIO.db))
            daskDF = dd.from_pandas(baseMediaCrossMatchData["Name"], npartitions=self.nPart)
            artistMatchResults = daskDF.map_partitions(lambda df: df.apply(lambda artistName: compareMediaCrossMatchData["Name"].apply(getLevenshtein, x2=artistName))).compute(scheduler='processes')
            artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults, self.matchReqs["Artist"])
            mediaData          = self.prepareMediaData(artistNameMatches, baseMediaCrossMatchData, compareMediaCrossMatchData)
            if verbose: ts.stop()
            
            
            ########################################################################################################################################
            ## 5) Cross Match Artist Albums Names
            ########################################################################################################################################
            if verbose: ts = Timestat("String Matching {0} [{1}] Album Names".format(mediaData.shape[0], compareIO.db))
            albumCrossMatchResults = self.matchMediaData(mediaData)
            crossMatchResults      = self.selectMatches(albumCrossMatchResults)
            if verbose: ts.stop()
            
            
            ########################################################################################################################################
            ## 6) Get Final Matches
            ########################################################################################################################################
            baseCrossMatchIDs = crossMatchResults["Single"].apply(lambda matchResult: list(matchResult.index)[0])
            crossMatchDF = DataFrame(matchResults["Single"].apply(lambda matchResult: list(matchResult.index)[0]), columns=["CompareID"])
            crossMatchDF["BaseIDMap"] = crossMatchDF["CompareID"].map(baseCrossMatchIDs)
            correctMatches = crossMatchDF[crossMatchDF.index == crossMatchDF["BaseIDMap"]]
            results[compareDB] = correctMatches
            
            tsMatch.update()
        
        self.results = results
        tsMatch.stop()
            

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
    def selectArtistsForMediaMatch(self, artistMatchResults: DataFrame, artistNameCutoff: float) -> 'Series':        
        nearArtistNameMatches = artistMatchResults.apply(lambda values: values[values >= artistNameCutoff].to_dict(), axis=1)
        if self.verbose: print("   Found {0} Name Results".format(nearArtistNameMatches.shape[0]))
        artistNameMatches = nearArtistNameMatches[nearArtistNameMatches.apply(len) > 0]
        if self.verbose: print("   Found {0} Artists With One Or More Matches".format(artistNameMatches.shape[0]))
        if self.verbose: print("   Found {0} Possible Matches".format(artistNameMatches.apply(len).sum()))    
        return artistNameMatches

    def prepareMediaData(self, artistNameMatches: Series, baseMediaMatchData: DataFrame, compareMediaMatchData: DataFrame) -> 'DataFrame':
        def getMatchMediaData(row):
            return {"Name": row["Name"], "Media": list(set(getFlatList([mediaValues for mediaValues in row[self.mediaTypes].values if isinstance(mediaValues,list)])))}
        baseMediaValues    = baseMediaMatchData.loc[artistNameMatches.index].apply(lambda row: getMatchMediaData(row), axis=1)
        compareMediaValues = artistNameMatches.apply(lambda compareIDMatches: {compareID: getMatchMediaData(compareMediaMatchData.loc[compareID]) for compareID in compareIDMatches.keys()})

        retval = concat([baseMediaValues,compareMediaValues], axis=1)
        retval.columns = ["BaseMedia", "CompareMedia"]    
        return retval
    
    def selectMatches(self, albumMatchResults: Series) -> 'dict':
        rankResults = albumMatchResults.apply(lambda x: DataFrame({compareID: compareIDResult["Rank"].min(axis=1) for compareID,compareIDResult in x.items()}).T)
        nearResults = rankResults.apply(lambda df: df[((df["Medium"] <  self.matchReqs["Medium"]) & (df["Tight"] >= self.matchReqs["Tight"])) |  
                                                      ((df["Medium"] >=  self.matchReqs["Medium"]) & (df["Tight"] < self.matchReqs["Tight"]))])
        goodResults = rankResults.apply(lambda df: df[(df["Medium"] >= self.matchReqs["Medium"]) & (df["Tight"] >= self.matchReqs["Tight"])])

        multipleMatches = goodResults[goodResults.apply(lambda df: df.shape[0]) > 1]
        print("  ==> Found {0: >4} Multiple Good Matches".format(multipleMatches.shape[0]))
        singleMatches   = goodResults[goodResults.apply(lambda df: df.shape[0]) == 1]
        print("  ==> Found {0: >4} Good Matches".format(singleMatches.shape[0]))
        nearMatches     = nearResults[nearResults.apply(lambda df: df.shape[0]) >= 1]
        print("  ==> Found {0: >4} Near Matches".format(nearMatches.shape[0]))

        retval = {"Multiple": multipleMatches, "Single": singleMatches, "Near": nearMatches}
        return retval