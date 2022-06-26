""" Quick Summary of PanDB Match Status """

__all__ = ["MatchCounts", "MatchDBCounts"]

from gate import IOStore
from musicdb import PanDBIO
from pandas import DataFrame, Series, isna
from master import MasterMetas, MasterDBs

class MatchCounts:
    def __init__(self):
        mdbs = MasterDBs()
        self.mcs  = {db: MatchDBCounts(db, verbose=False) for db in mdbs.getDBs()}
        
    def get(self, db=None):
        retval = self.mcs if db is None else self.mcs.get(db)
        return retval
    
    def getDF(self):
        mcDF = Series({db: mdbc.stats for db,mdbc in self.mcs.items()}).apply(Series).T
        return mcDF
    

class MatchDBCounts:
    def __init__(self, db, mediaTypes=None, **kwargs):
        mm = MasterMetas()
        if mediaTypes is None:
            mediaTypes = list(mm.getMedias().values())
        elif isinstance(mediaTypes,list):
            pass
        elif isinstance(mediaTypes,str):
            mediaTypes = [mediaTypes]
        assert isinstance(mediaTypes,list),f"MediaTypes [{mediaTypes}] is not a list!"
        self.verbose = kwargs.get('verbose', False)
        if self.verbose: print("========= {0} =========".format(db))
        
        pdbio = PanDBIO()
        ios   = IOStore()
        mdbio = ios.get(db)
        
        self.searchData    = DataFrame()
        self.matchedIDs    = DataFrame()
        self.unmatchedData = DataFrame()
        self.stats         = Series(dtype='object')        
        
        try:
            summaryNameData   = mdbio.data.getSummaryNameData()
            summaryNameData.name = "ArtistName"
        except:
            print("Could not get SummaryName Data")
            summaryNameData = None
            
        try:
            searchNameData = mdbio.data.getSearchNameData()
        except:
            print("Could not get SearchName Data")
            searchNameData = None
            
        try:
            summaryNumAlbumsData = mdbio.data.getSummaryNumAlbumsData()
        except:
            print("Could not get SummaryNumAlbums Data")
            summaryNumAlbumsData = None
            
        try:
            summaryCountsData = mdbio.data.getSummaryCountsData()
        except:
            print("Could not get SummaryCounts Data")
            summaryCountsData = None

        if isinstance(mediaTypes,list):
            try:
                summaryCountsData = summaryCountsData[mediaTypes].rename(columns={col: "Num{0}".format(col) for col in summaryCountsData.columns})
                summaryCountsData["NumMedia"] = summaryCountsData.sum(axis=1)
            except:
                print("Could not manipulate SummaryCounts Data")
                summaryCountsData = None
        else:
            try:
                summaryCountsData["NumMedia"] = summaryCountsData.sum(axis=1)
            except:
                print("Could not manipulate SummaryCounts Data")
                summaryCountsData = None
            
        
        ###################################
        # Join Data
        ###################################
        try:
            self.searchData = DataFrame(searchNameData).join(summaryNameData).join(summaryNumAlbumsData).join(summaryCountsData)
            self.searchData = self.searchData.drop(["Name"], axis=1)
        except:
            print("Could not join all data")
            self.searchData = DataFrame()
        #self.searchData = self.searchData.drop(['Name'], axis=0)
        if self.verbose: print("  ===> Found {0: >7}  Artists Search Data".format(self.searchData.shape[0]))
            
        
        ###################################
        # Get Master Data
        ###################################
        try:
            self.pdbidLookup = pdbio.getIndexLookup(db)
            self.pdbidLookup.name = "PanDBID"
        except:
            print("Could not get PanDBID Lookup")
            self.pdbidLookup = Series(dtype='object')

        self.searchData = self.searchData.join(self.pdbidLookup)
        def matchType(x):
            if isinstance(x,str):
                return 1
            elif isinstance(x,list):
                return 2
            elif isna(x):
                return 0
            else:
                raise ValueError(f"Not sure what pdbid [{x}] this is...")
                
        self.searchData["MatchType"] = self.searchData["PanDBID"].apply(matchType)

        numMatched   = self.searchData[self.searchData["MatchType"] > 0].shape[0]
        numArtists   = self.searchData.shape[0]
        numUnMatched = numArtists - numMatched
        if numArtists > 0:
            self.fracMatched = round(100*numMatched/numArtists,1)
        else:
            self.fracMatched = -1.0
        if self.verbose: print(f"  ===> Found {numUnMatched: >7}  Unmatched Artists Search Data")
        if self.verbose: print(f"  ===> Found {self.fracMatched: >7}% Fraction Matched Artists")

        try:
            self.unmatchedData = self.searchData[self.searchData["MatchType"] == 0].sort_values(by="NumMedia", ascending=False)
            self.stats = Series({"fM": self.fracMatched, "Max": self.unmatchedData["NumMedia"].max(),
                          "10": self.unmatchedData["NumMedia"].head(10).min(), "100": self.unmatchedData["NumMedia"].head(100).min(),
                          "500": self.unmatchedData["NumMedia"].head(500).min(), "2500": self.unmatchedData["NumMedia"].head(2500).min()}, name=db)
        except:
            print("Could not sort media data")
            self.unmatchedData = DataFrame()
            self.stats = Series(dtype='object')
            
        if self.verbose:
            print("  Unmatched Stats:")
            for k,v in self.stats.items():
                print("    {0: <4} ==> {1}".format(k,v))

        

    def get(self):
        return self.searchData.sort_values(by="NumMedia", ascending=False)
            
    def getUnmatched(self):
        return self.unmatchedData.sort_values(by="NumMedia", ascending=False)