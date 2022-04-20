""" Quick Summary of PanDB Match Status """

__all__ = ["MatchCounts", "MatchDBCounts"]

from gate import IOStore
from musicdb import PanDBIO
from pandas import DataFrame, Series

class MatchCounts:
    def __init__(self):
        from master import MasterDBs
        self.mcs  = {db: MatchDBCounts(db, verbose=False) for db in MasterDBs().getDBs()}
        
    def get(self, db=None):
        retval = self.mcs if db is None else self.mcs.get(db)
        return retval
    
    def getDF(self):
        mcDF = Series({db: mdbc.stats for db,mdbc in self.mcs.items()}).apply(Series).T
        return mcDF
    

class MatchDBCounts:
    def __init__(self, db, mediaTypes=["Album", "SingleEP"], **kwargs):
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
            
        try:
            summaryCountsData = summaryCountsData[mediaTypes].rename(columns={col: "Num{0}".format(col) for col in summaryCountsData.columns})
            summaryCountsData["NumMedia"] = summaryCountsData.sum(axis=1)
        except:
            print("Could not manipulate SummaryCounts Data")
            summaryCountsData = None
            
        
        ###################################
        # Join Data
        ###################################
        try:
            self.searchData = DataFrame(searchNameData).join(summaryNameData).join(summaryNumAlbumsData).join(summaryCountsData)
        except:
            print("Could not join all data")
            self.searchData = DataFrame()
        #self.searchData = self.searchData.drop(['Name'], axis=0)
        if self.verbose: print("  ===> Found {0: >7}  Artists Search Data".format(self.searchData.shape[0]))

        
        try:
            self.matchedIDs = DataFrame(Series({idx: 1 for idx in eval("pdbio.get{0}Data()".format(db)) if isinstance(idx,str)}, name="Matched"))
        except:
            print("Could not get matched IDs")
            self.matchedIDs = DataFrame(Series({}, name="Matched"))

        self.searchData = self.searchData.join(self.matchedIDs)
        self.searchData["Matched"] = self.searchData["Matched"].fillna(0).astype(int)

        numMatched = self.searchData["Matched"].sum()
        numArtists = self.searchData.shape[0]
        if numArtists > 0:
            self.fracMatched = round(100*numMatched/numArtists,1)
        else:
            self.fracMatched = -1.0
        if self.verbose: print("  ===> Found {0: >7}  Unmatched Artists Search Data".format(self.searchData["Matched"].sum()))
        if self.verbose: print("  ===> Found {0: >7}% Fraction Matched Artists".format(self.fracMatched))

        try:
            self.unmatchedData = self.searchData[self.searchData["Matched"] == 0].sort_values(by="NumMedia", ascending=False)
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
        return self.searchData
            
    def getUnmatched(self):
        return self.unmatchedData