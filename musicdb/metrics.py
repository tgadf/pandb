""" Primary Music Database IO """

__all__ = ["PanDBMetrics"]

from master import MasterDBs
from gate import IDStore, IOStore
from timeutils import Timestat
from pandas import to_numeric, Series, DataFrame
from tabulate import tabulate
from .pandb import PanDB
            
####################################################################################################################    
## Summary
####################################################################################################################    
class PanDBMetrics:
    def __init__(self, **kwargs):
        self.pdb      = PanDB(**kwargs)
        self.ids      = IDStore()
        self.verbose  = kwargs.get('verbose', True)
        self.mmeDF    = None
        
        
    ####################################################################################################################    
    ## Summary
    ####################################################################################################################    
    def summary(self):
        self.mmeDF = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        dbs = MasterDBs().getDBs()
        dbCounts = self.mmeDF[dbs].count()
        dbCounts.name = "MatchedID"
        print(tabulate(DataFrame(dbCounts), headers='keys', tablefmt='rst'))        
        

    ####################################################################################################################    
    ## Append Metadata To DataFrame
    ####################################################################################################################
    def addMetrics(self, **kwargs):
        self.verbose = kwargs.get('verbose', self.verbose)
        orderit = kwargs.get('order', True)
        saveit = kwargs.get('save', False)
        ts = Timestat("Adding Metrics To PanDB")
        ios = IOStore()

        ######################################################################
        # Calculations
        ######################################################################
        if self.verbose: print("  Loading Albums Data")
        dbAlbums = {}
        for db,mdbio in ios.get().items():
            if self.verbose: print(f"    ==> {db}")
            numAlbums = mdbio.data.getSummaryNumAlbumsData()
            dbAlbums[db] = numAlbums if isinstance(numAlbums,Series) else Series(dtype='object')
        if self.verbose: print("  Getting DB Albums")
        dfAlbums = DataFrame({db: to_numeric(dbIDMatches.apply(dbAlbums[db].get), errors='coerce') for db,dbIDMatches in mmeDF.items() if db in dbAlbums})
        if self.verbose: print("  Getting DB Rank")
        dfRank   = DataFrame({db: dbIDs[dbIDs.notna()].rank(pct=True) for db,dbIDs in dfAlbums.items()}).fillna(0.0).mean(axis=1).rank(pct=True)
        dfRank.name = "Rank"

        ######################################################################
        # Join Rank
        ######################################################################
        if self.verbose: print("  Adding Rank")
        if "Rank" in mmeDF.columns:
            mmeDF.drop(["Rank"], axis=1, inplace=True)
        mmeDF         = mmeDF.join(dfRank)
        mmeDF["Rank"] = mmeDF["Rank"].fillna(0.0).rank(method='max', ascending=False).apply(int)

        ######################################################################
        # Join Albums
        ######################################################################   
        if self.verbose: print("  Adding Albums")     
        if "Albums" in mmeDF.columns:
            mmeDF.drop(["Albums"], axis=1, inplace=True)
        dfAllAlbums = dfAlbums.sum(axis=1).fillna(0).astype(int)
        dfAllAlbums.name = "Albums"
        mmeDF           = mmeDF.join(dfAllAlbums)
        mmeDF["Albums"] = to_numeric(mmeDF["Albums"], errors='coerce').fillna(0)

        ######################################################################
        # Join Counts
        ######################################################################       
        if self.verbose: print("  Adding Counts")       
        if "Counts" in mmeDF.columns:
            mmeDF.drop(["Counts"], axis=1, inplace=True)
        mmeDF["Counts"] = mmeDF.drop(["Rank", "Albums", "ArtistName"], axis=1).count(axis=1)
        self.mmeDF = mmeDF

        ts.stop()

        if orderit is True:
            self.orderColumns()
        elif saveit is True:      
            if self.verbose: print("  Saving Results")
            self.pdb.saveData(data=mmeDF)
            
            
            
    def sortByRank(self):
        if self.verbose: print("Sorting By Rank")
        self.mmeDF = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        self.mmeDF = self.mmeDF.sort_values(by=["Rank"], ascending=True)
        self.pdb.saveData(data=self.mmeDF)
        
    def sortByAlbums(self):
        if self.verbose: print("Sorting By Albums")
        self.mmeDF = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        self.mmeDF = self.mmeDF.sort_values(by=["Albums"], ascending=False)    
        self.pdb.saveData(data=self.mmeDF)    
        
    def sortByCounts(self):
        if self.verbose: print("Sorting By Counts/Albums")
        self.mmeDF = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        self.mmeDF = self.mmeDF.sort_values(by=["Counts", "Albums"], ascending=False)
        self.pdb.saveData(data=self.mmeDF)     
        
    def orderColumns(self, sort=True):
        if self.verbose: print("Ordering Columns")
        self.mmeDF = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        dbs = MasterDBs().getDBs()
        try:
            self.mmeDF = self.mmeDF[["ArtistName"] + ['Rank', "Counts", "Albums"] + dbs]
        except:
            raise ValueError("Need to call addMetrics first")

        if sort is True:
            self.sortByRank()