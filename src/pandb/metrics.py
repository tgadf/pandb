""" Primary Music Database IO """

__all__ = ["PanDBMetrics"]

from dbmaster import MasterDBs, MasterMetas
from musicdb import getdbios
from utils import Timestat
from hashlib import md5
from pandas import Series, DataFrame, merge, concat
from tabulate import tabulate
from .pandb import PanDB
            

###############################################################################
# Global PanDB Metrics Class
###############################################################################
class PanDBMetrics:
    def __init__(self, **kwargs):
        self.pdb = PanDB(**kwargs)
        self.verbose = kwargs.get('verbose', True)
        self.mmeDF = None
        
        self.dbMediaCounts = None
        self.dbRankValues = None
        self.dbCounts = None
        self.subRank = None
        
    ###########################################################################
    # Get Data
    ###########################################################################
    def setData(self):
        self.mmeDF = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        
    ###########################################################################    
    # Save Data
    ###########################################################################    
    def saveData(self):
        assert isinstance(self.mmeDF, DataFrame), "Nothing has been altered with the PanDB Data"
        ts = Timestat("Saving PanDB Data With Metrics")
        self.pdb.saveData(data=self.mmeDF)
        ts.stop()
        
    ###########################################################################    
    # Summary
    ###########################################################################    
    def summary(self):
        self.setData()
        dbs = MasterDBs().getDBs()
        dbMediaCounts = self.mmeDF[dbs].count()
        dbMediaCounts.name = "MatchedID"
        print(tabulate(DataFrame(dbMediaCounts), headers='keys', tablefmt='rst'))
        
    ###########################################################################
    # Append Metadata To DataFrame
    ###########################################################################
    def getAlbumCounts(self, **kwargs):
        self.setData()
        self.verbose = kwargs.get('verbose', self.verbose)
        mm  = MasterMetas()
        rankCounts = mm.getMediaRanks()
        ts = Timestat("Loading DB Album Counts", ind=2)
        dbSummaryCounts = {db: mdbio.data.getSummaryCountsData() for db,mdbio in getdbios().items()}
        ts.stop()
        
        ts            = Timestat("Creating PanDB ID AlbumCounts", ind=2)
        dbMediaCounts      = {}
        rankCounts    = mm.getMediaRanks()
        for db,dbCountValues in dbSummaryCounts.items():
            dbids         = self.mmeDF[self.mmeDF[db].notna()][[db]].copy(deep=True)
            if isinstance(dbCountValues,DataFrame):
                dbRankCounts  = {}
                for rank,rankCols in rankCounts.items():
                    cols = [col for col in rankCols if col in dbCountValues.columns]
                    dbRankCounts[rank] = dbCountValues[cols].sum(axis=1) if len(cols) >0 else Series({}, dtype='object')
                    #dbRankCounts = concat([dbRankCounts,dbRankCount]) if isinstance(dbRankCounts
                    #dbRankCounts = concat({rank: dbCountValues[rankCols].sum(axis=1) for rank,rankCols in rankCounts.items()}, axis=1)
                    
                dbRankCounts  = concat(dbRankCounts, axis=1).reset_index().rename(columns={"index": db})
                pdbIDCounts   = merge(dbids, dbRankCounts, on=db, how='left')
                pdbIDCounts   = pdbIDCounts.drop([db], axis=1)
                pdbIDCounts.index = dbids.index
            else:
                pdbIDCounts      = dbids
                for rank in rankCounts.keys():
                    pdbIDCounts[rank] = 0
            dbMediaCounts[db]  = pdbIDCounts
            #ts.update(cmt=db)
            
        self.dbMediaCounts = dbMediaCounts
        ts.stop()
        
        ts = Timestat("Summing Rank Counts", ind=2)
        dbCountValues = {rank: concat({db: dbRankCounts[rank] for db,dbRankCounts in self.dbMediaCounts.items()}, axis=1).fillna(0.0).sum(axis=1) for rank in rankCounts.keys()}
        dbCountValues = concat(dbCountValues, axis=1).fillna(0.0)
        self.dbCountValues = self.mmeDF[["ArtistName"]].copy(deep=True)
        for rank in rankCounts.keys():
            dbMediaRankCount = dbCountValues[rank].astype(int)
            dbMediaRankCount.name = f"{rank}Count"
            self.dbCountValues = self.dbCountValues.join(dbMediaRankCount)
        self.dbCountValues = self.dbCountValues.drop(["ArtistName"], axis=1).fillna(0.0).astype(int)
        ts.stop()    
        
        
    def getAlbumRanks(self, **kwargs):
        self.setData()
        self.verbose = kwargs.get('verbose', self.verbose)
        mm   = MasterMetas()
        mdbs = MasterDBs()
        rankCounts = mm.getMediaRanks()
        dbTypes    = mdbs.getDBTypes()
        dbWeight   = mdbs.getDBWeights()
        #dbWeight   = Series(dbTypes).map(self.dbTypeWeights.get)
        
        ts = Timestat("Computing DB Ranks", ind=2)
        dbCountRank   = {db: dbRankCounts.rank(pct=True) for db,dbRankCounts in self.dbMediaCounts.items()}
        dbRankValues  = {rank: concat({db: dbRank[rank] for db,dbRank in dbCountRank.items()}, axis=1).fillna(0.0) for rank in rankCounts.keys()}
        dbRankValues  = {rank: self.mmeDF[["ArtistName"]].join(dbRankValues[rank]).fillna(0.0).drop(["ArtistName"], axis=1) for rank in rankCounts.keys()}
        self.dbRankValues = self.mmeDF[["ArtistName"]].copy(deep=True)
        #return dbWeight,dbRankValues
        for rank in rankCounts.keys():
            #print(f"**** {rank} ****")
            #print(f"Columns = {dbRankValues[rank].columns}")
            #print(f"Weight  = {dbWeight}")
            weightedRank = dbRankValues[rank].dot(dbWeight).rank(ascending=False).astype(int)
            weightedRank.name = f"{rank}Rank"
            self.dbRankValues = self.dbRankValues.join(weightedRank)
        self.dbRankValues = self.dbRankValues.drop(["ArtistName"], axis=1)
        ts.stop()
        
        
    def getCounts(self, **kwargs):
        self.setData()
        self.verbose = kwargs.get('verbose', self.verbose)
        mdbs = MasterDBs()
        dbs  = mdbs.getDBs()
        ts = Timestat("Computing Matched DBs", ind=2)
        self.dbCounts = self.mmeDF[dbs].count(axis=1)
        self.dbCounts.name = "Counts"
        ts.stop()
        
        
    def getSubRank(self, **kwargs):
        self.setData()
        self.verbose = kwargs.get('verbose', self.verbose)
        retval = {}
        
        def getHash(name):
            m = md5()
            m.update(name.upper().encode())
            return m.hexdigest()
        
        ts = Timestat("Creating Artist Hashes", ind=2)
        self.mmeDF["Hash"] = self.mmeDF["ArtistName"].map(getHash)
        ts.stop()
        
        ts = Timestat("Getting Sub Rank", ind=2)
        N = self.mmeDF["Hash"].nunique()
        for n,(hval,gdf) in enumerate(self.mmeDF[["Hash"]].groupby('Hash')):
            idx = gdf.index
            N   = len(idx)
            val = tuple(zip(range(1,N+1),[N]*N))
            srv = dict(zip(idx,val))
            retval.update(srv)
        self.subRank = Series(retval, name="SubRank")
        self.mmeDF = self.mmeDF.drop(["Hash"], axis=1)
        ts.stop()

        

    def mergeMetrics(self, **kwargs):
        assert isinstance(self.dbRankValues, DataFrame), "Must call getAlbumRanks()"
        assert isinstance(self.dbCountValues, DataFrame), "Must call getAlbumCounts()"
        assert isinstance(self.dbCounts, Series), "Must call getCounts()"
        assert isinstance(self.subRank, Series), "Must call getSubRank()"
        self.setData()
        self.verbose = kwargs.get('verbose', self.verbose)
        mdbs = MasterDBs()
        dbs  = mdbs.getDBs()
        mm   = MasterMetas()
        rankCounts = mm.getMediaRanks()

        ##### Order: ArtistName (Update), Ranks, Counts, DbIDs
        ts = Timestat("Merging Metrics", ind=2)
        mergedData = self.mmeDF[["ArtistName", "Update"]].copy(deep=True)
        mergedData = mergedData.join(self.subRank)
        #print(f"Merged Size {mergedData.shape}")
        for rank in rankCounts.keys():
            #print(f"  {rank} Size {self.dbRankValues[f'{rank}Rank'].shape}")
            #print(f"  {rank} Size {self.dbCountValues[f'{rank}Count'].shape}")
            rankData = Series(tuple(zip(self.dbRankValues[f'{rank}Rank'], self.dbCountValues[f'{rank}Count'])), index=mergedData.index, name=f'{rank}Rank')
            mergedData = mergedData.join(rankData)
        mergedData = mergedData.join(self.dbCounts)
        self.mmeDF = mergedData.join(self.mmeDF[dbs])
        ts.stop()
            

    def sortByPrimaryRank(self, **kwargs):
        self.setData()
        self.verbose = kwargs.get('verbose', self.verbose)
        ts = Timestat("Sorting By Primary Rank", ind=2)
        self.mmeDF["Order"] = self.mmeDF["PrimaryRank"].apply(lambda rank: rank[0])
        self.mmeDF = self.mmeDF.sort_values(by="Order").drop(["Order"], axis=1)
        ts.stop()

        
    def addMetrics(self, **kwargs):
        ts = Timestat("Adding Metrics To Pandb Data")
        self.setData()
        self.getAlbumCounts()
        #dbWeight,dbRankValues = 
        self.getAlbumRanks()
        #return dbWeight,dbRankValues
        self.getCounts()
        self.getSubRank()
        #return self.dbRankValues,self.dbCountValues
        self.mergeMetrics()
        self.sortByPrimaryRank()
        self.saveData()
        ts.stop()