""" Master Match Categories """

__all__ = ["MatchID"]

from musicdb import MusicDBIO
from pandas import DataFrame, Series, notna, isna
from .matchdb import MatchDB

class MatchID:
    def __init__(self, baseDB: str, mdb: MatchDB, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        if self.verbose: print("MatchID()")
            
        self.baseDB  = baseDB
        self.mdb     = mdb
        self.pdbio   = MusicDBIO()
        self.pdbio.setData()
        self.dbMatchResult = None
        self.compareDBs    = []
        
        
    def join(self):
        dbMatches = []
        self.compareDBs = []

        for compareDB,compareDBResult in self.mdb.results.items():
            compareDBIDs = compareDBResult["CompareID"]
            compareDBIDs.name = compareDB
            self.compareDBs.append(compareDB)
            dbMatches.append(compareDBIDs)

        if len(dbMatches) > 0:
            dbMatchResult = DataFrame(dbMatches[0])
            for dbMatch in dbMatches[1:]:
                dbMatchResult = dbMatchResult.join(dbMatch, how='outer')
            self.dbMatchResult = dbMatchResult
            
            if self.verbose: print("  ==> Found {0} x {1} Matched Entries/DBs".format(self.dbMatchResult.shape[0], self.dbMatchResult.shape[1]))
        else:
            if self.verbose: print("  ==> No matches found...")
                

    def getIdx(self, row):
        indices = []
        for idx,val in row.iteritems():
            if isinstance(val,str):
                indices.append(val)
        idx = list(set(indices))    

        if len(idx) == 1:
            retval = idx[0] if isinstance(idx[0],str) else None
            return retval
        elif len(idx) == 0:
            return None
        else:
            vals = [val for val in idx if notna(val)]
            retval = vals[0] if len(vals) == 1 else vals
            return retval
        
        
    def getMasterID(self):
        if not isinstance(self.dbMatchResult, DataFrame):
            print("  ==> No matched results. Not matching to master DB")
            return
        
        if self.verbose: print("  ==> Getting Master ID Lookup")
        lookup = {compareDB: self.pdbio.getIndexLookup(compareDB) for compareDB in self.compareDBs}
        
        if self.verbose: print("  ==> Mapping Master ID Lookup From Matchd DB IDs")
        compareLookup = DataFrame({compareDB: compareDBID.map(lookup[compareDB]) for compareDB,compareDBID in self.dbMatchResult.iteritems()})
        
        self.masterIDXLookup = compareLookup.apply(self.getIdx, axis=1)
        self.masterIDXLookup.name = "MasterID"
        
        
    def joinMaster(self):
        if not isinstance(self.dbMatchResult, DataFrame):
            print("  ==> No matched results. Not matching to master DB")
            return
        if not isinstance(self.masterIDXLookup, Series):
            print("  ==> No masterID lookup results. Not matching to master DB")
            return
        
        self.matchedResults = self.dbMatchResult.join(self.masterIDXLookup).join(self.mdb.baseIO.mdbio.data.getSummaryNameData())
        self.matchedResults.index.name = self.baseDB
        self.matchedResults = self.matchedResults.reset_index()
        

        #################################################################################
        ## Group Matches
        #################################################################################
        isKnown  = self.matchedResults["MasterID"].notna()
        numMatch = self.matchedResults.drop(["MasterID", "Name"], axis=1).count(axis=1)
        
        toMerge  = {}
        toMerge["KnownMatch"] = self.matchedResults[isKnown].copy(deep=True)
        toMerge["GoodMatch"]  = self.matchedResults[(~isKnown) & (numMatch>=3)].copy(deep=True)
        toMerge["LooseMatch"] = self.matchedResults[(~isKnown) & (numMatch==2)].copy(deep=True)
        if "Deezer" in toMerge["LooseMatch"].columns and "LastFM" in toMerge["LooseMatch"].columns:
            toMerge["OkMatch"] = toMerge["LooseMatch"][(self.matchedResults["LastFM"].isna()) & (self.matchedResults["Deezer"].isna())]
        elif "Deezer" in toMerge["LooseMatch"].columns:
            toMerge["OkMatch"] = toMerge["LooseMatch"][(self.matchedResults["Deezer"].isna())]
        elif "LastFM" in toMerge["LooseMatch"].columns:
            toMerge["OkMatch"] = toMerge["LooseMatch"][(self.matchedResults["LastFM"].isna())]
        else:
            toMerge["OkMatch"] = toMerge["LooseMatch"]
            
        if self.verbose: print("  ==> Merging {0}/{1} Entries With PanDB".format(toMerge["KnownMatch"].shape[0], self.matchedResults.shape[0]))        
        if self.verbose: print("  ==> Adding {0}/{1} New Good Entries To PanDB".format(toMerge["GoodMatch"].shape[0], self.matchedResults.shape[0]))
        if self.verbose: print("  ==> Adding {0}/{1} New OK Entries To PanDB".format(toMerge["OkMatch"].shape[0], self.matchedResults.shape[0]))
        if self.verbose: print("  ==> Adding {0}/{1} New Loose Entries To PanDB (Maybe)".format(toMerge["LooseMatch"].shape[0], self.matchedResults.shape[0]))
        self.toMerge = toMerge
        
        
    def mergeMaster(self):
        if not isinstance(self.toMerge, dict):
            print("  ==> No matched results. Not matching to master DB")
            return
        
        ###################################################################################################################
        # Existing Artists
        ###################################################################################################################        
        for idx,row in self.toMerge["KnownMatch"].iterrows():
            midx = row["MasterID"]
            if notna(row[self.baseDB]):
                self.pdbio.setdbid(midx, self.baseDB, str(row[self.baseDB]))
            for compareDB,compareDBID in row[row.notna()].iteritems():
                if compareDB in self.compareDBs:
                    self.pdbio.setdbid(midx, compareDB, str(row[compareDB]))
                    
                    
        ###################################################################################################################
        # New Artists With 3+ Matches
        ###################################################################################################################
        for idx,row in self.toMerge["GoodMatch"].iterrows():
            name  = str(row["Name"])
            dbids = {db: dbid for db,dbid in row.drop(["Name", "MasterID"]).to_dict().items() if notna(dbid)}
            self.pdbio.newArtist(name=name, **dbids)
            
            
        ###################################################################################################################
        # New Artists With 2+ Matches
        ###################################################################################################################
        for idx,row in self.toMerge["OkMatch"].iterrows():
            name  = str(row["Name"])
            dbids = {db: dbid for db,dbid in row.drop(["Name", "MasterID"]).to_dict().items() if notna(dbid)}
            self.pdbio.newArtist(name=name, **dbids)
            
        self.pdbio.saveData()