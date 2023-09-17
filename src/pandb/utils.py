""" Primary Music Database Utils """

__all__ = ["PanDBUtils"]

from dbmaster import MasterDBs
from dbbase import MusicDBArtistName
#from musicdb import IDStore
from utils import Timestat, Now
from hashlib import md5
from pandas import Series, DataFrame, isna, concat
from uuid import uuid4
from .pandb import PanDB


###############################################################################
# Utility class for PanDB
###############################################################################
class PanDBUtils:
    def __init__(self, **kwargs):
        self.pdb = PanDB()
        #self.ids = IDStore()
        self.verbose = kwargs.get('verbose', True)
        self.mmeDF = None
        self.ts = Now()
        
    ###########################################################################
    # I/O functions for PanDB DataFrame object (pdb)
    ###########################################################################
    def setData(self, force=False):
        if force is False:
            self.mmeDF = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        else:
            self.mmeDF = self.pdb.getData()
        self.dbCols   = self.mmeDF.columns
                
    def saveData(self, mmeDF=None):
        mmeDF = mmeDF if isinstance(mmeDF, DataFrame) else self.mmeDF
        assert isisntance(mmeDF, DataFrame), f"Will not save mmeDF: [{type(mmeDF)}]"
        assert mmeDF["ArtistName"].isna().sum() == 0, f"Found None/NA In ArtistName Column"
        
        print(f"Saving PanDB DataFrame {self.getShape()} ... ", end="")
        self.pdb.saveData(data=mmeDF)
        print("Done")
                        
    def getData(self):
        self.setData()
        return self.mmeDF
    
    ###########################################################################
    # Info functions for PanDB DataFrame object (pdb)
    ###########################################################################
    def getShape(self):
        self.setData()
        return self.mmeDF.shape

    def getDBs(self):
        return MasterDBs().getDBs()
        
    def dbAssert(self, db):
        self.setData()
        assert db in self.mmeDF.columns, f"DB [{db}] is not known to PanDB"

    ###########################################################################
    ## Artist Helpers
    ###########################################################################
    def cleanArtistNames(self, saveit=True):
        self.setData()
        ts = Timestat("Cleaning Artist Names")
        manc = MusicDBArtistName()
        cleanNames = self.mmeDF["ArtistName"].map(manc.clean)
        numDiff = (cleanNames != self.mmeDF["ArtistName"]).sum()
        print(f"Found {numDiff} Names Needed To Be Cleaned")
        self.mmeDF["ArtistName"] = cleanNames
        if saveit is True:
            self.pdb.saveData(data=self.mmeDF)
        else:
            print("Not saving cleaned data.")
        ts.stop()
        
        
    ####################################################################################################################    
    ## Index
    ####################################################################################################################  
    def setIndex(self):
        self.setData()
        assert "SubRank" in self.mmeDF.columns, "Must call getSubRank() before setting index"
        self.cleanArtistNames(saveit=False)

        def getHash(name):
            m = md5()
            m.update(name.upper().encode())
            return m.hexdigest()[:12]

        ts = Timestat("Creating Artist Indices")
        self.mmeDF["Hash"] = self.mmeDF["ArtistName"].map(getHash)
        self.mmeDF["xx"] = 'xx'
        self.mmeDF.index = self.mmeDF["Hash"] + self.mmeDF['xx'] + self.mmeDF["SubRank"].apply(lambda sr: sr[0]).astype(str)
        self.mmeDF = self.mmeDF.drop(['Hash', 'xx'], axis=1)
        ts.stop()
        self.saveData()
        
        
    ####################################################################################################################    
    ## Timestamp
    ####################################################################################################################
    def updateTimestamp(self):
        return self.ts.get()
        

    ####################################################################################################################    
    ## Interal Helpers
    ####################################################################################################################
    def getIndexLookup(self, db):        
        self.setData()
        #if self.verbose: print(f"Getting PanDBID <=> {db} Lookup. ", end="")
        dblookup = {}
        notnaDBIDs = self.getNotNaDBIDs(db)[db]
        N = len(notnaDBIDs)
        for idx,dbid in notnaDBIDs.items():
            if isinstance(dbid,str):
                if dblookup.get(dbid) is None:
                    dblookup[dbid] = []
                dblookup[dbid].append(idx)
            elif isinstance(dbid,list):
                for val in dbid:
                    if dblookup.get(val) is None:
                        dblookup[val] = []
                    dblookup[val].append(idx)
        retval = Series(dblookup).apply(lambda dbid: dbid[0] if (isinstance(dbid,list) and len(dbid) == 1) else dbid)
        #if self.verbose: print(f"Found {len(retval)}/{N} IDs")
        return retval
    
    def getNotNaDBIDs(self, db):
        self.setData()
        self.dbAssert(db)
        return self.mmeDF[self.mmeDF[db].notna()]
    
    def getKnownDBIDs(self, db):
        self.setData()
        self.dbAssert(db)
        return self.mmeDF[self.mmeDF[db].notna()][db]

    def getNaDBIDs(self, db):
        self.setData()
        self.dbAssert(db)
        return self.mmeDF[~self.mmeDF[db].notna()]
    
    
    ####################################################################################################################    
    ## Manual Entries Helpers
    ####################################################################################################################
    def stripMME(self, df):
        assert isinstance(df,DataFrame), f"StripMME input [{type(df)}] is not a DataFrame"
        cnt    = df.count()
        retval = df[cnt[cnt > 0].index.to_list()]
        return retval

    def getMMEByID(self, db, dbID):
        self.setData()
        self.dbAssert(db)
        df = self.mmeDF[self.mmeDF[db] == str(dbID)]
        retval = self.stripMME(df)
        return retval
    
    def getMMEByMBURL(self, url):
        assert False, f"This doesn't work right now"
        mbID = self.ids.getmbid(url)
        df = self.getMMEByID('MusicBrainz', mbID)
        retval = self.stripMME(df)
        return retval

    def getMMEByArtist(self, name, match="E"):
        self.setData()
        if isinstance(name,list):
            if isinstance(match, str):
                if match == "E":
                    df = self.mmeDF[self.mmeDF["ArtistName"].isin(name)]
                    retval = self.stripMME(df)
                    return retval
                else:
                    print("Did not understand match={0}".format(match))
                    return None
            else:
                return None
        if isinstance(name,str):
            if isinstance(match, str):
                if match == "E":
                    df = self.mmeDF[self.mmeDF["ArtistName"] == name]
                    retval = self.stripMME(df)
                    return retval
                elif match == "C":
                    df = self.mmeDF[self.mmeDF["ArtistName"].str.contains(name)]
                    retval = self.stripMME(df)
                    return retval
                else:
                    print("Did not understand match={0}".format(match))
                    return None
            elif isinstance(match, int):
                idxs = self.mmeDF["ArtistName"].apply(lambda x: Levenshtein.ratio(x.upper(), name.upper())).sort_values(ascending=False).head(match).index
                df = self.mmeDF.loc[idxs]
                retval = self.stripMME(df)
                return retval
            else:
                return None

        
    ####################################################################################################################    
    ## Printers
    ####################################################################################################################    
    def printRow(self, idx):
        row = self.getRows(idx) if isinstance(idx,str) else None
        if isinstance(row,Series):
            df  = DataFrame(row[row.notna()]).T
            print(df.to_string())
    
    
    ####################################################################################################################    
    ## Interal Setter Helpers
    ####################################################################################################################    
    def isIndex(self, idx):
        if isinstance(idx,str):
            retval = idx in self.mmeDF.index
        elif hasattr(idx, '__iter__'):
            retval = all([idxVal in self.mmeDF.index for idxVal in idx])
        else:
            raise ValueError(f"Index Type [{type(idx)}] is not understood")
        return retval
            
    def getRows(self, idx, verbose=False):
        self.setData()
        if self.isIndex(idx):
            return self.mmeDF.loc[idx]
        else:
            if verbose: print(f"An index in ({idx}) is not a current Index")
            return None
        
    def getdbid(self, idx, db):
        self.setData()
        assert db in self.dbCols, f"DB [{db}] is not a column in PanDB"
        if self.isIndex(idx):
            return self.mmeDF.loc[idx,db]
        else:
            if verbose: print(f"An index in ({idx}) is not a current Index")
            return None
        
    
    def setdbid(self, idx, db, dbID, verbose=True, force=False):        
        self.setData()
        assert db in self.dbCols, f"DB [{db}] is not a column in PanDB"
        
        if dbID is None:
            self.mmeDF.loc[idx, db] = None
            self.mmeDF.loc[idx, "Update"] = self.updateTimestamp()
            if verbose: print(f"  ==> Set [{idx}/{db}] to [None]")
            return
        
        if db in ["Spotify", "SetListFM", "JioSaavn", "MusicBrainz", "Bandcamp", "YouTubeMusic", "Wikidata", "LastFM", "SpiritOfMetal"]:
            self.mmeDF.loc[idx, db] = str(dbID)
            self.mmeDF.loc[idx, "Update"] = self.updateTimestamp()
            if verbose: print(f"  ==> Set [{idx}/{db}] to [{dbID}]")
            return
        else:
            try:
                int(dbID)
            except:
                if force is False:
                    print(f"Could not set {idx}/{db} ==> {dbID} (not an integer)")
                    return
            self.mmeDF.loc[idx, db] = str(dbID)
            self.mmeDF.loc[idx, "Update"] = self.updateTimestamp()
            if verbose: print(f"  ==> Set [{idx}/{db}] to [{dbID}]")
    
    def setrymid(self, idx, dbID):
        if dbID is None:
            self.setdbid(idx, "RateYourMusic", dbID)
            return
        if dbID.startswith('[Artist') and dbID.endswith(']'):
            dbID = dbID[7:-1]
        elif dbID.startswith('Artist'):
            dbID = dbID[6:]    
        self.setdbid(idx, "RateYourMusic", dbID)
    
    def setgenid(self, idx, dbID):
        self.setdbid(idx, "Genius", dbID)
    
    def setdiscid(self, idx, dbID):
        self.setdbid(idx, "Discogs", dbID)
    
    def setamid(self, idx, dbID):
        if dbID is None:
            self.setdbid(idx, "AllMusic", dbID)
            return
        elif dbID.startswith('mn'):
            dbID = dbID[2:]    
        self.setdbid(idx, "AllMusic", dbID)
    
    def setmburl(self, idx, url):
        assert False, f"This doesn't work right now"
        self.setdbid(idx, "MusicBrainz", self.ids.getmbid(url))
    
    def setlfmurl(self, idx, url):
        self.setdbid(idx, "LastFM", self.ids.getmbid(url))

    def setspotid(self, idx, dbID):
        self.setdbid(idx, "Spotify", dbID)
        
    def setmbid(self, idx, dbID):
        self.setdbid(idx, "MusicBrainz", dbID)
    
    def setaotyid(self, idx, dbID):
        self.setdbid(idx, "AlbumOfTheYear", dbID)
    
    def setname(self, idx, name):
        self.mmeDF.loc[idx, "ArtistName"] = name
        self.mmeDF.loc[idx, "Update"] = self.updateTimestamp()
        print("  ==> Set [{0}] ArtistName To [{1}]".format(idx,name))
        
        
    ####################################################################################################################    
    ## New Artists
    ####################################################################################################################           
    def dropRows(self, idxs):
        self.setData()
        idxs = idxs if isinstance(idxs,list) else [idxs]
        assert isinstance(idxs,list), f"Idxs [{idxs}] is not a list"
        print(f"  ==> Dropping [{len(idxs)}] Rows ... ", end="")
        self.mmeDF.drop(idxs, axis=0, inplace=True)
        print("Done")

    def newArtist(self, name, **dbids):
        self.setData()
        rowData = {**{"ArtistName": name, "Update": self.updateTimestamp()}, **dbids}
        row = DataFrame(Series(rowData, name=str(uuid4()))).T
        self.mmeDF = concat([self.mmeDF,row], axis=0)
        print("  ==> Added New Row [{0}]".format(rowData))

    def addArtists(self, newPanDBData, **kwargs):
        assert isinstance(newPanDBData,DataFrame), f"NewPanDBData [{type(newPanDBData)}] is not a DataFrame"
        assert "ArtistName" in newPanDBData.columns, f"Could not find ArtistName in columns [{newPanDBData.columns}]"
        if newPanDBData.shape[0] > 0:
            self.setData()
            prevSize = self.mmeDF.shape[0]
            newPanDBData.index = [str(uuid4()) for i in newPanDBData.index]
            newPanDBData["Update"] = self.updateTimestamp()
            self.mmeDF = concat([self.mmeDF, newPanDBData], axis=0)
            assert self.mmeDF.shape[0] == prevSize + newPanDBData.shape[0], f"Error concating new artist Data [{self.mmeDF.shape[0]}] vs [{prevSize}] + [{newPanDBData.shape[0]}]"
            #for db,dbid in newPanDBData[self.getDBs()].items():
                
            if kwargs.get('verbose'): print("  ==> Added [{0}] New Matches".format(newPanDBData.shape[0]))

    def mergeTwoRows(self, idx1, idx2):
        """Merge Rows Idx1 & Idx2 ==> Idx1. Drop Idx2 when done."""
        assert all([isinstance(idx,str) for idx in [idx1,idx2]]), f"Indexes must be strings [{idx1}]/[{idx2}]"
        self.setData()
        if not all([idx in self.mmeDF.index for idx in [idx1,idx2]]):
            print(f"Indexes are not in PanDB [{idx1}]/[{idx2}]")
            return
        dbs = self.getDBs()
        nIDs = 0
        for db,dbID in self.mmeDF.loc[idx2][self.mmeDF.loc[idx2].notna()].items():
            if db not in dbs:
                continue
            if isna(self.mmeDF.loc[idx1,db]):
                self.setdbid(idx1, db, dbID, verbose=False, force=True)
                nIDs +=1
        print(f"  Merged [{idx2}] ==> [{idx1}] (NewIDs={nIDs})")
        self.dropRow(idx2)

    def mergeMultiRows(self, idxPairs, status=True):
        """Merge IdxPairs. Drop rows when done."""
        assert isinstance(idxPairs,list), f"IdxPairs must be a list"
        self.setData()
        dbs = self.getDBs()
        drops  = []
        N      = len(idxPairs)
        modVal = 100
        if N > 10000:
            modVal = 2500
        elif N > 2500:
            modVal = 500
        elif N > 1000:
            modVal = 250
        if status: ts = Timestat(f"Merging {len(idxPairs)} Rows")
        for n,(idx1,idx2) in enumerate(idxPairs):
            if status and (((n+1) % modVal == 0) or (n+1 == 100)): ts.update(n=n+1, N=N)
            if not all([idx in self.mmeDF.index for idx in [idx1,idx2]]):
                print(f"Indexes are not in PanDB [{idx1}]/[{idx2}]")
                continue
            nIDs = 0
            for db,dbID in self.mmeDF.loc[idx2][self.mmeDF.loc[idx2].notna()].items():
                if db not in dbs:
                    continue
                if isna(self.mmeDF.loc[idx1,db]):
                    self.setdbid(idx1, db, dbID, verbose=False, force=True)
                    nIDs +=1
            if not status: print(f"  Merged [{idx2}] ==> [{idx1}] (NewIDs={nIDs})")
            drops.append(idx2)
        if status: ts.stop()
        print(f"Dropping {len(drops)} Rows ... ", end="")
        self.mmeDF.drop(drops, axis=0, inplace=True)
        print("Done")