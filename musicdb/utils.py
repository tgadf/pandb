""" Primary Music Database Utils """

__all__ = ["PanDBUtils"]

from gate import IDStore, IOStore
from pandas import to_numeric, Series, DataFrame
from .pandb import PanDB

####################################################################################################################    
## Summary
####################################################################################################################    
class PanDBUtils:
    def __init__(self, **kwargs):
        self.pdb      = PanDB()
        self.ids      = IDStore()
        self.verbose  = kwargs.get('verbose', True)
        self.mmeDF    = None
        
        
    def setData(self):
        self.mmeDF    = self.mmeDF if isinstance(self.mmeDF, DataFrame) else self.pdb.getData()
        
        

    ####################################################################################################################    
    ## Interal Helpers
    ####################################################################################################################
    def getIndexLookup(self, db):
        self.setData()
        dblookup = {}
        for idx,dbid in self.getNotNaDBIDs(db)[db].iteritems():
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
        return retval
    
    def getNotNaDBIDs(self, db):
        self.setData()
        return self.mmeDF[self.mmeDF[db].notna()]
    
    def getNaDBIDs(self, db):
        self.setData()
        return self.mmeDF[~self.mmeDF[db].notna()]
    
    def getMMEByID(self, db, dbID):
        self.setData()
        return self.mmeDF[self.mmeDF[db] == str(dbID)]
    
    def getMMEByMBURL(self, url):
        mbID = self.ids.getmbid(url)
        return self.getMMEByID('MusicBrainz', mbID)

    def getMMEByArtist(self, name, match="E"):
        self.setData()
        if isinstance(match, str):
            if match == "E":
                return self.mmeDF[self.mmeDF["ArtistName"] == name]
            elif match == "C":
                return self.mmeDF[self.mmeDF["ArtistName"].str.contains(name)]
            else:
                print("Did not understand match={0}".format(match))
                return None
        elif isinstance(match, int):
            idxs = self.mmeDF["ArtistName"].apply(lambda x: Levenshtein.ratio(x.upper(), name.upper())).sort_values(ascending=False).head(match).index
            return self.mmeDF.loc[idxs]
        else:
            return None
    
    
    
    ####################################################################################################################    
    ## Interal Setter Helpers
    ####################################################################################################################
    def getRows(self, idx):
        self.setData()
        return self.mmeDF.loc[idx]
    
    def setdbid(self, idx, db, dbID, verbose=True):
        self.setData()
        if dbID is None:
            self.mmeDF.loc[idx, db] = None
            if verbose: print("  ==> Set [{0}/{1}] to [None]".format(idx,db))
            return
        
        if db == "Spotify":
            self.mmeDF.loc[idx, db] = str(dbID)
            if verbose: print("  ==> Set [{0}/{1}] to [{2}]".format(idx,db,dbID))
            return
                
        try:
            int(dbID)
        except:
            if verbose: print("Could not set {0}/{1} ==> {2}".format(idx,db,dbID))
            return
        self.mmeDF.loc[idx, db] = str(dbID)
        if verbose: print("  ==> Set [{0}/{1}] to [{2}]".format(idx,db,dbID))
    
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
        print("  ==> Set [{0}] ArtistName To [{1}]".format(idx,name))

    def nulldbs(self, idx, dbs):
        for db in dbs:
            self.setdbid(idx, db, None)
        
    def nulllastfm(self, idx):
        self.setdbid(idx, "LastFM", None)

    def nulldeezer(self, idx):
        self.setdbid(idx, "Deezer", None)
        
    def dropRow(self, idx):
        self.setData()
        self.mmeDF.drop([idx], axis=0, inplace=True)
        print("  ==> Dropped Row [{0}]".format(idx))

    def newArtist(self, name, **dbids):
        self.setData()
        rowData = {**{"ArtistName": name}, **dbids}
        row = DataFrame(Series(rowData, name=str(uuid4()))).T
        self.mmeDF = concat([self.mmeDF,row], axis=0)
        print("  ==> Added New Row [{0}]".format(rowData))


    def mergeRows(self, idx1, idx2):
        self.setData()
        for db,dbID in self.mmeDF.loc[idx2][self.mmeDF.loc[idx2].notna()].iteritems():
            if db in ["Album", "Counts", "Rank", "ArtistName"]:
                continue
            if isna(self.mmeDF.loc[idx1,db]):
                self.setdbid(idx1, db, dbID)
        print("  ==> Merged [{0}] and [{1}]".format(idx1,idx2))
        self.dropRow(idx2)