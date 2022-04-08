""" Primary Music Database """

__all__ = ["MusicDB", "MusicDBIO"]

from base import MusicDBDir, MusicDBData
from master import MasterParams, MusicDBPermDir
from gate import MusicDBGate
from fileutils import DirInfo,FileInfo
from timeutils import Timestat
from ioutils import FileIO
from pandas import to_numeric, DataFrame, Series, concat
from uuid import uuid4

class MusicDB:
    def __init__(self, **kwargs):
        mdbpd = MusicDBPermDir()
        self.data = {}
        self.verbose  = kwargs.get('debug', kwargs.get('verbose', False))
        if self.verbose:
            print("MusicDB():")
            print("  ==> Music Dir: {0}".format(mdbpd.getMusicDBPermPath().str))
                                   
        ############ Add Names ############
        self.addData("", MusicDBData(path=MusicDBDir(mdbpd.getMusicDBPermPath()), fname="manualEntries"), fname=True)
            

    def addData(self, key, mdbDataIO, fname=False):
        exec("self.get{0}Data  = mdbDataIO.get".format(key))
        exec("self.save{0}Data = mdbDataIO.save".format(key))
        if fname:
            exec("self.get{0}Filename  = mdbDataIO.getFilename".format(key))
        if self.data.get(key) is None:
            self.data[key] = mdbDataIO
            
            

            
####################################################################################################################    
## Summary
####################################################################################################################    
class MusicDBIO:
    def __init__(self, **kwargs):
        self.mdb      = MusicDB(**kwargs)
        self.mmeDF    = None
        
        
    ####################################################################################################################    
    ## I/O
    ####################################################################################################################
    def setData(self):
        self.mmeDF = self.mdb.getData() if self.mmeDF is None else self.mmeDF
        
    def getData(self):
        self.setData()
        return self.mmeDF

    def saveData(self, mmeDF=None):
        mmeDF = self.mmeDF if mmeDF is None else mmeDF
        print("Saving Master DataFrame To {0}".format(self.mdb.getFilename().str))
        self.mdb.saveData(data=mmeDF)
        
    def isValid(self, db):
        if not isinstance(self.mmeDF,DataFrame):
            self.setData()
        return db in self.mmeDF.columns
        
        
    ####################################################################################################################    
    ## Summary
    ####################################################################################################################    
    def summary(self):
        dT = self.mmeDF.describe()
        dT = dT[dT.index.isin(["count", "unique"])]
        print(tabulate(dT, headers='keys', tablefmt='rst'))
        
        

    ####################################################################################################################    
    ## Append Metadata To DataFrame
    ####################################################################################################################
    def addMetrics(self):
        ts = Timestat("Adding Metrics To PanDB")
        gate     = MusicDBGate()
        mdbios   = gate.getIO()

        ######################################################################
        # Get PanDB
        ######################################################################
        self.setData()

        ######################################################################
        # Calculations
        ######################################################################
        dbAlbums = {db: mdbio.data.getSummaryNumAlbumsData() for db,mdbio in mdbios.items()}
        dfAlbums = DataFrame({db: to_numeric(dbIDMatches.apply(dbAlbums[db].get), errors='coerce') for db,dbIDMatches in self.mmeDF.items() if db in dbAlbums})
        dfRank   = DataFrame({db: dbIDs[dbIDs.notna()].rank(pct=True) for db,dbIDs in dfAlbums.items()}).fillna(0.0).mean(axis=1).rank(pct=True)
        dfRank.name = "Rank"

        ######################################################################
        # Join Rank
        ######################################################################
        if "Rank" in self.mmeDF.columns:
            self.mmeDF.drop(["Rank"], axis=1, inplace=True)
        self.mmeDF         = self.mmeDF.join(dfRank)
        self.mmeDF["Rank"] = self.mmeDF["Rank"].fillna(0.0).rank(method='max', ascending=False).apply(int)

        ######################################################################
        # Join Albums
        ######################################################################        
        if "Albums" in self.mmeDF.columns:
            self.mmeDF.drop(["Albums"], axis=1, inplace=True)
        dfAllAlbums = dfAlbums.sum(axis=1).fillna(0).astype(int)
        dfAllAlbums.name = "Albums"
        self.mmeDF           = self.mmeDF.join(dfAllAlbums)
        self.mmeDF["Albums"] = to_numeric(self.mmeDF["Albums"], errors='coerce').fillna(0)

        ######################################################################
        # Join Counts
        ######################################################################        
        if "Counts" in self.mmeDF.columns:
            self.mmeDF.drop(["Counts"], axis=1, inplace=True)
        self.mmeDF["Counts"] = self.mmeDF.drop(["Rank", "Albums", "ArtistName"], axis=1).count(axis=1)

        ts.stop()

        
    def addAlbums(self):
        ts = timestat("Adding DB Albums/Rank To MasterManualEntries")
        if "Rank" in self.mmeDF.columns:
            self.mmeDF.drop(["Rank"], axis=1, inplace=True)
        if "Albums" in self.mmeDF.columns:
            self.mmeDF.drop(["Albums"], axis=1, inplace=True)
            
        def getNumAlbums(dbID,db):
            raise ValueError("This doesn't work yet")
            return self.mdbData.getArtistDBNumAlbumsFromID(db,dbID)
            
        self.mdbData.loadArtists()
        mmeDFAlbums = {db: self.mmeDF[db].apply(getNumAlbums, db=db) for db in self.dbCols}
        mmeDFRank   = {db: dbIDs[dbIDs.notna()].rank(pct=True) for db,dbIDs in mmeDFAlbums.items()}
        rank = DataFrame(mmeDFRank).fillna(0.0).mean(axis=1).rank(pct=True)
        rank.name="Rank"
        self.mmeDF = self.mmeDF.join(rank)
        self.mmeDF["Rank"]   = self.mmeDF["Rank"].fillna(0.0).rank(method='max', ascending=False).apply(int)
        self.mmeDF["Albums"] = DataFrame(mmeDFAlbums).sum(axis=1).apply(int)
        ts.stop()
        
        
    def sortByRank(self):
        self.mmeDF = self.mmeDF.sort_values(by=["Rank"], ascending=True)        
        
    def sortByAlbums(self):
        self.mmeDF = self.mmeDF.sort_values(by=["Albums"], ascending=False)        
        
    def sortByCounts(self):
        self.mmeDF = self.mmeDF.sort_values(by=["Counts"], ascending=False)        
        
    def orderColumns(self):
        cols = Series(self.mmeDF.columns)
        mdbs = Series(self.mdbGate.getDBs())
        rank = Series(["Rank", "Albums", "Counts"])
        name = Series(["ArtistName"])
        missingDBCols = mdbs[~mdbs.isin(cols)]
        if len(missingDBCols) > 0:
            print("Could not find {0} in list of existing columns".format(list(missingDBCols)))
            return
        propOrder = rank.append(name).append(mdbs)
        remainingCols = cols[~cols.isin(propOrder)]
        if len(remainingCols) > 0:
            print("Found the following existing, but unknown columns".format(list(remainingCols)))
            return
        print("Using this order: {0}".format(list(propOrder)))                
        self.mmeDF = self.mmeDF[propOrder]
        
        

    ####################################################################################################################    
    ## Interal Helpers
    ####################################################################################################################
    def getIndexLookup(self, db):
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
        return self.mmeDF[self.mmeDF[db].notna()]
    
    def getNaDBIDs(self, db):
        return self.mmeDF[~self.mmeDF[db].notna()]
    
    def getMMEByID(self, db, dbID):
        return self.mmeDF[self.mmeDF[db] == str(dbID)]
    
    def getMMEByMBURL(self, url):
        mbID = self.getmbid(url)
        return self.getMMEByID('MusicBrainz', mbID)

    def getMMEByArtist(self, name, match="E"):
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
        return self.mmeDF.loc[idx]
    
    def setdbid(self, idx, db, dbID):
        if dbID is None:
            self.mmeDF.loc[idx, db] = None
            print("  ==> Set [{0}/{1}] to [None]".format(idx,db))
            return
        
        if db == "Spotify":
            self.mmeDF.loc[idx, db] = str(dbID)
            print("  ==> Set [{0}/{1}] to [{2}]".format(idx,db,dbID))
            return
                
        try:
            int(dbID)
        except:
            print("Could not set {0}/{1} ==> {2}".format(idx,db,dbID))
            return
        self.mmeDF.loc[idx, db] = str(dbID)
        print("  ==> Set [{0}/{1}] to [{2}]".format(idx,db,dbID))
    
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
        self.setdbid(idx, "MusicBrainz", self.getmbid(url))
    
    def setlfmurl(self, idx, url):
        self.setdbid(idx, "LastFMAPI", self.getmbid(url))
        self.setdbid(idx, "LastFM", self.getmbid(url))

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
        self.mmeDF.drop([idx], axis=0, inplace=True)
        print("  ==> Dropped Row [{0}]".format(idx))

    def newArtist(self, name, **kwargs):
        row = {"ArtistName": name}
        row.update({k: v for k,v in kwargs.items() if k in self.mmeDF.columns})
        if len(row) > 1:
            nRow       = Series(row)
            nRow.name  = str(uuid4())
            self.mmeDF = concat([self.mmeDF, nRow])
            print("  ==> Added New Row [{0}]".format(nRow))
        else:
            print("Need valid db")

    def mergeRows(self, idx1, idx2):
        for db,dbID in self.mmeDF.loc[idx2][self.mmeDF.loc[idx2].notna()].iteritems():
            if isna(self.mmeDF.loc[idx1,db]):
                self.setdbid(idx1, db, dbID)
        print("  ==> Merged [{0}] and [{1}]".format(idx1,idx2))
        self.dropRow(idx2)