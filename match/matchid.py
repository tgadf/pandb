""" Master Match Categories """

__all__ = ["PanDBMatch"]

from musicdb import PanDBIO
from master import MusicDBPermDir
from ioutils import FileIO
from gate import IOStore
from pandas import DataFrame, Series, notna, isna, concat
from uuid import uuid4
from hashlib import md5
from match.matchdb import MatchDB

class PanDBMatch:
    def __init__(self, baseDB: str, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        if self.verbose: print("MatchID()")

        ###################################################################################################################
        # Access To Pan DB
        ###################################################################################################################        
        self.pdbio   = PanDBIO()
        self.pdbio.setData()
            
        self.baseDB  = baseDB
        self.qmap = {"Pure": 8, "Great": 7, "Good": 6, "Sole": 5, "Near": 4, "Loose": 3, "Low": 2, "Poor": 1}
        self.maxQual = max(self.qmap.values())+1
        self.compareDBs    = []
        self.hashmap = {}
        self.load()
        
        
    def getHashmap(self):
        return self.hashmap
        

    def load(self):
        io = FileIO()
        mdbpd = MusicDBPermDir()
        primaryMatchResults = io.get(mdbpd.getMatchPermPath().join("primaryMatch.p"))
        self.primaryMatchNames   = io.get(mdbpd.getMatchPermPath().join("primaryMatchNames.p"))
        crossMatchResults   = io.get(mdbpd.getMatchPermPath().join("crossMatch.p"))
        self.crossMatchNames     = io.get(mdbpd.getMatchPermPath().join("crossMatchNames.p"))        

        ################################################################################################################################################
        # Primary Match Results
        ################################################################################################################################################
        primaryMatchResults["CompareName"] = primaryMatchResults["Match"].apply(lambda x: x["Info"]["Name"])
        primaryMatchResults["QValue"] = primaryMatchResults["Quality"].map(self.qmap)
        primaryMatchResults = primaryMatchResults.join(primaryMatchResults["Match"].apply(lambda x: x["Rank"])).drop(["Match"], axis=1)
        primaryMatchResults = primaryMatchResults.rename(columns={"DB": "CompareDB"})

        primaryResults = Series({(baseid,comparedb): df.drop(["BaseID", "CompareDB"], axis=1) for (baseid,comparedb),df in primaryMatchResults.groupby(["BaseID","CompareDB"])})
        primaryMatches = primaryResults.apply(lambda df: df[df["QValue"] == df["QValue"].max()])


        
        ################################################################################################################################################
        # Cross Match Results
        ################################################################################################################################################
        crossMatchResults = crossMatchResults.rename(columns={"CompareID": "BaseIDCrossMatch"})
        crossMatchResults["QValue"] = crossMatchResults["Quality"].map(self.qmap)
        crossMatchResults = crossMatchResults.join(crossMatchResults["BaseID"].apply(Series)).drop(["BaseID"], axis=1)
        crossMatchResults = crossMatchResults.rename(columns={0: "BaseID", 1: "CompareDB", 2: "CompareID"})

        crossResults = Series({(compareid,comparedb): df.drop(["CompareID", "CompareDB"], axis=1) for (compareid,comparedb),df in crossMatchResults.groupby(["CompareID","CompareDB"])})
        crossMatches = crossResults.apply(lambda df: df[df["QValue"] == df["QValue"].max()])        
        
        

        ################################################################################################################################################
        # Segment By Num Matches
        ################################################################################################################################################
        numMatches = crossMatches.apply(lambda x: x.shape[0])
        matchGroup = {nM: crossMatches[numMatches == nM] for nM in numMatches.value_counts().index}
        

        
        ################################################################################################################################################
        # Single Matches w/ and w/o Multi Cross Matches
        ################################################################################################################################################
        singleResults = {}
        for (compareid,comparedb),df in matchGroup[1].iteritems():
            for _,row in df.iterrows():
                baseid = row["BaseID"]
                if baseid == row["BaseIDCrossMatch"]:
                    key = (baseid,comparedb)
                    if singleResults.get(key) is None:
                        singleResults[key] = {}
                    singleResults[key][compareid] = {"Quality": row["QValue"], "Rank": row["Match"]["Rank"], "Name": row["Match"]["Info"]["Name"]}


        singleFinalResults = {"Single": {}, "Multi": {}}
        for key,value in singleResults.items():
            maxValues = value
            if len(maxValues) > 1:
                valueMatches = Series(maxValues).apply(Series)
                maxValues = valueMatches[valueMatches["Quality"] == valueMatches["Quality"].max()].T.to_dict()
            if len(maxValues) > 1:
                singleFinalResults["Multi"][key] = value
            else:
                baseid,comparedb = key
                singleFinalResults["Single"].update({(baseid,comparedb,compareid): result for compareid,result in maxValues.items()})
        self.singleFinalResults = {"Single": Series(singleFinalResults["Single"]), "Multi": Series(singleFinalResults["Multi"]) }
        

        
        ################################################################################################################################################
        # Multi Matches
        ################################################################################################################################################       
        multiResults = {}
        for nM,mGroup in matchGroup.items():
            if nM == 1:
                continue
            for (compareid,comparedb),df in mGroup.iteritems():
                for _,row in df.iterrows():
                    baseid = row["BaseID"]
                    baseidcm = row["BaseIDCrossMatch"]
                    key = (baseid,comparedb,compareid,baseidcm)
                    if singleFinalResults["Single"].get(baseid) is not None:
                        multiResults[key] = {"Quality": row["QValue"], "Rank": row["Match"]["Rank"], "Name": row["Match"]["Info"]["Name"]}
        self.multiResults = Series(multiResults)       
        
        
        
    def select(self, minQual=3, maxQual=None, show=True):
        def getHash(baseid, comparedb, compareid):
            m = md5()
            m.update(baseid.encode())
            m.update(comparedb.encode())
            m.update(compareid.encode())
            return m.hexdigest()[:10]
        
        maxQual = maxQual if isinstance(maxQual, int) else self.maxQual
        
        matches = {}
        for baseid,baseresult in self.singleFinalResults["Single"].groupby(level=0):
            name = self.crossMatchNames[(self.baseDB, baseid)]
            first = True
            for (comparedb,compareid),compresult in baseresult.groupby(level=[1,2]):
                for key,result in compresult.iteritems():
                    qual  = result["Quality"]
                    if qual < minQual or qual >= maxQual:
                        continue
                    cname = self.crossMatchNames[(baseid,comparedb,compareid)]
                    idval = baseid if first is True else " "
                    first = False
                    hval  = getHash(baseid, comparedb, compareid)
                    assert self.hashmap.get(hval) is None, "OMG! Found a duplicate hash"
                    self.hashmap[hval] = (baseid,comparedb,compareid,key,qual,name,cname)
                    if show: print(f"{hval: <12} | {idval: <25}{comparedb: <15}{compareid: <40}{qual: <5}{name: <50}{cname: <50} | {hval}")
                    matches[key] = qual
                    for key,value in self.multiResults.get(baseid, {}).items():
                        qual  = value["Quality"]
                        if qual < minQual or qual >= maxQual:
                            continue
                        cname = self.crossMatchNames[(baseid,comparedb,compareid)]
                        hval  = getHash(baseid, comparedb, compareid)
                        assert self.hashmap.get(hval) is None, "OMG! Found a duplicate hash"
                        self.hashmap[hval] = (baseid,comparedb,compareid,key,qual,name,cname)
                        if show: print(f"{hval: <12} | {' ': <25}{comparedb: <15}{compareid: <40}{qual: <5}{name: <50}{cname: <50} | {hval}")

        if len(matches) > 0:
            self.uniqueIDs = Series(matches).index.unique(level=0)
            self.uniqueDBs = Series(matches).index.unique(level=1)                            
            print("  ==> Found [{0}] Matches For [{1}] [{2}] IDs From [{3}] DBs".format(len(matches), len(self.uniqueIDs), self.baseDB, len(self.uniqueDBs)))
            self.matches = Series(matches)
        else:
            self.uniqueIDs = []
            self.uniqueDBs = []
            print("  ==> Found [{0}] Matches For [{1}] [{2}] IDs From [{3}] DBs".format(len(matches), len(self.uniqueIDs), self.baseDB, len(self.uniqueDBs)))
            self.matches = None
            
        
    def include(self, vals: str, show: bool = True):
        hvals = [x.strip() for x in vals.split("\n") if len(x) > 0]
        matches = {}
        for hval in hvals:
            baseid,comparedb,compareid,key,qual,name,cname = self.hashmap[hval]
            if show: print(f"{hval: <12} | {baseid: <25}{comparedb: <15}{compareid: <40}{qual: <5}{name: <50}{cname: <50} | {hval}")
            matches[key] = qual

        if len(matches) > 0:
            self.uniqueIDs = Series(matches).index.unique(level=0)
            self.uniqueDBs = Series(matches).index.unique(level=1)                            
            print("  ==> Found [{0}] Matches For [{1}] [{2}] IDs From [{3}] DBs".format(len(matches), len(self.uniqueIDs), self.baseDB, len(self.uniqueDBs)))
            self.matches = Series(matches)
        else:
            self.uniqueIDs = []
            self.uniqueDBs = []
            print("  ==> Found [{0}] Matches For [{1}] [{2}] IDs From [{3}] DBs".format(len(matches), len(self.uniqueIDs), self.baseDB, len(self.uniqueDBs)))
            self.matches = None
        

    def master(self):
        if self.matches is None:
            return
        if self.verbose: print("  ==> Getting Master ID Lookup")
        lookup = {compareDB: self.pdbio.getIndexLookup(compareDB) for compareDB in self.uniqueDBs}
        ios    = IOStore()
        mdbio  = ios.get(self.baseDB)
        names  = mdbio.data.getSummaryNameData()        
        
        masterResultIndex = {}
        masterResultData  = {}
        for baseid,baseresult in self.matches.groupby(level=0):
            masterResultIndex[baseid] = {"ArtistName": names[baseid]}
            masterResultData[baseid] = {"ArtistName": names[baseid]}
            for (comparedb,compareid),compresult in baseresult.groupby(level=[1,2]):
                for key,result in compresult.iteritems():
                    masterResultIndex[baseid][comparedb] = lookup[comparedb].get(compareid)
                    masterResultData[baseid][comparedb]  = compareid
        masterResultIndex = Series(masterResultIndex).apply(Series)
        masterResultData  = Series(masterResultData).apply(Series).T
        self.masterResultData = masterResultData
        
        uniqueMasterIndex = masterResultIndex[self.uniqueDBs].apply(lambda row: Series([idx for idx in row.values if isinstance(idx,str)]).unique(), axis=1)
        self.knownMasterIndex  = uniqueMasterIndex[uniqueMasterIndex.apply(len) == 1].apply(lambda idx: idx[0])
        self.multiMasterIndex  = uniqueMasterIndex[uniqueMasterIndex.apply(len) > 1]
        self.newMasterIndex    = uniqueMasterIndex[uniqueMasterIndex.apply(len) == 0]      
        
        print("  ==> Found [{0}] Known Matches".format(len(self.knownMasterIndex)))
        print("  ==> Found [{0}] Multi Matches".format(len(self.multiMasterIndex)))
        print("  ==> Found [{0}] New Matches".format(len(self.newMasterIndex)))
        
        
    def merge(self, **kwargs):
        verbose  = kwargs.get('verbose', False)
        allownew = kwargs.get('allownew', True)
        if self.matches is None:
            print("No matches so no need to join with master...")
            return
        
        ###################################################################################################################
        # Existing Artists
        ###################################################################################################################        
        for baseid,masterid in self.knownMasterIndex.iteritems():
            masterData = self.masterResultData[baseid]
            name = masterData["ArtistName"]
            if verbose: print(baseid,'\t',name)
            dbids = {db: dbid for db,dbid in masterData.iteritems() if notna(dbid)}
            dbids[self.baseDB] = baseid
            for db,dbid in dbids.items():
                self.pdbio.setdbid(masterid, db, str(dbid), verbose=False)
        print("  ==> Added [{0}] Known Matches".format(len(self.knownMasterIndex)))
                
                
        ###################################################################################################################
        # New Artists
        ###################################################################################################################
        if allownew is True:
            mmeDF = self.pdbio.getData()
            newArtists = self.masterResultData[self.newMasterIndex.index].T
            if verbose:
                for baseid,name in newArtists["ArtistName"].iteritems():
                    print(baseid,'\t',name)        
            newArtists.index.name = self.baseDB
            newArtists = newArtists.reset_index()
            newArtists.index = [str(uuid4()) for i in newArtists.index]

            mmeDF = concat([mmeDF, newArtists], axis=0) if newArtists.shape[0] > 0 else mmeDF
            print("  ==> Added [{0}] New Matches".format(len(self.newMasterIndex)))            
            self.pdbio.saveData(mmeDF)
                
                
        ###################################################################################################################
        # Save Multi Master Data
        ###################################################################################################################        
        if allownew is True:
            io = FileIO()
            mdbpd = MusicDBPermDir()
            multiMasterIndexData = {baseid: {"Data": self.masterResultData[baseid], "Rows": self.pdbio.getRows(masteridxs)} for baseid,masteridxs in self.multiMasterIndex.iteritems()}
            savename = mdbpd.getMatchPermPath().join("multiMatch.p")
            io.save(idata = multiMasterIndexData, ifile = savename)
            print("  ==> Saving [{0}] Multi Match Artists' Data To {1}".format(len(multiMasterIndexData), savename.str))
            
        del self.pdbio