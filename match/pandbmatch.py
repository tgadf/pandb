""" Master Match Categories """

__all__ = ["PanDBMatch"]

from musicdb import PanDBIO
from master import MusicDBPermDir, MasterDBs
from ioutils import FileIO
from listUtils import getFlatList
from gate import IOStore
from pandas import DataFrame, Series, notna, isna, concat
from uuid import uuid4
from hashlib import md5
from .matchdb import MatchDB
from .utils import printIntro
from .matchlev import getLevenshtein
from numpy import unique


class PanDBMatch:
    def __init__(self, baseDB: str, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        printIntro("PanDBMatch")

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
        primaryMatchResults["QValue"] = primaryMatchResults["MediaQuality"].map(self.qmap)
        #primaryMatchResults = primaryMatchResults.join(primaryMatchResults["Match"].apply(lambda x: x["Rank"])).drop(["Match"], axis=1)
        primaryMatchResults = primaryMatchResults.rename(columns={"DB": "CompareDB"})

        primaryResults = Series({(baseid,comparedb): df.drop(["BaseID", "CompareDB"], axis=1) for (baseid,comparedb),df in primaryMatchResults.groupby(["BaseID","CompareDB"])})
        primaryMatches = primaryResults.apply(lambda df: df[df["QValue"] == df["QValue"].max()])


        
        ################################################################################################################################################
        # Cross Match Results
        ################################################################################################################################################
        crossMatchResults = crossMatchResults.rename(columns={"CompareID": "BaseIDCrossMatch"})
        crossMatchResults["QValue"] = crossMatchResults["MediaQuality"].map(self.qmap)
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
                    #singleResults[key][compareid] = {"MediaQuality": row["QValue"], "Rank": row["Match"]["Rank"], "Name": row["Match"]["Info"]["Name"]}
                    singleResults[key][compareid] = {"MediaQuality": row["QValue"], "Name": row["Match"]["Info"]["Name"]}


        singleFinalResults = {"Single": {}, "Multi": {}}
        for key,value in singleResults.items():
            maxValues = value
            if len(maxValues) > 1:
                valueMatches = Series(maxValues).apply(Series)
                maxValues = valueMatches[valueMatches["MediaQuality"] == valueMatches["MediaQuality"].max()].T.to_dict()
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
                        #multiResults[key] = {"MediaQuality": row["QValue"], "Rank": row["Match"]["Rank"], "Name": row["Match"]["Info"]["Name"]}
                        multiResults[key] = {"MediaQuality": row["QValue"], "Name": row["Match"]["Info"]["Name"]}
        self.multiResults = Series(multiResults)       
        
        
    def printSelect(self, hval, basedb, baseid, comparedb, compareid, first, nameQuality, mediaQuality, name, cname):
        print(f"{hval: <12}", end=" | ")
        if basedb == "MusicBrainz":
            pval = f"{baseid: <42}" if first is True else f"{' ': <42}"
        elif basedb == "Spotify":
            pval = f"{baseid: <30}" if first is True else f"{' ': <30}"
        else:
            pval = f"{baseid: <20}" if first is True else f"{' ': <20}"
        print(f"{pval}", end="")
        print(f"{comparedb: <15}", end="")
        if comparedb == "MusicBrainz":
            pval = f"{compareid: <42}"
        elif comparedb == "Spotify":
            pval = f"{compareid: <30}"
        else:
            pval = f"{compareid: <20}"
        print(f"{pval}", end="")
        print(f"{nameQuality: <4}", end="")
        print(f"{mediaQuality: <4}", end=" | ")
        print(f"{name: <40}", end="")
        print(f"{cname}")
        
        
    def getNameQuality(self, baseName, compName):
        similarity = getLevenshtein(baseName, compName)
        self.baseName = baseName
        self.compName = compName
        quality = None
        if similarity >= 1.0:
            quality = "Pure"
            return 5
        elif similarity >= 0.95:
            quality = "Great"
            return 4
        elif similarity >= 0.9:
            quality = "Good"
            return 3
        elif similarity >= 0.85:
            quality = "Near"
            return 2
        else:
            quality = "Low"
            return 1
        return quality
                    

    def select(self, minQual=3, maxQual=None, minName=1, show=True):
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
                    mediaQuality = result["MediaQuality"]
                    qual  = mediaQuality
                    if qual < minQual or qual >= maxQual:
                        continue
                    cname = self.crossMatchNames[(baseid,comparedb,compareid)]
                    nameQuality  = result.get("NameQuality", self.getNameQuality(name, cname))
                    if (qual == 1 and nameQuality < 4) or (nameQuality < minName):
                        continue
                    idval = baseid if first is True else " "
                    first = False
                    hval  = getHash(baseid, comparedb, compareid)
                    assert self.hashmap.get(hval) is None, "OMG! Found a duplicate hash"
                    self.hashmap[hval] = (baseid,comparedb,compareid,key,qual,name,cname)
                    #if show: print(f"{hval: <12} | {idval: <25}{comparedb: <15}{compareid: <40}{qual: <5}{name: <50}{cname: <50} | {hval}")
                    if show: self.printSelect(hval, self.baseDB, baseid, comparedb, compareid, first, nameQuality, mediaQuality, name, cname)
                    matches[key] = qual
                    for key,value in self.multiResults.get(baseid, {}).items():
                        mediaQuality = value["MediaQuality"]
                        qual  = mediaQuality
                        if qual < minQual or qual >= maxQual:
                            continue
                        cname = self.crossMatchNames[(baseid,comparedb,compareid)]
                        nameQuality  = value.get("NameQuality", self.getNameQuality(name, cname))
                        if (qual == 1 and nameQuality < 4) or (nameQuality < minName):
                            continue
                        hval  = getHash(baseid, comparedb, compareid)
                        assert self.hashmap.get(hval) is None, "OMG! Found a duplicate hash"
                        self.hashmap[hval] = (baseid,comparedb,compareid,key,qual,name,cname)
                        #if show: print(f"{hval: <12} | {' ': <25}{comparedb: <15}{compareid: <40}{qual: <5}{name: <50}{cname: <50} | {hval}")
                        if show: self.printSelect(hval, self.baseDB, baseid, comparedb, compareid, first, nameQuality, mediaQuality, name, cname)

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
        hvals = [x.split()[0].strip() for x in vals.split("\n") if len(x) > 0]
        matches = {}
        for hval in hvals:
            baseid,comparedb,compareid,key,mediaQuality,name,cname = self.hashmap[hval]
            nameQuality  = self.getNameQuality(name, cname)
            if show: self.printSelect(hval, self.baseDB, baseid, comparedb, compareid, True, nameQuality, mediaQuality, name, cname)
            #if show: print(f"{hval: <12} | {baseid: <25}{comparedb: <15}{compareid: <40}{qual: <5}{name: <50}{cname: <50} | {hval}")
            matches[key] = mediaQuality

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
        
        
    def getLookupValue(self, db, dbid):
        rows = self.pdbio.getMMEByID(db, dbid)
        idxs = rows.index
        if len(idxs) == 0:
            retval = None
        elif len(idxs) == 1:
            retval = list(idxs)[0]
        else:
            retval = list(idxs)
        return retval
        
    def pandbLookupSingle(self):
        if self.matches is None:
            return
        if self.verbose: print("  ==> Getting Master ID Lookup")
        #lookup = {compareDB: self.pdbio.getIndexLookup(compareDB) for compareDB in self.uniqueDBs}
        #lookup.update({self.baseDB: self.pdbio.getIndexLookup(self.baseDB)})
        ios    = IOStore()
        mdbio  = ios.get(self.baseDB)
        names  = mdbio.data.getSummaryNameData()
        
        masterResultIndex = {}
        masterResultData  = {}
        for baseid,baseresult in self.matches.groupby(level=0):
            masterResultIndex[baseid] = {"ArtistName": names[baseid]}
            masterResultData[baseid]  = {"ArtistName": names[baseid]}
            lookupValues = set()
            #baseLookupValue = lookup[self.baseDB].get(baseid)
            baseLookupValue = self.getLookupValue(self.baseDB, baseid)
            for (comparedb,compareid),compresult in baseresult.groupby(level=[1,2]):
                for key,result in compresult.iteritems():
                    #compLookupValue = lookup[comparedb].get(compareid)
                    compLookupValue = self.getLookupValue(comparedb, compareid)
                    if baseLookupValue is not None:
                        if isinstance(baseLookupValue,list):
                            for value in baseLookupValue:
                                lookupValues.add(value)
                        else:
                            lookupValues.add(baseLookupValue)
                    if compLookupValue is not None:
                        if isinstance(compLookupValue,list):
                            for value in compLookupValue:
                                lookupValues.add(value)
                        else:
                            lookupValues.add(compLookupValue)
                    if len(lookupValues) > 1:
                        pass
                        #multiIndexCntr += 1
                    #print(baseid,'->',baseLookupValue,'|','\t',comparedb,compareid,'->',compLookupValue,'| ==>',lookupValue)
                    lookupValue = list(lookupValues)
                    masterResultIndex[baseid][comparedb] = lookupValue
                    masterResultData[baseid][comparedb]  = compareid
        masterResultIndex   = Series(masterResultIndex).apply(Series)
        masterResultIndices = masterResultIndex.drop(['ArtistName'], axis=1).apply(lambda row: getFlatList([dbids for db, dbids in row.iteritems() if isinstance(dbids,list)]), axis=1)
        self.masterResultIndex = masterResultIndices
        self.knownMasterIndex  = masterResultIndices[masterResultIndices.apply(len) == 1].apply(lambda idx: idx[0])
        self.multiMasterIndex  = masterResultIndices[masterResultIndices.apply(len) > 1]
        self.newMasterIndex    = masterResultIndices[masterResultIndices.apply(len) == 0]      
        
        self.masterResultData  = Series(masterResultData).apply(Series).T
        
        print("  ==> Found [{0}] Known Matches".format(len(self.knownMasterIndex)))
        print("  ==> Found [{0}] Multi Matches".format(len(self.multiMasterIndex)))
        print("  ==> Found [{0}] New Matches".format(len(self.newMasterIndex)))

        
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
        else:           
            self.pdbio.saveData()
            
                
                
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
                    
        
    def mergeMultiRows(self):
        dbs = MasterDBs().getDBs()
        if len(self.multiMasterIndex) > 0:
            nMerged = 0
            for baseid,pandbIndices in self.multiMasterIndex.iteritems():
                if len(pandbIndices) != 2:
                    print("More than 2 indices")
                    continue
                print("")
                print("-"*150)
                pandbData = self.pdbio.getRows(pandbIndices)
                try:
                    nUniqueMax = pandbData[dbs].apply(lambda x: x.nunique()).max()
                except:
                    print("Could not merge rows due to error in nunique()")
                    continue
                if nUniqueMax == 1:
                    print(pandbData.drop(dbs, axis=1).to_string())
                    self.pdbio.mergeRows(*(pandbData.index))
                    nMerged += 1
                else:
                    print("Could not merge rows due to multiple IDs for a db")
                print('-'*150)

            if nMerged > 0:
                self.pdbio.saveData()
        else:
            print("Nothing to merge")