""" MusicDB Search Data Creater"""

__all__ = ["SearchData", "MusicDBIgnoreData"]

from base import MusicDBBaseData, MusicDBDir, MusicDBData
from master import MasterParams, MusicDBPermDir
from timeutils import Timestat
from fileutils import DirInfo,FileInfo
from ioutils import FileIO
from pandas import Series, DataFrame, notna
from .base import SummaryDataBase

class SearchData(SummaryDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        self.renames = {"&AMP;": "AND", "&": "AND"}
        

    def isSearchable(self, counts):
        reqMedia  = counts["Album"] >= 2 or counts["SingleEP"] >= 2
        reqName   = isinstance(counts["Name"],str) and len(counts["Name"]) > 0
        retval    = reqMedia and reqName
        return retval
    
    def renameSearchable(self, value):
        # &AMP; => &
        # & HIS => & AND HIS
        if isinstance(value,str):
            value = value.upper()
            for oldname,newname in self.renames.items():
                if oldname in value:
                    value = value.replace(oldname, newname)
        return value
    
    def makeSearchable(self, value):
        if isinstance(value,str):
            retval = self.renameSearchable(value)
        elif isinstance(value,list):
            retval = [self.renameSearchable(value) for val in value if isinstance(val,str)]
        elif value is None:
            retval = None
        else:
            raise ValueError("Can not make {0} uppercase".format(value))
        return retval
        
    ###########################################################################################################################################################
    # Artist ID => Name/URL Map
    ###########################################################################################################################################################
    def make(self, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)
        if verbose: ts = Timestat("Making {0} Search Data".format(self.db))
            
        self.medias = {"A": "Album", "B": "SingleEP", "C": "Appearance", "D": "Technical", "E": "Mix", "F": "Bootleg", "G": "AltMedia", "H": "Other"}
        
        omit = self.mdbdata.getOmitData()
        omit.loadIDs()
        if self.verbose: print("  ==> Found {0} IDs To Ignore".format(len(self.omit.omit)))

        artistCountsData  = self.mdbdata.getSummaryCountsData().join(self.mdbdata.getSummaryNameData())
        searchableResults = artistCountsData.apply(self.isSearchable, axis=1)
        
        for key in self.searchTypes:
            summaryData    = eval("self.mdbdata.getSummary{0}Data()".format(key))
            if isinstance(summaryData,(DataFrame,Series)):
                searchableData = summaryData.loc[searchableResults]
                searchData     = searchableData.apply(self.makeSearchable)
                searchData     = searchData[searchData.index.map(omit.isValid)]
                searchData.name = key
                if verbose: print("  ====> Saving [{0} / {1} / {2}] Searchable {3}  Data".format(len(searchData), len(searchableData), len(summaryData), "ID => {0}".format(key)))
                eval("self.mdbdata.saveSearch{0}Data".format(key))(data=searchData)
        
        if verbose: ts.stop()
            
            

class MusicDBIgnoreData:
    def __init__(self, **kwargs):
        mdbpd = MusicDBPermDir()
        self.data = {}
        self.verbose  = kwargs.get('debug', kwargs.get('verbose', False))
        if self.verbose:
            print("MusicDBIgnoreData():")
            print("  ==> Match Dir: {0}".format(mdbpd.getMetaPermPath().str))
                                   
        ############ Sort By DB ############
        self.mdbdata = MusicDBData(path=MusicDBDir(mdbpd.getMetaPermPath()), fname="manualIgnoreDBIDs", ext=".yaml")
        ignoreData = self.mdbdata.get()
        self.mp = MasterParams()
        ignoreDBIDData = {db: {} for db in self.mp.getDBs()}

        for gType,gData in ignoreData["General"].items():
            if self.verbose: print("    ==> {0}: {1}".format(gType,len(gData)))
            for db,dbID in gData.items():
                assert self.mp.isValid(db),"There is a non valid DB [{0}] in the ignore DBIDs file".format(db)
                ignoreDBIDData[db][dbID] = True
        for db,dbData in ignoreData["Specific"].items():
            assert self.mp.isValid(db),"There is a non valid DB [{0}] in the ignore DBIDs file".format(db)
            if self.verbose: print("    ==> {0}: {1}".format(db,len(dbData)))
            for artistName,dbID in dbData.items():
                if self.verbose: print("      ==> {0}: {1}".format(artistName,dbID))
                ignoreDBIDData[db][dbID] = True
        self.ignoreDBIDData = ignoreDBIDData
        
    def copyLocal(self):
        io = FileIO()
        localName = self.mdbdata.getFilename().name
        print("Saving a local copy of master data to [{0}] (Relative to calling notebook...)".format(localName))
        io.save(idata=self.mdbdata.get(), ifile=localName)
                
    def getData(self):
        return self.ignoreDBIDData
    
    def getDBData(self,db):
        assert self.mp.isValid(db),"Must provide a valid DB"
        return self.ignoreDBIDData.get(db, [])
    
    def isValid(self, db, dbID):
        dbData = self.getDBData(db)
        retval = dbData.get(dbID) == False
        return retval
        