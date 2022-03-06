""" I/O For Ignored DB IDs (e.g, Various Artists, Traditional, etc... """

__all__ = ["MusicDBIgnoreData"]

from base import MusicDBDir, MusicDBData
from master import MasterParams
from utils import MusicDBPermDir
from fileutils import DirInfo,FileInfo
from ioutils import FileIO

class MusicDBIgnoreData:
    def __init__(self, **kwargs):
        mdbpd = MusicDBPermDir()
        self.data = {}
        self.verbose  = kwargs.get('debug', kwargs.get('verbose', False))
        if self.verbose:
            print("MusicDBIgnoreData():")
            print("  ==> Match Dir: {0}".format(mdbpd.getMatchPermPath().str))
                                   
        ############ Sort By DB ############
        self.mdbdata = MusicDBData(path=MusicDBDir(mdbpd.getMatchPermPath()), fname="manualIgnoreDBIDs", ext=".yaml")
        ignoreData = self.mdbdata.get()
        self.mp = MasterParams()
        ignoreDBIDData = {db: {} for db in self.mp.getDBs()}

        for gType,gData in ignoreData["General"].items():
            for db,dbID in gData.items():
                assert self.mp.isValid(db),"There is a non valid DB [{0}] in the ignore DBIDs file".format(db)
                ignoreDBIDData[db][dbID] = True
        for db,dbData in ignoreData["Specific"].items():
            assert self.mp.isValid(db),"There is a non valid DB [{0}] in the ignore DBIDs file".format(db)
            for artistName,dbID in dbData.items():
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
        