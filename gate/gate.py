""" MusicDB Gate """

__all__ = ["MusicDBGate"]

from importlib import import_module
from master import MasterDBs
from pandas import DataFrame

#########################################################################################################################################################
# Music DB Gate
#########################################################################################################################################################
class MusicDBGate:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        mdbs = MasterDBs()
        self.getDBs  = mdbs.getDBs
        self.modVals = mdbs.getModVals(listIt=True)
        self.mdbios = {db: getattr(import_module('lib.{0}'.format(db.lower())), "MusicDBIO") for db in mdbs.getDBs()}
        
        if self.verbose:
            print("MusicDBGate()")
            print("  ==> DBs: {0}".format(self.dbs))
        

    #####################################################################################################################################
    # Music DB I/O
    #####################################################################################################################################
    def getIO(self, db=None):
        if db is None:
            return {db: mdbio() for db,mdbio in self.mdbios.items()}
        assert self.mp.isValid(db) == True,"Must give a valid db, not [{0}]".format(db)
        return self.mdbios.get(db)()
        

    #####################################################################################################################################
    # Meta Data
    #####################################################################################################################################
    def parseRawData(self, db=None, modVal=None):
        for db in self.getDBs(db):
            assert self.mp.isValid(db) == True,"Must give a valid db, not [{0}]".format(db)
            cmd = "self.mdbios[db].prd.parse()" if modVal is None else "self.mdbios[db].prd.parse(modVal)"
            print("  ==> {0}".format(cmd))
            exec(cmd)
        

    #####################################################################################################################################
    # Meta Data
    #####################################################################################################################################
    def makeMetaData(self, db=None, modVal=None):
        for db in self.getDBs(db):
            assert self.mp.isValid(db) == True,"Must give a valid db, not [{0}]".format(db)
            cmd = "self.mdbios[db].meta.make()" if modVal is None else "self.mdbios[db].meta.make(modVal)"
            print("  ==> {0}".format(cmd))
            exec(cmd)
        
        
    #####################################################################################################################################
    # Summary Data
    #####################################################################################################################################
    def makeSummaryData(self, db=None):
        for db in self.getDBs(db):
            assert self.mp.isValid(db) == True,"Must give a valid db, not [{0}]".format(db)
            cmd = "self.mdbios[db].sum.make()"
            print("  ==> {0}".format(cmd))
            exec(cmd)
        
                
    def loadArtists(self):
        self.artistNames = {db: DataFrame(mdbio.data.getArtistIDToNameData()).join(mdbio.data.getArtistIDToNumAlbumsData()) for db,mdbio in self.mdbios.items()}