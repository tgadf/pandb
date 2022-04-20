""" PanDB Gates """

__all__ = ["IOStore", "IDStore"]

from importlib import import_module
from master import MasterDBs

#########################################################################################################################################################
# Music DB I/O Gate
#########################################################################################################################################################
class IOStore:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        mdbs = MasterDBs()        
        self.ios = {db: getattr(import_module('lib.{0}'.format(db.lower())), "MusicDBIO") for db in mdbs.getDBs()}
        for db,mdbio in self.ios.items():
            exec("self.get{0}IO = mdbio".format(db))
        
    def get(self, db=None, **kwargs):
        self.verbose = kwargs.get('verbose', self.verbose)
        retval = self.ios[db](verbose=self.verbose) if db is not None else {db: mdbio(verbose=self.verbose) for db,mdbio in self.ios.items()}
        return retval


#########################################################################################################################################################
# Music DB ID Gate
#########################################################################################################################################################
class IDStore:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        mdbs = MasterDBs()
        ios = {db: getattr(import_module('lib.{0}'.format(db.lower())), "MusicDBID") for db in mdbs.getDBs()}
        self.ios = {db: dbid() for db,dbid in ios.items()}
        for db,dbid in self.ios.items():
            exec("self.get{0}ID = dbid.get".format(db))
            exec("self.get{0}id = dbid.get".format(dbid.short))
        
    def get(self, db, s):
        return self.ios[db].get(s)