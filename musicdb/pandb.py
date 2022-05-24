""" Primary Music Database """

__all__ = ["PanDB"]

from base import MusicDBDir, MusicDBData
from master import MusicDBPermDir, MasterDBs

class PanDB:
    def __init__(self, **kwargs):
        mdbpd = MusicDBPermDir()
        self.data = {}
        self.verbose  = kwargs.get('debug', kwargs.get('verbose', False))
        if self.verbose:
            print("MusicDB():")
            print("  ==> Music Dir: {0}".format(mdbpd.getMusicDBPermPath().str))
                                   
        ############ Add Names ############
        self.addData("", MusicDBData(path=MusicDBDir(mdbpd.getMusicDBPermPath()), fname="manualEntries"), fname=True)
        if False:
            self.mdbs = MasterDBs()
            for db in self.mdbs.getDBs():
                self.addData(db, MusicDBData(path=MusicDBDir(mdbpd.getMusicDBPermPath()), fname=db), fname=False)
            

    def addData(self, key, mdbDataIO, fname=False):
        exec("self.get{0}Data  = mdbDataIO.get".format(key))
        exec("self.save{0}Data = mdbDataIO.save".format(key))
        if fname:
            exec("self.get{0}Filename  = mdbDataIO.getFilename".format(key))
        if self.data.get(key) is None:
            self.data[key] = mdbDataIO