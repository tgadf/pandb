""" Utility Map For Perminant Directories """

__all__ = ["MusicDBPermDir"]

from master import MasterParams
from fileutils import DirInfo
from sys import prefix

class MusicDBPermDir:
    def __init__(self, **kwargs):
        mkDirs        = kwargs.get('mkDirs', False)
        mp            = MasterParams()
        
        prefixPath    = DirInfo(prefix)
        if mkDirs: prefixPath.mkDir()
        
        projPath      = prefixPath.join(mp.getProjectName())
        if mkDirs: projPath.mkDir()

        libPath       = projPath.join("mdblib")
        if mkDirs: libPath.mkDir()

        dbPaths       = {db: libPath.join(db) for db in mp.getDBs()}
        for db,dbPath in dbPaths.items():
            if mkDirs: dbPath.mkDir()
            
        musicPath     = projPath.join(mp.getMusicDBName())
        if mkDirs: musicPath.mkDir()
            
        matchPath     = projPath.join("mdbmatch")
        if mkDirs: matchPath.mkDir()
            
        self.mp          = mp
        self.dbPaths     = dbPaths
        self.musicDBPath = musicPath
        self.matchPath   = matchPath
        
    def getDBPermPath(self, db):
        assert self.mp.isValid(db) == True, "Must pass valid DB to getDBPermPath"
        return self.dbPaths[db]
    
    def getMusicDBPermPath(self):
        return self.musicDBPath
    
    def getMatchPermPath(self):
        return self.matchPath