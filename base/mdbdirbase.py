""" Base class for music db directories """

__all__ = ["MusicDBDir", "MusicDBBaseDirs"]

from master import MasterParams    
from fileutils import DirInfo
from inspect import ismethod, signature

##################################################################################################################
# Base MusicDB Dir Class
##################################################################################################################
class MusicDBDir:
    def __init__(self, path, arg=False, child=None):
        self.path = path
        if isinstance(path, DirInfo):
            self.meth = path.get
            self.methParams = len(signature(self.meth).parameters)
        elif isinstance(path, MusicDBDir):
            self.meth = path.get
            self.methParams = path.getNumArgs()
        else:
            raise ValueError("Must pass a DirInfo or MusicDBDir class object as path")
        self.arg   = arg
        self.child = child
        
    def getNumArgs(self):
        numargs = self.methParams + 1 if self.arg is True else self.methParams
        return numargs
        
    def getPathArgs(self, *args):
        pathArgs=None
        if self.arg is False:
            assert isinstance(args,tuple) and len(args) == self.getNumArgs(), "DirIO get() didn't get the number of expected path arguments [{0}]==>[{1}]".format(self.getNumArgs(), len(args))
            assert len(args) <= 1, "DirIO get() is not ready for more than two path arguments"
            pathArgs = {"Path": args, "Arg": None} if len(args) > 0 else {"Path": None, "Arg": None}
        elif self.arg is True:
            assert isinstance(args,tuple) and len(args) == self.getNumArgs(), "DirIO get() didn't get the number of expected path arguments [{0}]==>[{1}]".format(self.getNumArgs(), len(args))
            assert len(args) <= 2, "DirIO get() is not ready for more than two path arguments"
            pathArgs = {"Path": None, "Arg": args[0]} if len(args) == 1 else {"Path": args[0], "Child": args[1]}
        assert pathArgs is not None, "Somehow pathArgs is None in getPathArgs()"
        return pathArgs
    
    def getDirInfo(self, *args):
        pathArgs = self.getPathArgs(*args)
        
        dinfo = self.meth() if pathArgs["Path"] is None else self.meth(pathArgs["Path"])
        dinfo = dinfo.join(pathArgs["Arg"]) if self.arg is True else dinfo
        dinfo = dinfo.join(self.child) if self.child is not None else dinfo
        return dinfo
    
    def mkDir(self, *args):
        self.getDirInfo(*args).mkDir()
        
    def get(self, *args):
        return self.getDirInfo(*args)

        
##################################################################################################################
# Container For Music DB BaseDirs
##################################################################################################################
class MusicDBBaseDirs:
    def __init__(self, db, **kwargs):
        
        ############ Args ############
        debug  = kwargs.get('debug', False)
        debug  = kwargs.get('verbose', debug)
        mkDirs = kwargs.get('mkDirs', False)
        local  = kwargs.get('local', False)
        
        mp = MasterParams(debug=debug)
        if not mp.isValid(db):
            raise ValidError("DB [{0}] is not valid!".format(db))
        self.db = db
        
        dbname  = db.lower()
        rawPath = mp.getRawPath() if local is False else mp.getSumPath()
        modPath = mp.getModPath()
        sumPath = mp.getSumPath()
        self.dirs = {}
        
        ########################################
        # Base Directories (all dbs)
        ########################################
        self.dirs["Raw"]       = MusicDBDir(path=rawPath.join("artists-{0}".format(dbname)))
        self.dirs["ModVal"]    = MusicDBDir(path=modPath.join("artists-{0}-db".format(dbname)))
        self.dirs["Meta"]      = MusicDBDir(path=modPath.join(["artists-{0}-db".format(dbname), "metadata"]))
        self.dirs["Summary"]   = MusicDBDir(path=sumPath.join("db-{0}".format(dbname)))
        if mkDirs: _ = [mdbdir.mkDir() for key,mdbdir in self.dirs.items()]
        self.dirs["RawModVal"] = MusicDBDir(path=self.dirs["Raw"], arg=True)

        
        ########################################
        # Create Dynamic I/O Functions
        ########################################
        for key,mdbdir in self.dirs.items():
            self.addDir(key, mdbdir)

            
        ########################################
        # Make Dynamic Paths
        ########################################
        if mkDirs: _ = [self.getRawModValDataDir(modVal).mkDir() for modVal in mp.getModVals()]
            
        if debug:
            print("MusicDBBaseDirs(db={0})".format(db))
            if local:
                print("   Using Local Path For Raw Data <<====")
            print("   RawDataDir     = {0}".format(self.getRawDataDir().path))
            print("   ModValDataDir  = {0}".format(self.getModValDataDir().path))
            print("   MetaDataDir    = {0}".format(self.getMetaDataDir().path))
            print("   SummaryDataDir = {0}".format(self.getSummaryDataDir().path))
            
            
    def getMusicDBDir(self, key):
        if self.dirs.get(key):
            return self.dirs[key]
        raise ValueError("Did not find MusicDBDir key [{0}]".format(key))
        
    def addDir(self, key, path):
        assert isinstance(path, MusicDBDir), "MusicDBDirBase.addDir takes a key and a MusicDBDir object"
        exec("self.get{0}DataDir = path.get".format(key))
        if self.dirs.get(key) is None:
            self.dirs[key] = path