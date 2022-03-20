""" Base class for music db data names """

__all__ = ["MusicDBData", "MusicDBBaseData"]

from master import MasterParams
from .mdbdirbase import MusicDBDir, MusicDBBaseDirs
from ioutils import FileIO
from numpy import ravel, ndarray

class MusicDBData:
    def __init__(self, path, arg=False, fname=None, prefix=None, suffix=None, ext=".p"):
        if not isinstance(path, MusicDBDir):
            raise TypeError("MusicDBData only takes MusicDBDir for a path")
        self.path   = path
        self.arg    = arg
        self.fname  = fname
        self.prefix = prefix
        self.suffix = suffix
        self.ext    = ext
        
        assert (self.arg and not isinstance(self.fname,str)) or (not self.arg and isinstance(self.fname,str)), "Must set arg or fname"
        
    def getNumArgs(self):
        numargs = self.path.getNumArgs() + 1 if self.arg is True else self.path.getNumArgs()
        return numargs       
        
    def getArgs(self, *args):
        numPathArgs = self.path.getNumArgs()
        args = ravel(args)
        if numPathArgs == 1 and self.arg is True:
            assert isinstance(args,(tuple,list,ndarray)) and len(args) == 2, "DataBaseIO filename needs a directory arg and a file arg"
            return {"Path": args[0], "Arg": args[1]}
        elif numPathArgs == 0 and self.arg is True:
            assert isinstance(args,(tuple,list,ndarray)) and len(args) == 1, "DataBaseIO filename needs a file arg"
            return {"Path": None, "Arg": args[0]}
        elif numPathArgs == 1 and self.arg is False:
            assert isinstance(args,(tuple,list,ndarray)) and len(args) == 1, "DataBaseIO filename needs a directory arg"
            return {"Path": args[0], "Arg": None}
        elif numPathArgs == 0 and self.arg is False:
            assert isinstance(args,(tuple,list,ndarray)) and len(args) == 0, "DataBaseIO filename does not need any arg values"        
            return {"Path": None, "Arg": None}
        
    def getFilename(self, *args):
        fileArgs = self.getArgs(*args)
        path   = self.path.get() if fileArgs["Path"] is None else self.path.get(fileArgs["Path"])
        path.mkDir()
        
        fname = None
        if self.arg is False:
            assert self.fname is not None, "Must set fname if there is no arg for DataBaseIO"
            fname = "{0}{1}".format(self.fname, self.ext)
        else:            
            if self.prefix is not None:
                fname = "{0}-{1}{2}".format(self.prefix, fileArgs["Arg"], self.ext)
            elif self.suffix is not None:
                fname = "{0}-{1}{2}".format(fileArgs["Arg"], self.suffix, self.ext)
            else:
                fname = "{0}{1}".format(fileArgs["Arg"], self.ext)

        assert isinstance(fname,str), "Somehow filename is not set..."
        return path.join(fname)
            
    def get(self, *args):
        return FileIO().get(self.getFilename(*args))

    def save(self, **kwargs):
        if kwargs.get('data') is None:
            raise ValueError("Must pass key=data & value=<the data> to save()")
        data = kwargs['data']        
        args = [v for k,v in kwargs.items() if k not in ['data']]
        FileIO().save(ifile=self.getFilename(ravel(args)), idata=data)

    
    
class MusicDBBaseData:
    def __init__(self, mdbdir, **kwargs):
        if not isinstance(mdbdir, MusicDBBaseDirs):
            raise TypeError("Must mass type MusicDBBaseDirs")
        self.mdbdir = mdbdir
        self.db = mdbdir.db
        self.data = {}
        self.debug  = kwargs.get('verbose', kwargs.get('debug', False))

        mp = MasterParams()
        
        modValDataDir  = mdbdir.getMusicDBDir("ModVal")
        metaDataDir    = mdbdir.getMusicDBDir("Meta")
        summaryDataDir = mdbdir.getMusicDBDir("Summary")
        
        #########################################################################################################
        # Data Classes
        #########################################################################################################
        
        ##### ModVal Data
        self.data["ModVal"]  = MusicDBData(path=modValDataDir, arg=True, suffix="DB")
        
        ##### Meta Data
        self.metas    = mp.getMetas()
        self.searches = mp.getSearches()
        self.medias   = mp.getMedias()
        for meta in self.metas.keys():
            self.data["Meta{0}".format(meta)]  = MusicDBData(path=metaDataDir, arg=True, suffix=meta)
        
        ##### Summary
        for meta,summaryKeys in self.metas.items():
            for key in summaryKeys:
                fname = "Summary{0}".format(key)
                self.data[fname] = MusicDBData(path=summaryDataDir, fname=fname)
                
        ##### Search
        for key in self.searches:
            fname = "Search{0}".format(key)
            self.data[fname] = MusicDBData(path=summaryDataDir, fname=fname)

        
        #########################################################################################################
        # Create Dynamic I/O Functions
        #########################################################################################################
        for key,mdbDataIO in self.data.items():
            addFilename = True if (key in ["ModVal"] or key.startswith("Meta")) else False
            self.addData(key, mdbDataIO, addFilename)    
            

    def addData(self, key, mdbDataIO, fname=False):
        exec("self.get{0}Data  = mdbDataIO.get".format(key))
        exec("self.save{0}Data = mdbDataIO.save".format(key))
        if fname:
            exec("self.get{0}Filename  = mdbDataIO.getFilename".format(key))
        if self.data.get(key) is None:
            self.data[key] = mdbDataIO