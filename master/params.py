""" Master Param Data """

__all__ = ["MasterParams", "MasterDBs", "MasterMetas", "MasterPaths", "MasterBasic"]

from fileutils import DirInfo

##################################################################################################################################
# Master List of Databases
##################################################################################################################################
class MasterDBs:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.dbs = ["Discogs", "Spotify", "LastFM", "Genius", "RateYourMusic", "MetalArchives", "Deezer", "AllMusic", "MusicBrainz", "AlbumOfTheYear", "SetListFM", "Beatport", "Traxsource", "MyMixTapez"]
        self.valid = {db: True for db in self.dbs}
        if verbose is True:
            print("MasterDBs()")
            print("{0: <18}{1}".format("  ==> DBs:", self.dbs))
        
    def isValid(self, db):
        return self.valid.get(db, False)
    
    def getDBs(self):
        return self.dbs

        
##################################################################################################################################
# Master List of Metas
##################################################################################################################################
class MasterMetas:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.medias = {"A": "Album", "B": "SingleEP", "C": "Appearance", "D": "Technical", "E": "Mix", "F": "Bootleg", "G": "AltMedia", "H": "Other"}
        self.mediaAlbums = [self.medias['A'],self.medias['B']]
        self.metas  = {"Basic": ["Name", "Ref", "NumAlbums"], "Media": ["{0}Media".format(media) for media in self.medias.values()],
                       "Genre": ["Genre"], "Bio": ["Bio"], "Link": ["Link"], "Metric": ["Metric"], "Counts": ["Counts"]}
        self.searches = ["Name"] + ["{0}Media".format(media) for media in ["Album", "SingleEP", "Appearance", "Technical", "Mix", "Bootleg", "AltMedia", "Other"]]
        if verbose is True:
            print("MasterMetas()")
            print("{0: <18}{1}".format("  ==> Media:", list(self.medias.values())))
            print("{0: <18}{1}".format("  ==> Metas:", list(self.metas.keys())))
            print("{0: <18}{1}".format("  ==> Searches:", list(self.searches)))
        
    def getMedias(self):
        return self.medias
    
    def getMetas(self):
        return self.metas
    
    def getSearches(self):
        return self.searches

        
##################################################################################################################################
# Master List of Paths
##################################################################################################################################
class MasterPaths:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        mkDirs = kwargs.get('mkDirs', False)
        rawPathDrive  = DirInfo("/Volumes/Piggy")
        modPathDrive  = DirInfo("/Volumes/Seagate")
        sumPathDrive  = DirInfo("/Users/tgadfort/Music")
        
        if not sumPathDrive.exists():
            raise ValueError("Sum Drive [{0}] Does Not Exist.".format(sumPathDrive.str))
                        
        if mkDirs:
            if not rawPathDrive.exists():
                print("Raw Drive [{0}] Does Not Exist. Setting To [{1}]".format(rawPathDrive.str, sumPathDrive.str))
                rawPathDrive = sumPathDrive
        
            if not modPathDrive.exists():
                print("Mod Drive [{0}] Does Not Exist. Setting To [{1}]".format(rawPathDrive.str, sumPathDrive.str))
                modPathDrive = sumPathDrive
        
        self.rawPath  = rawPathDrive.join("Discog")
        self.modPath  = modPathDrive.join("Discog")
        self.sumPath  = sumPathDrive.join("Discog")
        if verbose is True:
            print("MasterPaths()")
            print("{0: <18}{1}".format("  ==> Raw:", self.rawPath.str))
            print("{0: <18}{1}".format("  ==> Mod:", self.modPath.str))
            print("{0: <18}{1}".format("  ==> Sum:", self.sumPath.str))
    
    def getRawPath(self):
        return self.rawPath

    def getModPath(self):
        return self.modPath

    def getSumPath(self):
        return self.sumPath

        
##################################################################################################################################
# Master Basic Info
##################################################################################################################################
class MasterBasic:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.maxModValue = 100
        self.projectName = "pandb"
        self.musicdbName = "musicdb"  
        if verbose is True:
            print("MasterBasic()")
            print("{0: <18}{1}".format("  ==> ModVals:", self.maxModValue))
            print("{0: <18}{1}".format("  ==> Project:", self.getProjectName()))
            print("{0: <18}{1}".format("  ==> MusicDB:", self.getMusicDBName())) 

    def getMaxModVal(self):
        return self.maxModValue
    
    def getModVals(self, listIt=False):
        retval = range(self.getMaxModVal())
        retval = list(retval) if listIt is True else retval
        return retval

    def getProjectName(self):
        return self.projectName

    def getMusicDBName(self):
        return self.musicdbName
        

##################################################################################################################################
# Master List of Params
##################################################################################################################################
class MasterParams():
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        for mCls in [MasterBasic(**kwargs),MasterPaths(**kwargs), MasterMetas(**kwargs), MasterDBs(**kwargs)]:
            for method in [attribute for attribute in dir(mCls) if callable(getattr(mCls, attribute)) and attribute.startswith('__') is False]:
                exec("self.{0} = mCls.{0}".format(method))