""" Master Param Data """

__all__ = ["MasterParams"]

from fileutils import DirInfo

class MasterParams:
    def __init__(self, **kwargs):
        debug  = kwargs.get('debug', kwargs.get('verbose', False))
        mkDirs = kwargs.get('mkDirs', False)
        
        
        ################################################################################################
        # Metadatas
        ################################################################################################
        self.medias = {"A": "Album", "B": "SingleEP", "C": "Appearance", "D": "Technical", "E": "Mix", "F": "Bootleg", "G": "AltMedia", "H": "Other"}
        self.metas  = {"Basic": ["Name", "Ref", "NumAlbums"], "Media": ["{0}Media".format(media) for media in self.medias.values()],
                       "Genre": ["Genre"], "Bio": ["Bio"], "Link": ["Link"], "Metric": ["Metric"], "Counts": ["Counts"]}
        self.searches = ["Name"] + ["{0}Media".format(media) for media in ["Album", "SingleEP"]]
        
        
        ################################################################################################
        # Master List of Databases
        ################################################################################################
        self.dbs = ["Discogs", "Spotify", "LastFM", "Genius", "RateYourMusic", "MetalArchives",
                    "Deezer", "AllMusic", "MusicBrainz", "AlbumOfTheYear", "SetListFM"]
        self.valid = {db: True for db in self.dbs}
        
        
        ################################################################################################
        # Master List of Paths
        ################################################################################################
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
        
        
        ################################################################################################
        # Max Mod Values
        ################################################################################################        
        self.maxModValue = 100
        
        
        ################################################################################################
        # Project Values
        ################################################################################################        
        self.projectName = "pandb"
        self.musicdbName = "musicdb"
        
        
        if debug:
            print("MasterParams()")
            print("  ==> DBs:       {0}".format(self.getDBs()))
            print("  ==> Raw Path:  {0}".format(self.getRawPath().str))
            print("  ==> Mod Path:  {0}".format(self.getModPath().str))
            print("  ==> Sum Path:  {0}".format(self.getSumPath().str))
            print("  ==> MaxModVal: {0}".format(self.maxModValue))
            print("  ==> Project:   {0}".format(self.getProjectName()))
            print("  ==> MusicDB:   {0}".format(self.getMusicDBName()))

        
    ################################################################################################
    # Master I/O
    ################################################################################################
    def getMedias(self):
        return self.medias
    
    def getMetas(self):
        return self.metas
    
    def getSearches(self):
        return self.searches
    
    def getMaxModVal(self):
        return self.maxModValue
    
    def getModVals(self, listIt=False):
        retval = range(self.getMaxModVal())
        retval = list(retval) if listIt is True else retval
        return retval
    
    def isValid(self, db):
        return self.valid.get(db, False)
    
    def getDBs(self):
        return self.dbs

    def getRawPath(self):
        return self.rawPath

    def getModPath(self):
        return self.modPath

    def getSumPath(self):
        return self.sumPath

    def getProjectName(self):
        return self.projectName

    def getMusicDBName(self):
        return self.musicdbName