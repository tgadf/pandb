""" Primary Music DB I/O Classes """

__all__ = ["MusicDBIO"]

from base import MusicDBDir, MusicDBData, MusicDBIOBase
from .musicdbid import MusicDBID
from .parserawdata import ParseRawData
from .metadata import MetaData

class MusicDBIO(MusicDBIOBase):
    def __init__(self, **kwargs):
        super().__init__(db="MusicBrainz", **kwargs)
        self.getdbid   = MusicDBID().get
        self.getModVal = self.mv.get
        self.prd       = ParseRawData(self.data, self.dir, **kwargs)
        self.meta      = MetaData(self.data, **kwargs)


        ############################################################
        # DB-specific Dir
        ############################################################
        self.dir.addDir("RawSearch", MusicDBDir(path=self.dir.getMusicDBDir("Raw"), child="search"))
        for fileType in ["Artist", "Album", "Recording", "Work"]:
            key    = "ModVal{0}".format(fileType)
            mdbdir = MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="{0}".format(fileType.lower()))
            self.dir.addDir(key, mdbdir)
            mdbdir.mkDir()
            #self.dir.getMusicDBDir("ModVal{0}".format(fileType)).mkDir()
        
        ############################################################
        # DB-specific Data
        ############################################################
        for fileType in ["Artist", "ArtistURL", "ArtistRecording", "ArtistWork", "ArtistReleaseGroup"]:
            key     = "Search{0}".format(fileType)
            mdbdir  = self.dir.getMusicDBDir("RawSearch")
            mdbdata = MusicDBData(path=mdbdir, fname="{0}DataFrame".format(fileType))
            self.data.addData(key, mdbdata, fname=True)
            
        for fileType in ["Artist", "Album", "Recording", "Work"]:
            key     = "ModVal{0}".format(fileType)
            mdbdir  = self.dir.getMusicDBDir("ModVal{0}".format(fileType))
            mdbdata = MusicDBData(path=mdbdir, arg=True, suffix="DB")
            self.data.addData(key, mdbdata, fname=True)
