""" Primary Music DB I/O Classes """

__all__ = ["MusicDBIO"]

from base import MusicDBDir, MusicDBData, MusicDBIOBase
from .musicdbid import MusicDBID
from .parserawdata import ParseRawData
from .metadata import MetaData
from .searchomit import SearchOmit

class MusicDBIO(MusicDBIOBase):
    def __init__(self, **kwargs):
        super().__init__(db="Deezer", **kwargs)
        self.getdbid   = MusicDBID().get
        self.getModVal = self.mv.get
        self.prd       = ParseRawData(self.data, self.dir, **kwargs)
        self.meta      = MetaData(self.data, **kwargs)
        mkDirs         = kwargs.get('mkDirs', False)

        ############################################################
        # Omit Data
        ############################################################
        self.data.addData("Omit", SearchOmit())

        ############################################################
        # DB-specific Dir
        ############################################################
        self.dir.addDir("RawSearch", MusicDBDir(path=self.dir.getMusicDBDir("Raw"), child="search"))
        
        dType = "Artist"
        self.dir.addDir(f"Raw{dType}ModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal"), child=f"{dType.lower()}s"))
        self.dir.addDir(f"ModVal{dType}", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child=f"{dType.lower()}"))
        if mkDirs: self.dir.getMusicDBDir(f"ModVal{dType}").mkDir()
        
        dType = "Album"
        self.dir.addDir(f"Raw{dType}ModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal"), child=f"{dType.lower()}s"))
        self.dir.addDir(f"ModVal{dType}", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child=f"{dType.lower()}"))
        if mkDirs: self.dir.getMusicDBDir(f"ModVal{dType}").mkDir()
        
        dType = "Track"
        self.dir.addDir(f"Raw{dType}ModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal"), child=f"{dType.lower()}s"))
        self.dir.addDir(f"ModVal{dType}", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child=f"{dType.lower()}"))
        if mkDirs: self.dir.getMusicDBDir(f"ModVal{dType}").mkDir()
        
        ############################################################
        # DB-specific Data
        ############################################################
        self.data.addData("RelatedArtists", MusicDBData(path=self.dir.getMusicDBDir("RawSearch"), fname="deezerRelatedArtistsData"))
        self.data.addData("ArtistsInfo", MusicDBData(path=self.dir.getMusicDBDir("RawSearch"), fname="deezerArtistsInfo"))
        
        dType = 'Artist'
        self.data.addData(f"RawArtist", MusicDBData(path=self.dir.getMusicDBDir(f"Raw{dType}ModVal"), arg=True), fname=True)
        self.data.addData(f"ModVal{dType}", MusicDBData(path=self.dir.getMusicDBDir(f"ModVal{dType}"), arg=True, suffix="DB"), fname=True)
        
        dType = 'Album'
        self.data.addData(f"RawArtist{dType}", MusicDBData(path=self.dir.getMusicDBDir(f"Raw{dType}ModVal"), arg=True), fname=True)
        self.data.addData(f"ModVal{dType}", MusicDBData(path=self.dir.getMusicDBDir(f"ModVal{dType}"), arg=True, suffix="DB"), fname=True)
        
        dType = 'Track'
        self.data.addData(f"RawArtist{dType}", MusicDBData(path=self.dir.getMusicDBDir(f"Raw{dType}ModVal"), arg=True), fname=True)
        self.data.addData(f"ModVal{dType}", MusicDBData(path=self.dir.getMusicDBDir(f"ModVal{dType}"), arg=True, suffix="DB"), fname=True)