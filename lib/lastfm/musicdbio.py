""" Primary Music DB I/O Classes """

__all__ = ["MusicDBIO"]

from base import MusicDBDir, MusicDBData, MusicDBIOBase
from utils import poolParseIO
from .musicdbid import MusicDBID
from .parserawdata import ParseRawData
from .metadata import MetaData
from .searchomit import SearchOmit

class MusicDBIO(MusicDBIOBase):
    def __init__(self, **kwargs):
        super().__init__(db="LastFM", **kwargs)
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
        self.dir.addDir("RawAlbumModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal"), child="albums"))     
        self.dir.addDir("RawSearch", MusicDBDir(path=self.dir.getMusicDBDir("Raw"), child="search"))    
        self.dir.addDir("ModValArtist", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="artist"))
        if mkDirs: self.dir.getMusicDBDir("ModValArtist").mkDir()
        self.dir.addDir("ModValAlbum", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="album"))
        if mkDirs: self.dir.getMusicDBDir("ModValAlbum").mkDir()
        
        ############################################################
        # DB-specific Data
        ############################################################
        self.data.addData("SearchArtist", MusicDBData(path=self.dir.getMusicDBDir("RawSearch"), fname="spotifyArtistsData"))
        self.data.addData("RawArtistInfo", MusicDBData(path=self.dir.getMusicDBDir("RawModVal"), arg=True), fname=True)
        self.data.addData("RawArtistAlbum", MusicDBData(path=self.dir.getMusicDBDir("RawAlbumModVal"), arg=True), fname=True)
        self.data.addData("ModValArtist", MusicDBData(path=self.dir.getMusicDBDir("ModValArtist"), arg=True, suffix="DB"), fname=True)
        self.data.addData("ModValAlbum", MusicDBData(path=self.dir.getMusicDBDir("ModValAlbum"), arg=True, suffix="DB"), fname=True)