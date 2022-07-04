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
        self.dir.addDir("RawArtistModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal")))
        self.dir.addDir("ModValArtist", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="artist"))
        if mkDirs: self.dir.getMusicDBDir("ModValArtist").mkDir()
        
        ############################################################
        # DB-specific Data
        ############################################################
        self.data.addData("RelatedArtists", MusicDBData(path=self.dir.getMusicDBDir("RawSearch"), fname="deezerRelatedArtistsData"))
        self.data.addData("ArtistsInfo", MusicDBData(path=self.dir.getMusicDBDir("RawSearch"), fname="deezerArtistsInfo"))
        self.data.addData("ModValArtist", MusicDBData(path=self.dir.getMusicDBDir("ModValArtist"), arg=True, suffix="DB"), fname=True)
