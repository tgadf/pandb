""" Primary Music DB I/O Classes """

__all__ = ["MusicDBIO"]

from base import MusicDBDir, MusicDBData, MusicDBIOBase
from .musicdbid import MusicDBID
from .parserawdata import ParseRawData
from .metadata import MetaData

class MusicDBIO(MusicDBIOBase):
    def __init__(self, **kwargs):
        super().__init__(db="AllMusic", **kwargs)
        self.getdbid   = MusicDBID().get
        self.getModVal = self.mv.get
        self.prd       = ParseRawData(self.data, self.dir, **kwargs)
        self.meta      = MetaData(self.data, **kwargs)


        ############################################################
        # DB-specific Dir
        ############################################################
        self.dir.addDir("RawSearch", MusicDBDir(path=self.dir.getMusicDBDir("Raw"), child="search"))    
        
        self.dir.addDir("RawArtistModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal")))
        self.dir.addDir("ModValArtist", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="artist"))
        self.dir.getMusicDBDir("ModValArtist").mkDir()
        
        self.dir.addDir("RawSongModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal"), child="song"))
        self.dir.addDir("ModValSong", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="song"))
        self.dir.getMusicDBDir("ModValSong").mkDir()
        
        self.dir.addDir("RawCreditModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal"), child="credit"))
        self.dir.addDir("ModValCredit", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="credit"))
        self.dir.getMusicDBDir("ModValCredit").mkDir()
        
        self.dir.addDir("RawCompositionModVal", MusicDBDir(path=self.dir.getMusicDBDir("RawModVal"), child="composition"))
        self.dir.addDir("ModValComposition", MusicDBDir(path=self.dir.getMusicDBDir("ModVal"), child="composition"))
        self.dir.getMusicDBDir("ModValComposition").mkDir()
        
        
        ############################################################
        # DB-specific Data
        ############################################################
        self.data.addData("SearchArtist", MusicDBData(path=self.dir.getMusicDBDir("RawSearch"), fname="allmusicArtistsData"))
        
        self.data.addData("RawArtist", MusicDBData(path=self.dir.getMusicDBDir("RawArtistModVal"), arg=True), fname=True)
        self.data.addData("RawArtistSong", MusicDBData(path=self.dir.getMusicDBDir("RawSongModVal"), arg=True), fname=True)
        self.data.addData("RawArtistCredit", MusicDBData(path=self.dir.getMusicDBDir("RawCreditModVal"), arg=True), fname=True)
        self.data.addData("RawArtistComposition", MusicDBData(path=self.dir.getMusicDBDir("RawCompositionModVal"), arg=True), fname=True)
                        
        self.data.addData("ModValArtist", MusicDBData(path=self.dir.getMusicDBDir("ModValArtist"), arg=True, suffix="DB"), fname=True)
        self.data.addData("ModValSong", MusicDBData(path=self.dir.getMusicDBDir("ModValSong"), arg=True, suffix="DB"), fname=True)
        self.data.addData("ModValCredit", MusicDBData(path=self.dir.getMusicDBDir("ModValCredit"), arg=True, suffix="DB"), fname=True)
        self.data.addData("ModValComposition", MusicDBData(path=self.dir.getMusicDBDir("ModValComposition"), arg=True, suffix="DB"), fname=True)