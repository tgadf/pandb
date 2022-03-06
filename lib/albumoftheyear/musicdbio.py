""" Primary Music DB I/O Classes """

__all__ = ["MusicDBIO"]

from base import MusicDBDir, MusicDBData, MusicDBIOBase
from .musicdbid import MusicDBID
from .parserawdata import ParseRawData
from .metadata import MetaData

class MusicDBIO(MusicDBIOBase):
    def __init__(self, **kwargs):
        super().__init__(db="AlbumOfTheYear", **kwargs)
        self.getdbid   = MusicDBID().get
        self.getModVal = self.mv.get
        self.prd       = ParseRawData(self.data, self.dir, **kwargs)
        self.meta      = MetaData(self.data, **kwargs)

        ############################################################
        # DB-specific Dir
        ############################################################
        
        ############################################################
        # DB-specific Data
        ############################################################
        self.data.addData("Raw", MusicDBData(path=self.dir.getMusicDBDir("RawModVal"), arg=True), fname=True)