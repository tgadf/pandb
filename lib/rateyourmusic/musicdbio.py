""" Primary Music DB I/O Classes """

__all__ = ["MusicDBIO"]

from base import MusicDBDir, MusicDBData, MusicDBIOBase
from .musicdbid import MusicDBID
from .parserawdata import ParseRawData
from .metadata import MetaData
from .searchomit import SearchOmit

class MusicDBIO(MusicDBIOBase):
    def __init__(self, **kwargs):
        super().__init__(db="RateYourMusic", **kwargs)
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
        
        ############################################################
        # DB-specific Data
        ############################################################
        self.data.addData("Raw", MusicDBData(path=self.dir.getMusicDBDir("RawModVal"), arg=True), fname=True)
        
        self.dir.addDir("SearchMedia", MusicDBDir(path=self.dir.getMusicDBDir("Summary"), child="searchmedia"))    
        if mkDirs: self.dir.getMusicDBDir("SearchMedia").mkDir()

        self.data.addData("SearchMedia", MusicDBData(path=self.dir.getMusicDBDir("SearchMedia"), arg=True, prefix="SearchMedia"), fname=True)