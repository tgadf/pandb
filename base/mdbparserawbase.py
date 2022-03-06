""" Base Class For Parsing Raw Data """

__all__ = ["ParseRawDataBase"]
         
from mdbbase import MusicDBBaseData, MusicDBBaseDirs
from mdbid import MusicDBIDModVal

class ParseRawDataBase:
    def __init__(self, mdbdata, mdbdir, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("ParseRawData(mdbdata) is not of type MusicDBBaseData")
        if not isinstance(mdbdir, MusicDBBaseDirs):
            raise ValueError("ParseRawData(mdbdir) is not of type MusicDBBaseDirs")
        self.mdbdata   = mdbdata
        self.mdbdir    = mdbdir
        self.verbose   = kwargs.get('debug', kwargs.get('verbose', False))
        self.mv        = MusicDBIDModVal()
        self.db        = mdbdir.db