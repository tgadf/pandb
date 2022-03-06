""" Music DB I/O Bases Class """

__all__ = ["MusicDBIOBase"]

from dbid import MusicDBIDModVal
from utils import MusicDBArtistName
from .mdbdirbase import MusicDBBaseDirs
from .mdbdatabase import MusicDBBaseData
from meta import SummaryData

    
##################################################################################################################
# Base I/O Class
##################################################################################################################
class MusicDBIOBase:
    def __init__(self, db, **kwargs):
        self.db        = db
        self.dir       = MusicDBBaseDirs(db, **kwargs)
        self.data      = MusicDBBaseData(self.dir, **kwargs)
        self.manc      = MusicDBArtistName()
        self.mv        = MusicDBIDModVal()
        self.sum       = SummaryData(self.data, **kwargs)        
