""" Genre MetaData Classes """

__all__ = ["GenreMetaData"]

from .base import MetaDataBase
from mdbid import MusicDBIDModVal
from timeutils import Timestat
from pandas import Series, DataFrame

class GenreMetaData(MetaDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        
    def make(self, modVal=None):
        if self.verbose: ts = Timestat("Making Basic {0} MetaData".format(self.db))
        modVals = [modVal] if isinstance(modVal,(str,int)) else list(range(MusicDBIDModVal().maxModVal))
