""" MusicDB Search Data Creater"""

__all__ = ["SearchData"]

from base import MusicDBBaseData
from timeutils import Timestat
from .base import SummaryDataBase

class SearchData(SummaryDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        
        
    def isSearchable(self, counts):
        retval = counts["Album"] >= 2 or counts["SingleEP"] >= 2
        return retval
    
    def makeSearchable(self, value):
        if isinstance(value,str):
            retval = value.upper()
        elif isinstance(value,list):
            retval = [val.upper() for val in value if isinstance(val,str)]
        elif value is None:
            retval = None
        else:
            raise ValueError("Can not make {0} uppercase".format(value))
        return retval
        
    ###########################################################################################################################################################
    # Artist ID => Name/URL Map
    ###########################################################################################################################################################
    def make(self):
        if self.verbose: ts = Timestat("Making {0} Search Data".format(self.db))
            
        self.medias = {"A": "Album", "B": "SingleEP", "C": "Appearance", "D": "Technical", "E": "Mix", "F": "Bootleg", "G": "AltMedia", "H": "Other"}

        artistCountsData  = self.mdbdata.getSummaryCountsData()        
        searchableResults = artistCountsData.apply(self.isSearchable, axis=1)
        
        for key in self.searchTypes:
            summaryData = eval("self.mdbdata.getSummary{0}Data".format(key))()
            searchData = summaryData.loc[searchableResults].apply(self.makeSearchable)
            searchData.name = key
            print("  ====> Saving [{0} / {1}] Searchable {2}  Data".format(len(searchData), len(summaryData), "ID => {0}".format(key)))
            eval("self.mdbdata.saveSearch{0}Data".format(key))(data=searchData)
        
        if self.verbose: ts.stop()