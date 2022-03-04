""" MusicDB Summary Data Creater"""

__all__ = ["MusicDBSummaryData"]

from mdbbase import MusicDBBaseData
from mdbmaster import MasterParams
from mdbutils import MusicDBArtistName
from mdbid import MusicDBIDModVal
from timeutils import Timestat
from pandas import Series

class MusicDBSummaryData:
    def __init__(self, mdbdata, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("MusicDBSummaryData(mdbdata) is not of type MusicDBBaseData")
        self.mdbdata = mdbdata
        self.db      = mdbdata.db
        self.verbose = kwargs.get('debug', kwargs.get('verbose', False))
        self.manc    = MusicDBArtistName()
        self.modVals = MasterParams().getModVals(listIt=True)
        
        
    ########################################################################################################################
    # Artist ID => Name/URL Map
    ########################################################################################################################
    def make(self):
        if self.verbose: ts = Timestat("Making Basic {0} Summary Data".format(self.db))
        
        artistIDToName      = Series(dtype = 'object')
        artistIDToRef       = Series(dtype = 'object')
        artistIDToNumAlbums = Series(dtype = 'object')
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaBasicData(modVal)
            
            artistIDToName = artistIDToName.append(modValMetaData["ArtistName"].apply(self.manc.clean))
            artistIDToRef  = artistIDToRef.append(modValMetaData["URL"])
            artistIDToNumAlbums  = artistIDToNumAlbums.append(modValMetaData["NumAlbums"])
                
        print("  ====> Saving [{0}] {1} Basic Summary Data".format(len(artistIDToName), "ID => Name"))
        artistIDToName.name = "Name"
        artistIDToName.index.name = "ArtistID"        
        self.mdbdata.saveArtistIDToNameData(data=artistIDToName)
        
        print("  ====> Saving [{0}] {1} Basic Summary Data".format(len(artistIDToRef), "ID => Ref"))
        artistIDToRef.name = "Ref"
        artistIDToRef.index.name = "ArtistID"        
        self.mdbdata.saveArtistIDToRefData(data=artistIDToRef)
        
        print("  ====> Saving [{0}] {1} Basic Summary Data".format(len(artistIDToNumAlbums), "ID => Num Albums"))
        artistIDToNumAlbums.name = "NumAlbums"
        artistIDToNumAlbums.index.name = "ArtistID"        
        self.mdbdata.saveArtistIDToNumAlbumsData(data=artistIDToNumAlbums)
        
        if self.verbose: ts.stop()