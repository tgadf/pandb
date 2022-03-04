""" Basic MetaData Classes """

__all__ = ["BasicMetaDataBase", "MediaMetaDataBase"]

from .base import MetaDataBase
from mdbid import MusicDBIDModVal
from timeutils import Timestat
from pandas import Series, DataFrame


class BasicMetaDataBase(MetaDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        
    def make(self, modVal=None):
        if self.verbose: ts = Timestat("Making Basic {0} MetaData".format(self.db))
        modVals = [modVal] if isinstance(modVal,(str,int)) else list(range(MusicDBIDModVal().maxModVal))
        for i,modVal in enumerate(modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))
            modValData = self.mdbdata.getModValData(modVal)
            
            basicMetaData = self.getBasicMetaData(modValData)
            basicMetadata["ArtistName"] = basicMetadata["ArtistName"].apply(lambda x: x if isinstance(x,str) else None)
            basicMetadata["URL"] = basicMetadata["URL"].apply(lambda x: x if isinstance(x,str) else None)
            
            self.mdbdata.saveBasicMetaData(modval=modVal, data=basicMetadata)
        if self.verbose: ts.stop()  
            
    def getBasicMetaData(self, modValData):
        artistNames     = modValData.apply(lambda rData: rData.artist.name)
        artistNames.name = "ArtistName"
        artistURLs      = modValData.apply(lambda rData: rData.url.url)
        artistURLs.name = "URL"
        artistNumAlbums = modValData.apply(lambda rData: sum(rData.mediaCounts.counts.values()))
        artistNumAlbums.name = "NumAlbums"            

        metaData = DataFrame([artistNames,artistURLs,artistNumAlbums]).T
        return metaData
    
    
class MediaMetaDataBase(MetaDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        
    def make(self, modVal=None):
        if self.verbose: ts = Timestat("Making Media {0} MetaData".format(self.db))
        modVals = [modVal] if isinstance(modVal,(str,int)) else list(range(MusicDBIDModVal().maxModVal))
        for i,modVal in enumerate(modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))
            modValData = self.mdbdata.getModValData(modVal)
            mediaMetadata = modValData.apply(lambda rData: {mediaType: {release.code: release.album for release in mediaTypeData} 
                                                            for mediaType,mediaTypeData in rData.media.media.items()})
            self.mdbdata.saveMediaMetaData(modval=modVal, data=mediaMetadata)
        if self.verbose: ts.stop()