""" Media MetaData Classes """

__all__ = ["MediaMetaData"]

from .base import MetaDataUtilsBase
from timeutils import Timestat
from pandas import DataFrame

class MediaMetaData:
    def __init__(self, mediaTypeRank, **kwargs):
        self.utils         = MetaDataUtilsBase(**kwargs)
        self.mediaTypeRank = mediaTypeRank
        self.maxMediaNum   = kwargs.get("MaxMediaNum", 500)

            
    ###############################################################################################################
    # Media MetaData
    ###############################################################################################################
    def getRankedMediaCountsData(self, rankedMediaData):
        retval = {mediaTypeRank: sum([len(x) for x in mediaTypeRankData.values()]) for mediaTypeRank,mediaTypeRankData in rankedMediaData.items()}
        return retval
        
    def getRankedMediaData(self, mediaData):
        retval = {}
        if isinstance(mediaData,dict):
            for mediaType,mediaTypeData in mediaData.items():
                mediaTypeRankName = self.mediaTypeRank.getMediaTypeRank(mediaType)
                if retval.get(mediaTypeRankName) is None:
                    retval[mediaTypeRankName] = {}
                retval[mediaTypeRankName][mediaType] = mediaTypeData
        return retval
        
    def getMediaMetaData(self, modValData):
        artistMediaData = modValData.apply(self.utils.getMedia, maxNum=self.maxMediaNum)
        artistMedia     = artistMediaData.apply(self.getRankedMediaData)
        
        mediaRankData = {mediaRankTypeName: artistMedia.apply(lambda x: x.get(mediaRankTypeName) if isinstance(x,dict) else None) for rank,mediaRankTypeName in self.mediaTypeRank.mediaTypes.items()}
                
        metaData = DataFrame(mediaRankData)
        return metaData