""" Universal (All DBs) MetaData Classes """

__all__ = ["UniversalMetaData"]

from .base import MetaDataUtilsBase
from timeutils import Timestat
from listUtils import getFlatList
from pandas import DataFrame, Series
from statistics import median


class UniversalMetaData:
    def __init__(self, **kwargs):
        self.utils       = MetaDataUtilsBase(**kwargs)

    ###############################################################################################################
    # Basic MetaData
    ###############################################################################################################
    def getBasicMetaData(self, modValData):
        artistNames     = modValData.apply(lambda rData: rData.artist.name)
        artistNames.name = "ArtistName"
        artistURLs      = modValData.apply(lambda rData: rData.url.url)
        artistURLs.name = "URL"
        artistNumAlbums = modValData.apply(lambda rData: sum(rData.mediaCounts.counts.values()))
        artistNumAlbums.name = "NumAlbums"

        metaData = DataFrame([artistNames,artistURLs,artistNumAlbums]).T
        return metaData
    

    ###############################################################################################################
    # Date MetaData
    ###############################################################################################################
    def getDatesMetaData(self, modValData):
        def getMediaDateStats(mediaDates):
            mediaTypeDates = {}
            for mediaType,mediaTypeYears in mediaDates.items():
                mediaTypeYearsData = []
                for year in mediaTypeYears:
                    try:
                        yearValue = int(year)
                    except:
                        continue
                    mediaTypeYearsData.append(yearValue)

                if len(mediaTypeYearsData) > 0:
                    mediaTypeDates[mediaType] = mediaTypeYearsData
            mediaTypeDates  = getFlatList(mediaTypeDates.values())
            mediaDatesStats = (min(mediaTypeDates), max(mediaTypeDates), int(median(mediaTypeDates))) if len(mediaTypeDates) > 0 else (None,None,None)
            return mediaDatesStats

        artistMediaDates = modValData.apply(self.utils.getMediaDates).apply(getMediaDateStats)
        
        metaData = artistMediaDates.apply(Series)
        metaData.columns = ["MinYear", "MaxYear", "MedianYear"]
        return metaData