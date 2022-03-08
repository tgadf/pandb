""" Universal (All DBs) MetaData Classes """

__all__ = ["UniversalMetaData"]

from .base import MetaDataUtilsBase
from timeutils import Timestat
from pandas import DataFrame

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