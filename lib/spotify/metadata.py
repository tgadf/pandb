""" DB-specific Metadata """

__all__ = ["MetaData"]

from mdbmeta import MetaDataBase, MetaDataUtilsBase, UniversalMetaData
from pandas import DataFrame
from listUtils import getFlatList
from timeutils import Timestat

from .musicdbid import MusicDBID
from .rawdbdata import RawDBData


#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class MetaData(MetaDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        self.utils = SpotifyMetaDataUtils()
        self.umd   = UniversalMetaData()

        if self.verbose: print("{0} ModValMetaData".format(self.db))
        self.dbmetas = {}
        for meta in self.mdbdata.metas:
            func = "get{0}MetaData".format(meta)
            if hasattr(self.umd.__class__, func) and callable(getattr(self.umd.__class__, func)):
                self.dbmetas[meta] = eval("self.umd.{0}".format(func))
                if self.verbose: print("  ==> {0}".format(meta))
            elif hasattr(self.__class__, func) and callable(getattr(self.__class__, func)):
                self.dbmetas[meta] = eval("self.{0}".format(func))
                if self.verbose: print("  ==> {0}".format(meta))
                
        
    def make(self, modVal=None):
        modVals = self.getModVals(modVal)
        if self.verbose: ts = Timestat("Making {0} {1} MetaData".format(len(modVals), self.db))
        
        for i,modVal in enumerate(modVals):            
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))
            modValData = self.mdbdata.getModValData(modVal)

            for meta,metaFunc in self.dbmetas.items():
                metaData = metaFunc(modValData)
                eval("self.mdbdata.saveMeta{0}Data".format(meta))(modval=modVal, data=metaData)                        
                    
        if self.verbose: ts.stop()
            
            
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getLinkMetaData(self, modValData):
        artistType = modValData.apply(self.utils.getType)
        artistType.name = "Type"
        
        metaData = DataFrame(artistType)
        return metaData
        
                    
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getGenreMetaData(self, modValData):
        artistGenres = modValData.apply(self.utils.getGenres)
        artistGenres.name = "Genre"
        metaData = DataFrame(artistGenres)
        return metaData
        
                    
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getMetricMetaData(self, modValData):
        artistFollowers = modValData.apply(self.utils.getFollowers)
        artistFollowers.name = "Followers"

        artistPopularity = modValData.apply(self.utils.getPopularity)
        artistPopularity.name = "Popularity"

        metaData = DataFrame([artistFollowers,artistPopularity]).T
        return metaData

                
#####################################################################################################################################
# 
#####################################################################################################################################
class SpotifyMetaDataUtils(MetaDataUtilsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mdbid   = MusicDBID()        
                        
    def getFollowers(self, rData):
        return self.getExtraData(rData, "Followers", 0)

    def getPopularity(self, rData):
        return self.getExtraData(rData, "Popularity", 0)
        
    def getGenres(self, rData):
        retval = self.getGenresData(rData)
        return retval
                
    def getType(self, rData):
        return self.getGeneralData(rData, "Type", "artist")
    
    def getMedia(self, rData):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData}.values()) for mediaType,mediaTypeData in media.items()}
        return retval