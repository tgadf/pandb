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
        self.utils = LastFMMetaDataUtils()
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
        similarArtists = modValData.apply(self.utils.getSimilarArtists)
        similarArtists.name = "SimilarArtists"   

        artistMB = modValData.apply(self.utils.getMBID)
        artistMB.name = "MBID"   

        metaData = DataFrame([similarArtists,artistMB]).T
        return metaData      

            
    ###############################################################################################################
    # Genre MetaData
    ###############################################################################################################
    def getGenreMetaData(self, modValData):
        artistTag = modValData.apply(self.utils.getTags)
        artistTag.name = "Tag"
           
        metaData = DataFrame(artistTag)
        return metaData

            
    ###############################################################################################################
    # Metric MetaData
    ###############################################################################################################
    def getMetricMetaData(self, modValData): 
        playCounts = modValData.apply(self.utils.getPlayCounts)
        playCounts.name = "PlayCounts"
        
        listeners = modValData.apply(self.utils.getListeners)
        listeners.name = "Listeners"
           
        metaData = DataFrame([playCounts,listeners]).T
        return metaData
           

#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class LastFMMetaDataUtils(MetaDataUtilsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mdbid   = MusicDBID()        
        
    def getItem(self, item):
        retval = None
        if self.isRawLinkData(item):
            retval = {item.text: self.mdbid.get(item.title)}
        elif self.isRawURLInfoData(item):
            retval = {item.name: self.mdbid.get(item.url)}
        elif self.isRawTextData(item):
            retval = item.text
        return retval
    
    def getTextItems(self, items):
        retval = [item.text for item in items] if isinstance(items,list) else None
        return retval
          
    def getItems(self, items):
        retval = [self.getItem(item) for item in items] if isinstance(items,list) else None
        return retval

    def getTags(self, rData):
        retval = self.getTagsData(rData)
        return retval
    
    def getMBID(self, rData):
        retval = self.getExternalData(rData, "MBID")
        return retval
    
    def getPlayCounts(self, rData):
        retval = self.getExtraData(rData, "PlayCount", 0)
        return retval
    
    def getListeners(self, rData):
        retval = self.getExtraData(rData, "Listeners", 0)
        return retval

    def getSimilarArtists(self, rData):
        retval = self.getItems(self.getExtraData(rData, "SimilarArtists", []))
        return retval
        
    def getMedia(self, rData, maxNum=500):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData[:maxNum]}.values()) for mediaType,mediaTypeData in media.items()}
        return retval