""" DB-specific Metadata """

__all__ = ["MetaData"]

from meta import MetaDataBase, MediaTypeRankBase, MetaDataUtilsBase, MediaMetaData, UniversalMetaData
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
        self.utils = GeniusMetaDataUtils()
        self.umd   = UniversalMetaData()
        self.mmd   = MediaMetaData(MediaTypeRank())

        if self.verbose: print("{0} ModValMetaData".format(self.db))
        self.dbmetas = {}
        for meta in self.mdbdata.metas:
            func = "get{0}MetaData".format(meta)
            if hasattr(self.umd.__class__, func) and callable(getattr(self.umd.__class__, func)):
                self.dbmetas[meta] = eval("self.umd.{0}".format(func))
                if self.verbose: print("  ==> {0} (Universal)".format(meta))
            elif hasattr(self.mmd.__class__, func) and callable(getattr(self.mmd.__class__, func)):
                self.dbmetas[meta] = eval("self.mmd.{0}".format(func))
                if self.verbose: print("  ==> {0} (Media)".format(meta))
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
                if self.verbose: print("  ==> {0} ... ".format(meta), end="")
                metaData = metaFunc(modValData)
                if self.verbose: print("{0}".format(metaData.shape))
                eval("self.mdbdata.saveMeta{0}Data".format(meta))(modval=modVal, data=metaData)                        
                    
        if self.verbose: ts.stop()

            
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getLinkMetaData(self, modValData): 
        artistAliases = modValData.apply(self.utils.getAliases)
        artistAliases.name = "Aliases"
           
        relatedArtists = modValData.apply(self.utils.getRelated)
        relatedArtists = relatedArtists.apply(self.utils.fixRelated)
        relatedArtists.name = "RelatedArtists"
           
        metaData = DataFrame([artistAliases,relatedArtists]).T
        return metaData

            
    ###############################################################################################################
    # Metric MetaData
    ###############################################################################################################
    def getMetricMetaData(self, modValData): 
        artistFollowers = modValData.apply(self.utils.getFollowers)
        artistFollowers.name = "Followers"
           
        metaData = DataFrame(artistFollowers)
        return metaData
        
    

#####################################################################################################################################
# Media Type Rank  Utils
#####################################################################################################################################
class MediaTypeRank(MediaTypeRankBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mediaRanking['B'] = ["Song"]
        
        

#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class GeniusMetaDataUtils(MetaDataUtilsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mdbid   = MusicDBID()
        
    def getItem(self, item):
        retval = None
        if self.rawio.isRawLinkData(item):
            retval = {item.text: self.mdbid.get(item.href)}
        elif isinstance(item,artistDBTextClass):
            retval = item.text
        return retval  
    
    def getTextItems(self, items):
        retval = [item.text for item in items] if isinstance(items,list) else None
        return retval
          
    def getItems(self, items):
        retval = [self.getItem(item) for item in items] if isinstance(items,list) else None
        return retval
    
    def getAliases(self, rData):
        retval = self.getGeneralData(rData, "Aliases", [])
        return retval
    
    def getFollowers(self, rData):
        retval = self.getExtraData(rData, "Followers", 0)
        return retval
    
    def getRelated(self, rData):
        retval = self.getExtraData(rData, "Related", {})
        return retval
    
    def fixRelated(self, relatedArtists):
        if not isinstance(relatedArtists,dict):
            return relatedArtists
        
        retval = {}        
        for href,name in relatedArtists.items():
            artistID = self.mdbid.get(href)
            if artistID is not None:
                if isinstance(name,list) and len(name) == 1:
                    retval[artistID] = name[0]
                else:
                    retval[artistID] = name
        return retval
            
    def getMedia(self, rData):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData}.values()) for mediaType,mediaTypeData in media.items()}
        return retval