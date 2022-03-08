""" Metadata Base Class """

__all__ = ["MetaDataBase", "MetaDataUtilsBase", "SummaryDataBase", "MediaTypeRankBase"]

from master import MasterParams
from base import RawDataBase, MusicDBBaseData
from utils import MusicDBArtistName
from pandas import Series

#########################################################################################################################################################
# Summary Data Base
#########################################################################################################################################################
class SummaryDataBase:
    def __init__(self, mdbdata, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("mdbdata is not of type MusicDBBaseData")
        mp = MasterParams()
        self.searchTypes  = mp.getMetas()
        self.summaryTypes = mp.getMetas()
        self.mdbdata = mdbdata
        self.db      = mdbdata.db
        self.verbose = kwargs.get('debug', kwargs.get('verbose', False))
        self.manc    = MusicDBArtistName()
        self.modVals = MasterParams().getModVals(listIt=True)


#########################################################################################################################################################
# Meta Data Base
#########################################################################################################################################################
class MetaDataBase:
    def __init__(self, mdbdata, **kwargs):
        self.mdbdata = mdbdata
        self.db      = mdbdata.db
        self.verbose = kwargs.get("debug", kwargs.get("verbose", False))
        
    def getModVals(self, modVal=None):
        modVals = [modVal] if isinstance(modVal,(str,int)) else MasterParams().getModVals()
        return modVals
    
    
    
#########################################################################################################################################################
# Media Type Rank Base
#########################################################################################################################################################
class MediaTypeRankBase:
    def __init__(self, **kwargs):
        self.verbose      = kwargs.get('verbose', False)
        self.mediaTypes   = MasterParams().getMedias()
        self.last         = Series(self.mediaTypes).index.max()
        self.mediaRanking = {rank: [] for rank in self.mediaTypes.keys()}
        
        
    def sortMediaData(self, mediaData):
        results      = {rank: {} for rank in self.mediaRanking.keys()}
        remaining    = {}
        for mediaType,cnt in mediaData.iteritems():
            values = Series({rank: sum([tag in mediaType for tag in rankTags]) for rank,rankTags in self.mediaRanking.items() if rank not in [self.last]})
            values = values[values > 0]
            if len(values) > 0:
                results[values.index.max()][mediaType] = cnt
            else:
                remaining[mediaType] = cnt
                
        ## Move overflow to remaining
        remaining.update(results[self.last])
        del results[self.last]
        results.update({"Remaining": remaining})
        return results
    
    
    def getMediaTypeRank(self, mediaType):
        if isinstance(mediaType,str):
            values = Series({rank: sum([tag in mediaType for tag in rankTags]) for rank,rankTags in self.mediaRanking.items() if rank not in [self.last]})
            values = values[values > 0]
            retval = self.mediaTypes[values.index.max()] if len(values) > 0 else self.mediaTypes[self.last]
            return retval
        else:
            retval = self.mediaTypes[self.last]
            return retval
        


#########################################################################################################################################################
# Meta Data Utils Base
#########################################################################################################################################################
class MetaDataUtilsBase:
    def __init__(self, **kwargs):
        self.rawbase = RawDataBase()
        #self.isRawData = self.rawbase.isRawData
        self.isRawTextData = self.rawbase.isRawTextData
        self.isRawLinkData = self.rawbase.isRawLinkData
        self.isRawURLInfoData = self.rawbase.isRawURLInfoData
        self.isRawMediaData = self.rawbase.isRawMediaData
        
    def isRawData(self, rData):
        retval = rData.__class__.__name__ == "RawData"
        return retval
        
    def getProfileData(self, rData):
        retval = getattr(rData, 'profile') if (hasattr(rData, 'profile') and self.isRawData(rData)) else None
        return retval
        
    def getExtraData(self, rData, key, default=None):
        profileData = self.getProfileData(rData)
        extraData   = getattr(profileData, 'extra') if hasattr(profileData, 'extra') else None
        retval      = getattr(extraData, key) if hasattr(extraData, key) else default
        return retval
        
    def getGeneralData(self, rData, key, default=None):
        profileData = self.getProfileData(rData)
        generalData = getattr(profileData, 'general') if hasattr(profileData, 'general') else None
        retval      = getattr(generalData, key) if hasattr(generalData, key) else default
        return retval
        
    def getExternalData(self, rData, key, default=None):
        profileData  = self.getProfileData(rData)
        externalData = getattr(profileData, 'external') if hasattr(profileData, 'external') else None
        retval       = getattr(externalData, key) if hasattr(externalData, key) else default
        return retval
        
    def getGenresData(self, rData, default=None):
        profileData = self.getProfileData(rData)
        retval      = getattr(profileData, 'genres') if hasattr(profileData, 'genres') else default
        return retval
        
    def getTagsData(self, rData, default=None):
        profileData = self.getProfileData(rData)
        retval      = getattr(profileData, 'tags') if hasattr(profileData, 'tags') else default
        return retval
        
    def getMediaData(self, rData, default=None):
        mediaData = getattr(rData, 'media') if (hasattr(rData, 'media') and self.isRawData(rData)) else None
        retval    = getattr(mediaData, 'media') if hasattr(mediaData, 'media') else default
        return retval
    
    def getMedia(self, rData, maxNum=100):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData[:maxNum]}.values()) for mediaType,mediaTypeData in media.items()}
        return retval