""" Metadata Base Class """

__all__ = ["MetaDataBase", "MetaDataUtilsBase"]

from master import MasterParams
from base import RawDataBase

class MetaDataBase:
    def __init__(self, mdbdata, **kwargs):
        self.mdbdata = mdbdata
        self.db      = mdbdata.db
        self.verbose = kwargs.get("debug", kwargs.get("verbose", False))
        
    def getModVals(self, modVal=None):
        modVals = [modVal] if isinstance(modVal,(str,int)) else MasterParams().getModVals()
        return modVals

           
class MetaDataUtilsBase:
    def __init__(self, **kwargs):
        self.rawbase = RawDataBase()
        self.isRawData = self.rawbase.isRawData
        self.isRawTextData = self.rawbase.isRawTextData
        self.isRawLinkData = self.rawbase.isRawLinkData
        self.isRawURLInfoData = self.rawbase.isRawURLInfoData
        self.isRawMediaData = self.rawbase.isRawMediaData
        
    def getProfileData(self, rData):
        profileData  = rData.profile if self.isRawData(rData) else None
        return profileData
        
    def getExtraData(self, rData, key, default=None):
        profileData = self.getProfileData(rData)
        if profileData is None:
            return default
        retval = profileData.extra.get(key,default) if isinstance(profileData.extra, dict) else default
        return retval
        
    def getGeneralData(self, rData, key, default=None):
        profileData = self.getProfileData(rData)
        if profileData is None:
            return default
        retval = profileData.general.get(key,default) if isinstance(profileData.general, dict) else default
        return retval
        
    def getExternalData(self, rData, key, default=None):
        profileData = self.getProfileData(rData)
        if profileData is None:
            return default
        retval = profileData.external.get(key,default) if isinstance(profileData.external, dict) else default
        return retval
        
    def getGenresData(self, rData, default=None):
        profileData = self.getProfileData(rData)
        if profileData is None:
            return default
        retval = profileData.genres if profileData is not None else default
        return retval
        
    def getTagsData(self, rData, default=None):
        profileData = self.getProfileData(rData)
        if profileData is None:
            return default
        retval = profileData.tags if profileData is not None else default
        return retval
        
    def getMediaData(self, rData, default=None):
        mediaData  = rData.media if self.isRawData(rData) else default
        retval = mediaData.media if self.isRawMediaData(mediaData) else default
        return retval
    
    def getMedia(self, rData, maxNum=100):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData[:maxNum]}.values()) for mediaType,mediaTypeData in media.items()}
        return retval