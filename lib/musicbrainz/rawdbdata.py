""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from mdbbase import RawDataBase
from .musicdbid import MusicDBID
from pathlib import PosixPath
from hashlib import md5
from pandas import Series

class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()
        
        
    ##############################################################################################################################
    ## Parse Artist Data
    ##############################################################################################################################
    def getArtistData(self, inputdata):
        self.getSeriesData(inputdata)
        self.assertData()

        artistData  = self.bsdata
        artistName  = str(artistData["ArtistName"])
        artistGID   = artistData['ArtistGID']
        artistURL   = "https://musicbrainz.org/artist/{0}".format(artistGID)
        artistID    = artistData["MyArtistID"]


        ########################################################################
        # Get General Data
        ########################################################################        
        generalData = {}
        generalData["SortName"]   = artistData["ArtistSortName"]
        generalData["Aliases"]    = artistData["Aliases"]
        generalData["Gender"]     = artistData["Gender"]
        generalData["Country"]    = artistData["Country"]
        generalData["Formed"]     = artistData["Formed"]
        generalData["Disbanded"]  = artistData["Disbanded"]
        generalData["ArtistType"] = artistData["ArtistType"]
        generalData["ISNI"]       = artistData["ISNICode"]
        generalData = {k: v for k,v in generalData.items() if v is not None}
        generalData = generalData if len(generalData) > 0 else None

        
        ########################################################################
        # Get URLs
        ########################################################################
        externalData = {}
        artistURLs = artistData["URLs"]
        artistURLs = artistURLs if isinstance(artistURLs, list) else []
        for (urlType,url) in artistURLs:
            adblink      = self.makeRawLinkData(None)
            adblink.href = url
            adblink.err  = None
            if externalData.get(urlType) is None:
                externalData[urlType] = []
            externalData[urlType].append(adblink)
        externalData = externalData if len(externalData) > 0 else None
            
        data                = {}
        data["artist"]      = self.makeRawNameData(name=artistName, err=None)
        data["meta"]        = self.makeRawMetaData(title=None, url=artistURL)
        data["url"]         = self.makeRawURLData(url=artistURL)
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["pages"]       = self.makeRawPageData(ppp=1, tot=1, redo=False, more=False)
        data["profile"]     = self.makeRawProfileData(general=generalData, external=externalData)
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)   
        
        
    ##############################################################################################################################
    ## Parse Album Data
    ##############################################################################################################################
    def getAlbumData(self, inputdata):
        self.getTupleData(inputdata)
        self.assertData()

        artistID,artistData  = self.bsdata
        mediaData = {}
        for code,(albumName,mediaName,albumGID) in artistData.items():
            if mediaData.get(mediaName) is None:
                mediaData[mediaName] = []
            albumURL = "https://musicbrainz.org/releasegroup/{0}".format(albumGID)                
            amdc = self.makeRawMediaReleaseData(album=albumName, url=albumURL, artist=None, code=code)
            mediaData[mediaName].append(amdc)
            
        data                = {}
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["media"]       = self.makeRawMediaData(media=mediaData)
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.makeRawFileInfoData(None)
        return self.makeRawData(**data)
        
        
    ##############################################################################################################################
    ## Parse Recording Data
    ##############################################################################################################################
    def getRecordingData(self, inputdata):
        self.getTupleData(inputdata)
        self.assertData()

        artistID,artistData  = self.bsdata
        artistRecordings = Series(artistData).drop_duplicates()
        mediaData = {}
        mediaName = "Recordings"
        if mediaData.get(mediaName) is None:
            mediaData[mediaName] = []
        codes = {}
        for idx,(recName,recTime) in artistRecordings.items():
            m = md5()
            m.update(str(recName).encode('utf-8'))
            m.update(str(recTime).encode('utf-8'))
            hashval = m.hexdigest()
            code    = str(int(hashval, 16) % int(1e6))
            if codes.get(code) is not None:
                continue
            codes[code] = True

            amdc = self.makeRawMediaReleaseData(album=str(recName), url=None, artist=None, code=code)
            mediaData[mediaName].append(amdc)
            
        data                = {}
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["media"]       = self.makeRawMediaData(media=mediaData)
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.makeRawFileInfoData(None)
        return self.makeRawData(**data)
    
    
    
    ##############################################################################################################################
    ## Parse Work Data
    ##############################################################################################################################
    def getWorkData(self, inputdata):
        self.getTupleData(inputdata)
        self.assertData()

        artistID,artistData  = self.bsdata
        mediaData = {}
        for workID,workType,workName in artistData:
            mediaName = "OtherWork" if workType is None else workType
            if mediaData.get(mediaName) is None:
                mediaData[mediaName] = []
            m = md5()
            codes = {}
            m.update(str(workID).encode('utf-8'))
            m.update(str(mediaName).encode('utf-8'))
            m.update(str(workName).encode('utf-8'))
            hashval = m.hexdigest()
            code    = str(int(hashval, 16) % int(1e6))
            if codes.get(code) is not None:
                continue
            codes[code] = True

            amdc = self.makeRawMediaReleaseData(album=str(workName), url=None, artist=None, code=code)
            mediaData[mediaName].append(amdc)
            
        data                = {}
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["media"]       = self.makeRawMediaData(media=mediaData)
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.makeRawFileInfoData(None)
        return self.makeRawData(**data)