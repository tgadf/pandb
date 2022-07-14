""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from base import RawDataBase
from pandas import to_datetime
from .musicdbid import MusicDBID
from .rawapidata import deezerAlbum, deezerArtist, deezerTrack


class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()
    
        
    ##############################################################################################################################
    ## Parse Master Release Data
    ##############################################################################################################################
    def getAlbumData(self, inputdata):
        self.getDictData(inputdata)
        self.assertData()
        code         = str(self.bsdata['id'])
        album        = self.bsdata['title']
        url          = self.bsdata['url']
        numTracks    = self.bsdata['numTracks']
        duration     = self.bsdata['duration']
        fans         = self.bsdata['fans']
        date         = self.bsdata['date']
        label        = self.bsdata['label']
        upc          = self.bsdata['upc']
        genres       = [genre['name'] for genre in self.bsdata['genres']]
        contributors = self.bsdata['contributors']
        artist       = self.bsdata['artist']
        try:
            year = to_datetime(date).year
        except:
            year = None
            
        amdc = self.makeRawMediaReleaseData(album=album, url=url, artist={"Primary": artist, "Contributors": contributors}, code=code, aformat={"Genres": genres, "Label": label, "Fans": fans, "NumTracks": numTracks, "Duration": duration, "UPC": upc}, aclass=None, year=year)
        return amdc
    
    
    ##############################################################################################################################
    ## Parse Data
    ##############################################################################################################################
    def getArtistData(self, inputdata):
        self.getListData(inputdata)
        self.assertData()
        
        artistsData = {}
        tracks = self.bsdata
        for item in tracks:
            dArtist = deezerArtist(item)
            dAlbum  = deezerAlbum(item)
            dAlbum.setArtistID(dArtist.id)
            dTrack  = deezerTrack(item)
            dTrack.setArtistID(dArtist.id)
            dTrack.setAlbumID(dAlbum.id)
            
            if artistsData.get(dArtist.id) is None:
                artistsData[dArtist.id] = {"Artist": dArtist, "Tracks": {}, "Albums": {}}
            artistsData[dArtist.id]["Tracks"][dTrack.id] = dTrack
            artistsData[dArtist.id]["Albums"][dAlbum.id] = dAlbum


        retval = {}
        for artistID,artistIDData in artistsData.items():            
            artistName = artistIDData["Artist"].name
            artistURL  = artistIDData["Artist"].link
            artistType = artistIDData["Artist"].type
            
            generalData = {"Type": artistType}
            extraData   = {"TopTracks": artistIDData["Artist"].tracks}
            
            mediaData = {}
            
            mediaType = "Albums"
            for albumID,albumData in artistIDData["Albums"].items():
                albumName   = albumData.name
                albumURL    = albumData.tracks
                albumType   = albumData.type
                
                amdc = self.makeRawMediaReleaseData(album=albumName, url=albumURL, artist=artistID, code=albumID, year=None, aclass=None, aformat={"Type": albumType})
                if mediaData.get(mediaType) is None:
                    mediaData[mediaType] = []
                mediaData[mediaType].append(amdc)
            
            mediaType = "Tracks"
            for trackID,trackData in artistIDData["Tracks"].items():
                trackName     = trackData.title
                trackURL      = trackData.link
                trackType     = trackData.type
                trackDuration = trackData.duration
                trackRank     = trackData.rank
                
                amdc = self.makeRawMediaReleaseData(album=trackName, url=trackURL, artist=artistID, code=trackID, year=None, aclass=None, aformat={"Type": trackType, "Duration": trackDuration, "Rank": trackRank})
                if mediaData.get(mediaType) is None:
                    mediaData[mediaType] = []
                mediaData[mediaType].append(amdc)
            
            
            data                = {}
            data["artist"]      = self.makeRawNameData(name=artistName, err=None)
            data["meta"]        = self.makeRawMetaData(title=None, url=artistURL)
            data["url"]         = self.makeRawURLData(url=artistURL)
            data["ID"]          = self.makeRawIDData(ID=artistID)
            data["pages"]       = self.makeRawPageData(ppp=1, tot=1, redo=False, more=False)
            data["profile"]     = self.makeRawProfileData(general=generalData, extra=extraData)
            data["media"]       = self.makeRawMediaData(media=mediaData)
            data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
            data["info"]        = self.getInfo()
            retval[artistID]    = self.makeRawData(**data)

        return retval