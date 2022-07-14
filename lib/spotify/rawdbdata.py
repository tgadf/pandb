""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from base import RawDataBase
from pandas import to_datetime
from .musicdbid import MusicDBID

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
                    
        artistData = self.bsdata
        
        artistID         = artistData.get('sid')
        artistID         = str(artistID) if artistID is not None else None
        artistURI        = artistData.get('uri')
        artistType       = artistData.get('stype')
        artistPopularity = artistData.get('popularity')
        artistName       = artistData.get('name')
        artistAPIURL     = artistData.get('href')
        artistGenres     = artistData.get('genres', [])
        artistFollowers  = artistData.get('followers')
        artistURL        = artistData.get('urls', {}).get('spotify')
        
        generalData  = {"Type": artistType}
        genresData   = artistGenres if len(artistGenres) > 0 else None
        externalData = {'SpotifyAPI': {"URL": artistAPIURL, "URI": artistURI}}
        extraData    = {'Followers': artistFollowers, "Popularity": artistPopularity}
            
        data                = {}
        data["artist"]      = self.makeRawNameData(name=artistName, err=None)
        data["meta"]        = self.makeRawMetaData(title=None, url=artistURL)
        data["url"]         = self.makeRawURLData(url=artistURL)
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["pages"]       = self.makeRawPageData(ppp=1, tot=1, redo=False, more=False)
        data["profile"]     = self.makeRawProfileData(general=generalData, genres=genresData, external=externalData, extra=extraData)
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)
    
    
    ##############################################################################################################################
    ## Parse Albums Data
    ##############################################################################################################################
    def getAlbumData(self, inputdata):
        self.getDictData(inputdata)
        self.assertData()
                    
        mediaData = {}
        albumsData = self.bsdata
        albumsURL  = albumsData.get('href')
        artistID   = albumsData.get('artistID')
        artistID   = str(artistID) if artistID is not None else None
        

        for albumData in albumsData.get('albums', []):
            albumID      = albumData.get('sid')
            albumGroup   = albumData.get('album_group')
            albumType    = albumData.get('album_type')
            albumSType   = albumData.get('stype')
            albumArtists = [{artist['sid']: artist['name']} for artist in albumData.get('artists', [])]
            albumURL     = albumData.get('urls', {}).get('spotify')
            albumURI     = albumData.get('uri')
            albumAPI     = albumData.get('href')
            albumName    = albumData.get('name')
            albumTracks  = albumData.get('numtracks')                
            albumDate    = albumData.get('date')
            try:
                albumYear    = to_datetime(albumDate).year
            except:
                albumYear    = None

            if all([albumGroup,albumType]):
                mediaName = " + ".join([albumGroup,albumType])
            elif albumGroup is not None:
                mediaName = albumGroup
            elif albumType is not None:
                mediaName = albumType
            else:
                mediaName = "Unknown"
                
            amdc = self.makeRawMediaReleaseData(album=albumName, url=albumURL, artist=albumArtists, code=albumID, year=albumYear, aclass=albumSType,
                                                aformat={"URI": albumURI, "API": albumAPI, "Date": albumDate, "NumTracks": albumTracks})
            if mediaData.get(mediaName) is None:
                mediaData[mediaName] = []
            mediaData[mediaName].append(amdc)
            
        data                = {}
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["media"]       = self.makeRawMediaData(media=mediaData)
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)