""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from base import RawDataBase
from .musicdbid import MusicDBID
from dbid import MusicDBIDModVal

class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()
        self.mv = MusicDBIDModVal()
        
        
    ##############################################################################################################################
    ## Parse Artist Data
    ##############################################################################################################################
    def getArtistData(self, inputdata):
        self.getPickledData(inputdata)
        self.assertData()
        
        artistData = self.bsdata
        artistName = artistData.get('name')
        artistURL  = artistData.get('url')
        artistID   = self.aid.getArtistID(artistURL) if artistName is not None else None
        artistMBID = artistData.get('mbid')
        artistType = artistData.get('type')

        artistBio       = artistData.get('bio')
        artistTags      = artistData.get('tags', [])
        artistSimilar   = artistData.get('similar', [])
        artistListeners = artistData.get('listeners')
        artistPlaycount = artistData.get('playcount')

        generalData  = {"Bio": artistBio} if artistBio is not None else None
        externalData = {"MBID": artistMBID} if artistMBID is not None else None
        extraData    = {}
        if len(artistSimilar) > 0:
            extraData["SimilarArtists"] = [self.makeRawURLInfoData(**similarArtist) for similarArtist in artistSimilar]
        if artistPlaycount is not None:
            extraData["PlayCount"] = artistPlaycount
        if artistListeners is not None:
            extraData["Listeners"] = artistListeners
        extraData = extraData if len(extraData) > 0 else None
        tagsData  = [tag['name'] for tag in artistTags] if len(artistTags) > 0 else None


        data                = {}
        data["artist"]      = self.makeRawNameData(name=artistName, err=None)
        data["meta"]        = self.makeRawMetaData(title=None, url=artistURL)
        data["url"]         = self.makeRawURLData(url=artistURL)
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["pages"]       = self.makeRawPageData(ppp=1, tot=1, redo=False, more=False)
        data["profile"]     = self.makeRawProfileData(general=generalData, external=externalData, extra=extraData, tags=tagsData)
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)

        
    ##############################################################################################################################
    ## Parse Albums Data
    ##############################################################################################################################
    def getAlbumData(self, inputdata):
        self.getPickledData(inputdata)
        self.assertData()

        mediaData = {}

        albumsData = self.bsdata
        tracks = albumsData.get('Tracks', [])
        albums = albumsData.get('Albums', [])

        mediaType = "Tracks"
        for track in tracks:
            trackName         = track.get('name')
            trackURL          = track.get('URL')
            trackID           = self.aid.getAlbumID(trackURL)
            trackMBID         = track.get('mbID')
            trackCounts       = track.get('counts')
            trackArtistName   = track.get('artistName')
            trackArtistMBID   = track.get('artistMBID')
            trackArtistURL    = track.get('artistURL')
            trackArtistID     = self.aid.getArtistID(trackArtistURL)
            trackArtistModVal = self.mv.get(trackArtistID)
            #trackArtists      = {"Name": trackArtistName, "ID": trackArtistID, "MBID": trackArtistMBID}
            trackArtists      = trackArtistName

            amdc = self.makeRawMediaReleaseData(album=trackName, url=trackURL, artist=trackArtists, code=trackID, aformat={"Counts": trackCounts}, year=None, aclass=None)
            if mediaData.get(trackArtistID) is None:
                mediaData[trackArtistID] = {}
            if mediaData[trackArtistID].get(mediaType) is None:
                mediaData[trackArtistID][mediaType] = []
            mediaData[trackArtistID][mediaType].append(amdc)


        mediaType = "Albums"
        for album in albums:
            albumName         = album.get('name')
            albumURL          = album.get('URL')
            albumID           = self.aid.getAlbumID(albumURL)
            albumMBID         = album.get('mbID')
            albumCounts       = album.get('counts')
            albumArtistName   = album.get('artistName')
            albumArtistMBID   = album.get('artistMBID')
            albumArtistURL    = album.get('artistURL')
            albumArtistID     = self.aid.getArtistID(albumArtistURL)
            albumArtistModVal = self.mv.get(albumArtistID)
            #albumArtists      = {"Name": albumArtistName, "ID": albumArtistID, "MBID": albumArtistMBID}
            albumArtists      = albumArtistName

            amdc = self.makeRawMediaReleaseData(album=albumName, url=albumURL, artist=albumArtists, code=albumID, aformat={"Counts": trackCounts}, year=None, aclass=None)
            if mediaData.get(albumArtistID) is None:
                mediaData[albumArtistID] = {}
            if mediaData[albumArtistID].get(mediaType) is None:
                mediaData[albumArtistID][mediaType] = []
            mediaData[albumArtistID][mediaType].append(amdc)

            
        retval = {}
        for artistID,artistIDData in mediaData.items():
            if artistID is None:
                continue

            data                = {}
            data["ID"]          = self.makeRawIDData(ID=artistID)
            data["media"]       = self.makeRawMediaData(media=artistIDData)
            data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
            data["info"]        = self.getInfo()
            retval[artistID] = self.makeRawData(**data)
                
        return retval