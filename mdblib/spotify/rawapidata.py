""" Raw Genius Data I/O """

__all__ = ["RawAPIData"]

from apiutils import APIIO
from pathlib import PurePosixPath
from urllib.parse import unquote, urlparse
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

class RawAPIData(APIIO):
    def __init__(self, debug=False):
        super().__init__("Spotify")
        auth_manager=SpotifyClientCredentials(client_id="61e441c3b90c4873aa0e6b9582564f95", client_secret="ae0d0f968bf443fdac1d9ac6ef65fc0f")
        self.sp = Spotify(auth_manager=auth_manager)

        self.apikey = {"client_id": "61e441c3b90c4873aa0e6b9582564f95", "client_secret": "ae0d0f968bf443fdac1d9ac6ef65fc0f"}
        
        self.baseURL = "https://api.spotify.com/artists"
        self.format  = "json"
        self.options = {"per_page": "500"}
        
        if debug:
            print("{0} API(Key={1})".format(self.name, self.apikey))

        
    #########################################################################################################################################
    # Artist Info
    #########################################################################################################################################
    def getArtistIDLookupResults(self, artistID):
        print("Searching For {0: <35}".format(artistID), end="")
        self.sleep(0.25)
        try:
            result = self.sp.artist(artistID)
        except:
            print("==> Error in Spotify ArtistID Lookup for {0}".format(artistID))
            return None
        retval  = [spotifyArtistRecord(result).get()]
        print(len(retval))
        return retval

        
    #########################################################################################################################################
    # Album Info
    #########################################################################################################################################
    def getAlbumIDLookupResults(self, albumID):
        print("Searching For {0: <35}".format(albumID), end="")
        self.sleep(0.25)
        try:
            result = self.sp.album(albumID)
        except:
            print("==> Error in Spotify AlbumID Lookup for {0}".format(albumID))
            return None
        retval  = [spotifyAlbumRecord(result).get()]
        print(len(retval))
        return retval
    
        
    #########################################################################################################################################
    # Artist Info
    #########################################################################################################################################
    def getArtistSearchResults(self, artistName, limit=50):
        print("Searching For {0: <50}".format(artistName), end="")
        self.sleep(0.25)
        try:
            result = self.sp.search(artistName, limit=limit, type='artist')
        except:
            print("==> Error in Spotify search for {0}".format(artistName))
            return None
            

        artists = result.get('artists', {}) if isinstance(result,dict) else {}
        href    = artists.get('href')
        total   = artists.get('total')
        nextURL = artists.get('next')
        prevURL = artists.get('previous')
        items   = artists.get('items', [])
        retval  = [spotifyArtistRecord(item).get() for item in items]
        print(len(retval))
        return retval        
    
        
    #########################################################################################################################################
    # Artist Tracks
    #########################################################################################################################################
    def getArtistTracks(self, artistName, artistID, limit=50):
        print("Searching For Tracks For {0: <50}\t".format("{0} ({1})".format(artistName,artistID)), end="")
        searchResults  = []
        offset         = 0
        try:
            requestResult  = self.sp.artist_top_tracks(artistID, limit=limit, offset=offset)
        except:
            print("==> Error in Spotify track search for {0}".format(artistName))
            return None            
        offset += limit

        if requestResult is None:
            return None
        totalResults   = requestResult.get('total', -1)
        nextURL        = requestResult.get('next', None)
        try:
            searchResults += requestResult['items']
        except:
            return None
        print("   ===> {0: <8}   {1}".format("[{0}]".format(totalResults), len(searchResults)), end=" ")
        while nextURL is not None:
            try:
                requestResult  = self.sp.artist_albums(artistID, limit=limit, offset=offset)
            except:
                print("==> Error in Spotify track search for {0}".format(artistName))
                return None
            offset += limit
            try:
                searchResults += requestResult['items']
            except:
                return None
            nextURL        = requestResult.get('next', None)
            if nextURL:
                #print("{0}".format(len(searchResults)), end="")
                print(".", end="")
                self.sleep(1)
        print(" {0}".format(len(searchResults)))
        return searchResults
    

    #########################################################################################################################################
    # Artist Albums
    #########################################################################################################################################
    def getArtistAlbums(self, artistName, artistID, limit=50):
        print("Searching For Albums For {0: <50}\t".format("{0} ({1})".format(artistName,artistID)), end="")
        searchResults  = []
        offset         = 0
        try:
            requestResult  = self.sp.artist_albums(artistID, limit=limit, offset=offset)
        except:
            print("==> Error in Spotify albums search for {0}".format(artistName))
            return None                        
        offset += limit

        if requestResult is None:
            return None
        totalResults   = requestResult.get('total', -1)
        href           = requestResult.get('href')
        artistID       = PurePosixPath(unquote(urlparse(href).path)).parts[3]
        nextURL        = requestResult.get('next', None)
        try:
            searchResults += requestResult['items']
        except:
            return None
        print("   ===> {0: <8}   {1}".format("[{0}]".format(totalResults), len(searchResults)), end=" ")
        while nextURL is not None:
            self.sleep(3.5)
            try:
                requestResult  = self.sp.artist_albums(artistID, limit=limit, offset=offset)
            except:
                print("==> Error in Spotify albums search for {0}".format(artistName))
                return None                        
            offset += limit
            try:
                searchResults += requestResult['items']
            except:
                return None
            nextURL        = requestResult.get('next', None)
            if nextURL:
                #print("{0}".format(len(searchResults)), end="")
                print(".", end="")
        print(" {0}".format(len(searchResults)))

        albums = [spotifyAlbumRecord(album).get() for album in searchResults]
        retval = {'href': href, 'artistID': artistID, 'albums': albums}
        return retval
    
    

#########################################################################################################################################
# API Data Classes
#########################################################################################################################################
class spotifyArtistRecord:
    def __init__(self, item):
        self.urls       = item.get('external_urls', {})
        self.followers  = item.get('followers', {})
        self.genres     = item.get('genres', [])
        self.href       = item.get('href')
        self.sid        = item.get('id')
        self.name       = item.get('name')
        self.popularity = item.get('popularity')
        self.stype      = item.get('type')
        self.uri        = item.get('uri')
        
    def get(self):
        return self.__dict__
    
    
class spotifyAlbumRecord:
    def __init__(self, item):
        self.album_group = item.get('album_group')
        self.album_type  = item.get('album_type')
        self.artists     = [spotifyArtistRecord(artist).get() for artist in item.get('artists', [])]
        self.urls        = item.get('external_urls', {})
        self.href        = item.get('href')
        self.sid         = item.get('id')
        self.name        = item.get('name')
        self.numtracks   = item.get('total_tracks')
        self.stype       = item.get('type')
        self.uri         = item.get('uri')
        self.date        = item.get('release_date')
        self.date_prec   = item.get('release_date_precision')
        
    def get(self):
        return self.__dict__