""" Raw LastFM Data I/O """

__all__ = ["RawAPIData"]

from apiutils import APIIO
from urllib.parse import quote

class RawAPIData(APIIO):
    def __init__(self, debug=False):
        super().__init__("LastFM")
        self.apikey = "0017f8ea36758d0923766b8121a98984"
        
        self.baseURL = "http://ws.audioscrobbler.com/2.0/"
        self.format  = "json"
        self.options = {"limit": "10000"}
        self.method  = {"TopTracks": "artist.gettoptracks", "TopAlbums": "artist.gettopalbums", "ArtistInfo": "artist.getinfo", "ArtistSearch": "artist.search"}
        
        if debug:
            print("{0} API(Key={1})".format(self.name, self.apikey))
        

    def getTopTracksURL(self, artistName):
        url = "{0}?method={1}&artist={2}&api_key={3}&limit={4}&format={5}".format(self.baseURL, self.method["TopTracks"], quote(artistName), self.apikey, self.options["limit"], self.format)        
        return url

    def getTopAlbumsURL(self, artistName):
        url = "{0}?method={1}&artist={2}&api_key={3}&limit={4}&format={5}".format(self.baseURL, self.method["TopAlbums"], quote(artistName), self.apikey, self.options["limit"], self.format)
        return url

    
    ##################################################################################################################################################################
    # Artist Info
    ##################################################################################################################################################################
    def getArtistInfoURL(self, artistName):
        return "{0}?method={1}&artist={2}&api_key={3}&format={4}".format(self.baseURL, self.method["ArtistInfo"], quote(artistName), self.apikey, self.format)
    
    def getArtistInfo(self, artistName, show=True):
        print("Searching For {0: <50}".format(artistName), end="")
        response = self.get(self.getArtistInfoURL(artistName))
        record = lastfmArtistRecord(item=response.get('artist', {}), recordType="Artist").get() if isinstance(response,dict) else {}
        print("{0}/{1}".format(len(response),len(record)))
        return record


    ##################################################################################################################################################################
    # Artist Search
    ##################################################################################################################################################################
    def getArtistSearchURL(self, artistName):
        return "{0}?method={1}&artist={2}&api_key={3}&format={4}".format(self.baseURL, self.method["ArtistSearch"], quote(artistName), self.apikey, self.format)
    
    def getArtistSearchResults(self, artistName, show=True):
        print("Searching For {0: <50}".format(artistName), end="")
        response      = self.get(self.getArtistSearchURL(artistName))
        result        = response.get('results') if isinstance(response,dict) else {}
        totalResults  = result.get('opensearch:totalResults', 0) if isinstance(result,dict) else {}
        artistMatches = result.get('artistmatches', {}) if isinstance(result,dict) else {}
        artistMatches = artistMatches.get('artist', [])
        retval        = [lastfmArtistMatch(artistMatch).get() for artistMatch in artistMatches]
        print("{0: <8}{1}".format(totalResults, len(retval)))
        return retval    
    
    
    
##################################################################################################################################################################
# API Data Classes
##################################################################################################################################################################
class lastfmArtistMatch:
    def __init__(self, artistMatch):
        if isinstance(artistMatch,dict):
            self.name      = artistMatch.get('name')
            self.listeners = artistMatch.get('listeners')
            self.mbid      = artistMatch.get('mbid')
            self.url       = artistMatch.get('url')
        
    def get(self):
        return self.__dict__

class lastfmMediaRecord:
    def __init__(self, item, recordType):
        self.type = recordType
        
        self.URL    = item.get('url')
        self.name   = item.get('name')
        self.mbID   = item.get('mbid')
        self.counts = item.get('playcount')
        artist = item.get('artist', {})
        self.artistName = artist.get('name')
        self.artistMBID = artist.get('mbid')
        self.artistURL  = artist.get('url')
        
    def get(self):
        return self.__dict__
    

class lastfmSimilarArtistRecord:
    def __init__(self, item):
        self.name = item.get('name')
        self.url  = item.get('url')
        
    def get(self):
        return self.__dict__
    
    
class lastfmArtistRecord:
    def __init__(self, item, recordType):
        if isinstance(item,dict):
            self.type = recordType

            self.name = item.get('name')
            self.mbid = item.get('mbid')
            self.url  = item.get('url')

            stats     = item.get('stats', {})
            self.listeners = stats.get('listeners')
            self.playcount = stats.get('playcount')

            similarArtists = item.get('similar', {}).get('artist', [])
            self.similar   = [lastfmSimilarArtistRecord(artist).get() for artist in similarArtists]
            self.tags      = item.get('tags', {}).get('tag', [])
            self.bio       = item.get('bio', {})
        
    def get(self):
        return self.__dict__