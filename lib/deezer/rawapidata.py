""" Raw Deezer Data I/O """

__all__ = ["RawAPIData"]

from apiutils import APIIO

class RawAPIData(APIIO):
    def __init__(self, debug=False):
        super().__init__("Deezer")
        self.apikey = None
        
        self.baseURL = "https://api.deezer.com"
        self.format  = "json"
        self.options = {}
        
        print("{0} API(Key=None)".format(self.name))
        
        
    #############################################################################################################################
    # Artist Top 5 Tracks
    #############################################################################################################################
    def getArtistTop5URL(self, artistID):
        return "{0}/artist/{1}/top".format(self.baseURL, artistID)
    
    
    #############################################################################################################################
    # Artist Info
    #############################################################################################################################
    def getArtistInfoURL(self, artistID):
        return "{0}/artist/{1}".format(self.baseURL, artistID)
    
    def getArtistInfoData(self, artistName,artistID):
        print("Searching For Artist Info For {0: <50}\t".format("{0} ({1})".format(artistName,artistID)), end="")
        searchResults  = []
        requestResult  = self.get(self.getArtistInfoURL(artistID))
        if requestResult is None:
            print("Error")
            return None
        retval = deezerRelatedArtist(requestResult).get() if isinstance(requestResult, dict) else {}
        print(len(retval) > 0)
        return retval

    
    #############################################################################################################################
    # Artist Related
    #############################################################################################################################
    def getArtistRelatedURL(self, artistID):
        return "{0}/artist/{1}/related".format(self.baseURL, artistID)
    
    def getArtistRelatedData(self, artistName,artistID):
        print("Searching For Related Artists For {0: <50}\t".format("{0} ({1})".format(artistName,artistID)), end="")
        searchResults  = []
        requestResult  = self.get(self.getArtistRelatedURL(artistID))
        if requestResult is None:
            print("Error")
            return None
        requestData = requestResult.get('data', []) if isinstance(requestResult, dict) else []
        relatedArtists = [deezerRelatedArtist(item).get() for item in requestData]
        #retval = {"ArtistID": artistID, "ArtistName": artistName, "Related": relatedArtists}
        retval = relatedArtists
        print(len(relatedArtists))
        return retval

    
    #############################################################################################################################
    # Artist Albums
    #############################################################################################################################
    def getArtistAlbumsURL(self, artistID):
        return "{0}/artist/{1}/albums".format(self.baseURL, artistID)

    
    #############################################################################################################################
    # Artist Albums
    #############################################################################################################################
    def getGenreArtistsURL(self, genreID):
        return "{0}/genre/{1}/artists".format(self.baseURL, genreID)

    
    #############################################################################################################################
    # Artist Albums
    #############################################################################################################################
    def getGenreArtistsURL(self, genreID):
        return "{0}/genre/{1}/artists".format(self.baseURL, genreID)
        
        

    #############################################################################################################################
    # Artist Search
    #############################################################################################################################
    def getArtistSearchURL(self, artistName):
        return "{0}/search?q=artist:\"{0}\"".format(self.baseURL, quote(artistName))

    def getArtistSearch(self, artistName):
        print("Searching For {0: <50}".format(artistName), end="")
        searchResults  = []
        requestResult  = self.get(self.getArtistSearchURL(artistName))
        if requestResult is None:
            return None
        totalResults   = requestResult.get('total', -1)
        nextURL        = requestResult.get('next', None)
        try:
            searchResults += requestResult['data']
        except:
            return None
        print("   ===> {0}".format(len(searchResults)), end=" ")
        #print("   ===> {0: <10}  :  {1}".format("{0}/{1}".format(len(searchResults),totalResults),nextURL))
        while nextURL is not None:
            requestResult  = self.get(nextURL)
            try:
                searchResults += requestResult['data']
            except:
                return None
            nextURL        = requestResult.get('next', None)
            if nextURL:
                print(".", end="")
                #print("   ===> {0: <10}  :  {1}".format("{0}/{1}".format(len(searchResults),totalResults),nextURL))
        print(" {0}".format(len(searchResults)))
        return searchResults
    
    
    #############################################################################################################################
    # Albums Data URL
    #############################################################################################################################

    #############################################################################################################################
    # Albums Data
    #############################################################################################################################
    def getAlbumURL(self, albumID):
        return f"{self.baseURL}/album/{albumID}"

    def getAlbumData(self, artistName, albumID, albumName):
        print("Searching For {0: <100}".format(f"{artistName} / {albumName} ({albumID})"), end="")
        searchResults  = []
        requestResult  = self.get(self.getAlbumURL(albumID))        
        retval         = DeezerAlbumData(requestResult).get()
        retvalID       = retval.get('id', 'ERROR')
        
        print(f" {retvalID}")
        return retval
    
    
    
##################################################################################################################################################################
# API Data Classes
##################################################################################################################################################################
class DeezerContributorData:
    def __init__(self, record):
        if isinstance(record, dict):
            self.id   = record.get('id')
            self.name = record.get('name')
            self.type = record.get('type')
            self.role = record.get('role')
            
    def get(self):
        return self.__dict__
    
    
class DeezerAlbumArtistData:
    def __init__(self, record):
        if isinstance(record, dict):
            self.id   = record.get('id')
            self.name = record.get('name')
            self.type = record.get('type')
            
    def get(self):
        return self.__dict__
    
    
class DeezerAlbumGenreData:
    def __init__(self, record):
        if isinstance(record, dict):
            self.id   = record.get('id')
            self.name = record.get('name')
            
    def get(self):
        return self.__dict__


class DeezerAlbumTrackData:
    def __init__(self, record):
        if isinstance(record, dict):
            self.id         = record.get('id')
            self.title      = record.get('title')
            self.titleShort = record.get('title_short')
            self.type       = record.get('type')
            self.url        = record.get('link')
            self.rank       = record.get('rank')
            
            artist = record.get('artist', {})
            self.artist = DeezerAlbumArtistData(artist).get()
            
            album = record.get('album', {})
            self.album = {album['id']: album['title']}
            
    def get(self):
        return self.__dict__
    
    
class DeezerAlbumData:
    def __init__(self, record):
        if isinstance(record, dict):
            self.id         = record.get('id')
            self.title      = record.get('title')
            self.upc        = record.get('upc')
            self.url        = record.get('link')
            self.type       = record.get('type')            
            self.label      = record.get('label')
            self.numTracks  = record.get('nb_tracks')
            self.duration   = record.get('duration')
            self.fans       = record.get('fans')
            self.date       = record.get('release_date')
            self.recordType = record.get('record_type')
            self.tracks     = record.get('tracklist')
                        
            genres = record.get('genres', {}).get('data', [])
            self.genres = [DeezerAlbumGenreData(genre).get() for genre in genres]
            
            contributors = record.get('contributors', [])
            self.contributors = [DeezerContributorData(contributor).get() for contributor in contributors]
                                                                          
            artist = record.get('artist', {})
            self.artist = DeezerAlbumArtistData(artist).get()
            
            tracks = record.get('tracks', {}).get('data', [])
            self.tracks = [DeezerAlbumTrackData(track).get() for track in tracks]
            
    def get(self):
        return self.__dict__


class deezerTrack:
    def __init__(self, item):
        self.id        = item.get('id')
        self.id        = str(self.id) if self.id is not None else None
        self.link      = item.get('link')
        self.title     = item.get('title_short')
        self.title     = item.get('title') if self.title is None else self.title
        self.version   = item.get('title_version')
        self.type      = item.get('type')
        self.rank      = item.get('rank')
        self.duration  = item.get('duration')
        self.artistID  = None
        self.albumID   = None

    def setArtistID(self, artistID):
        self.artistID = artistID

    def setAlbumID(self, albumID):
        self.albumID = albumID


class deezerArtist:
    def __init__(self, item):    
        artistData  = item.get('artist', {})
        self.id     = artistData.get('id')
        self.id     = str(self.id) if self.id is not None else None
        self.name   = artistData.get('name')
        self.link   = artistData.get('link')
        self.tracks = artistData.get('tracklist')
        self.type   = artistData.get('type')


class deezerAlbum:
    def __init__(self, item):
        albumData   = item.get('album', {})
        self.id     = albumData.get('id')
        self.id     = str(self.id) if self.id is not None else None
        self.name   = albumData.get('title')
        self.tracks = albumData.get('tracklist')
        self.type   = albumData.get('type')
        self.artistID    = None

    def setArtistID(self, artistID):
        self.artistID = artistID
        
        
class deezerRelatedArtist:
    def __init__(self, item):
        self.id      = item.get('id')
        self.name    = item.get('name')
        self.link    = item.get('link')
        self.tracks  = item.get('tracklist')
        self.type    = item.get('type')
        self.picture = item.get('picture')
        self.albums  = item.get('nb_album')
        self.fans    = item.get('nb_fan')
        
    def get(self):
        return self.__dict__