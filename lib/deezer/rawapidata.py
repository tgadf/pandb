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
    # Artist Search
    #############################################################################################################################
    def getArtistSearchURL(self, artistName):
        return "{0}/search?q=artist:\"{0}\"".format(self.baseURL, quote(artistName))

    def getArtistSearch(self, artistName):
        print("Searching For {0: <50}".format(artistName), end="")
        searchResults  = []
        requestResult  = self.get(self.getArtistSearch(artistName))
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
    
    
    
##################################################################################################################################################################
# API Data Classes
##################################################################################################################################################################
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