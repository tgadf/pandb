""" Raw Genius Data I/O """

__all__ = ["RawAPIData"]

from apiutils import APIIO
from urllib.parse import quote
from pandas import Series

class RawAPIData(APIIO):
    def __init__(self, debug=False):
        super().__init__("Genius")
        client_access_token = "lllWDHXkTwmxqpZCPyAA8EwX4pilPXKf7x4E_PKNDfMtiwtXvfahmVYL6WSb2mlQ"
        self.apikey = client_access_token

        self.baseURL = "http://api.genius.com"
        self.format  = "json"
        self.options = {"per_page": "50"}
        self.debug   = debug

        if debug:
            print("{0} API(Key={1})".format(self.name, self.apikey))
        
        
    ##################################################################################################################################################################
    # API Parser
    ##################################################################################################################################################################
    def getResponse(self, response):
        retval = response.get('response', {}) if isinstance(response, dict) else {}
        return retval

    
    ##################################################################################################################################################################
    # Artist Info
    ##################################################################################################################################################################
    def getArtistInfoURL(self, artist_id):
        return "{0}/artists/{1}?access_token={2}".format(self.baseURL, artist_id, self.apikey)
    
    def getArtistInfo(self, artistName, artistID):
        print("Searching For Songs For {0: <50}\t".format("{0} ({1})".format(artistName,artistID)), end="")
        geniusRecord = self.getResponse(self.get(self.getArtistInfoURL(artistID)))
        print(" {0}".format(len(geniusRecord)))
        return geniusRecord

        
    ##################################################################################################################################################################
    # Artist Search
    ##################################################################################################################################################################
    def getArtistSearchURL(self, search_term):
        #genius_search_url = f"http://api.genius.com/search?q={search_term}&access_token={client_access_token}"
        url="{0}/search?q={1}&access_token={2}".format(self.baseURL, quote(search_term), self.apikey)
        return url
    

    def getArtistSearchData(self, artistName):
        print("Searching For {0: <50}".format(artistName), end="")
        response = self.get(self.getArtistSearchURL(artistName))
        results  = self.getResponse(response)
        hits     = results.get('hits', [])
        geniusRecords = [geniusSearchRecord(item).get() for item in hits]
        retval = {item['artist']['id']: item['artist']['name'] for item in geniusRecords}
        print("{0}  [{1}]".format(len(geniusRecords),len(retval)))
        return retval


    ##################################################################################################################################################################
    # Artist Info
    ##################################################################################################################################################################
    def getArtistSongsURL(self, artist_id, page, per_page=50):
        #genius_artist_songs_url = f"http://api.genius.com/artists/{artist_id}/songs?per_page={per_page}?&page={page}&access_token={client_access_token}"
        return "{0}/artists/{1}/songs?per_page={2}&page={3}&access_token={4}".format(self.baseURL, artist_id, self.options["per_page"], page, self.apikey)
        #return genius_artist_songs_url    
        
    def getArtistSongs(self, artistName, artistID):
        print("Searching For Songs For {0: <50}\t".format("{0} ({1})".format(artistName,artistID)), end="")
        searchResults  = []
        page           = 1
        requestResult  = self.getResponse(self.get(self.getArtistSongsURL(artistID, page=page)))
        if len(requestResult) == 0:
            return None
        page = requestResult.get('next_page', None)
        try:
            searchResults += requestResult['songs']
        except:
            return None
        print("   ===> {0}".format(len(searchResults)), end=" ")
        while page is not None:
            self.sleep(2.0)
            requestResult  = self.getResponse(self.get(self.getArtistSongsURL(artistID, page=page)))
            try:
                searchResults += requestResult['songs']
            except:
                break
            page = requestResult.get('next_page', None)
            if page:
                #print("{0}".format(len(searchResults)), end="")
                print(".", end="")
        print(" {0}".format(len(searchResults)))

        albums = [geniusAlbumsRecord(album).get() for album in searchResults]
        retval = {'artistID': artistID, 'albums': albums}
        return retval
    
    

##################################################################################################################################################################
# API Data Classes
##################################################################################################################################################################
class geniusPrimaryArtist:
    def __init__(self, item):
        if isinstance(item, dict):
            self.api_path   = item.get('api_path')
            self.id         = item.get('id')
            self.name       = item.get('name')
            self.url        = item.get('url')
        
    def get(self):
        return self.__dict__
        

class geniusSearchRecord:
    def __init__(self, record):
        if isinstance(record, dict):
            item = record.get('result', {})
            self.api_path    = item.get('api_path')
            self.id          = item.get('id')
            self.lyrics_id   = item.get('lyrics_owner_id')
            self.pyongs_cnt  = item.get('pyongs_count')
            self.title       = item.get('title')
            self.artistNames = item.get('artist_names')
            primaryArtist    = item.get('primary_artist', {})
            self.artist      = geniusPrimaryArtist(primaryArtist).get()
        
    def get(self):
        return self.__dict__
    
    
class geniusAlbumsRecord:
    def __init__(self, item):
        if isinstance(item, dict):
            self.api_path    = item.get('api_path')
            self.id          = item.get('id')
            self.lyrics_id   = item.get('lyrics_owner_id')
            self.pyongs_cnt  = item.get('pyongs_count')
            self.full_title  = item.get('title_with_featured')
            self.title       = item.get('title')
            self.url         = item.get('url')
            self.artistNames = item.get('artist_names')
            primaryArtist    = item.get('primary_artist', {})
            self.artist      = geniusPrimaryArtist(primaryArtist).get()
        
    def get(self):
        return self.__dict__