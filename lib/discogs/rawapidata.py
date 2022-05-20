""" Raw Discogs Data I/O """

__all__ = ["RawAPIData"]

from apiutils import APIIO

class RawAPIData(APIIO):
    def __init__(self, debug=False):
        super().__init__("Discogs")
        self.apikey = "None"
        
        self.baseURL = "https://api.discogs.com"
        self.format  = "json"
        self.options = {"per_page": "500"}
        
        if debug:
            print("{0} API(Key={1})".format(self.name, self.apikey))
        
        #requestURL = "https://api.discogs.com/artists/{0}/releases?page=1&per_page=500".format(artistID)

        
    ##################################################################################################################################################################
    # Artist Release
    ##################################################################################################################################################################
    def getMasterReleaseURL(self, masterID):
        return f"{self.baseURL}/masters/{masterID}"
    
    def getMasterReleaseData(self, artistName, masterID):
        print("Searching For Releases For {0: <50}\t".format(f"{artistName} ({masterID})"), end="")
        searchResults  = []
        requestResult  = self.get(self.getMasterReleaseURL(masterID))
        
        if requestResult is None or len(requestResult) == 0:
            return None
        print(isinstance(requestResult,dict))
        return requestResult
        
        
    ##################################################################################################################################################################
    # Artist Release
    ##################################################################################################################################################################
    def getArtistReleasesURL(self, artistID):
        return "{0}/artists/{1}/releases?page=1&per_page={2}".format(self.baseURL, artistID, self.options["per_page"])
    
    def getArtistReleases(self, artistName, artistID):
        print("Searching For Releases For {0: <50}\t".format("{0} ({1})".format(artistName,artistID)), end="")
        searchResults  = []
        requestResult  = self.get(self.getArtistReleasesURL(artistID))
        if requestResult is None or len(requestResult) == 0:
            return None
        pagination     = requestResult.get('pagination', {})
        urls           = pagination.get('urls', {})
        nextURL        = urls.get('next', None)
        releases       = requestResult.get('releases', [])
        releaseResults = [discogsRelease(item).get() for item in releases]
        searchResults += releaseResults
        print("   ===> {0}".format(len(searchResults)), end=" ")
        while nextURL is not None:
            self.sleep(3)
            requestResult  = self.get(nextURL)
            if requestResult is None:
                return None
            pagination     = requestResult.get('pagination', {})
            urls           = pagination.get('urls', {})
            nextURL        = urls.get('next', None)
            releases       = requestResult.get('releases', [])
            releaseResults = [discogsRelease(item).get() for item in releases]
            searchResults += releaseResults
            if nextURL:
                print(".", end="")
        print(" {0}".format(len(searchResults)))
        return searchResults
    
    

##################################################################################################################################################################
# API Data Classes
##################################################################################################################################################################
class discogsRelease:
    def __init__(self, item):
        self.id     = item.get('id')
        self.type   = item.get('type')
        self.format = item.get('format')
        self.label  = item.get('label')
        self.name   = item.get('title')
        self.url    = item.get('resource_url')
        self.role   = item.get('role')
        self.artist = item.get('artist')
        self.year   = item.get('year')
        
        self.main_release = item.get('main_release')
        
    def get(self):
        return self.__dict__