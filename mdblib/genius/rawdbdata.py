""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from mdbbase import RawDataBase
from .musicdbid import MusicDBID

class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()
        
    ##############################################################################################################################
    ## Merge Raw Data
    ##############################################################################################################################
    def mergeRawData(self, artistData, albumsData):
        if all([isinstance(x,rawData) for x in [artistData, albumsData]]):
            artistData.media = albumsData.media
            artistData.mediaCounts = albumsData.mediaCounts
            artistData.info = albumsData.info
            return artistData
        elif isinstance(albumsData,rawData) and artistData is None:
            return albumsData
        elif isinstance(artistData,rawData) and albumsData is None:
            return artistData
        else:
            raise ValueError("Can not merge empty raw data")
        
        
    ##############################################################################################################################
    ## Parse Artist Data
    ##############################################################################################################################
    def getArtistData(self, inputdata):
        self.getPickledData(inputdata)
        self.assertData()
        geniusArtistData  = self.bsdata.get('artist', {})
        artistAliases     = geniusArtistData.get('alternate_names')
        artistURL         = geniusArtistData.get('url')
        artistAPIPath     = geniusArtistData.get('api_path')
        artistAPIURL      = "http://api.genius.com{0}".format(artistAPIPath)
        artistID          = geniusArtistData.get('id')
        artistID          = str(artistID) if isinstance(artistID,int) else None
        artistName        = geniusArtistData.get('name')
        artistFollowers   = geniusArtistData.get('followers_count')
        artistDescription = geniusArtistData.get('description', {})
        

        generalData = {}
        generalData["Aliases"]     = artistAliases
        generalData["Description"] = artistDescription
        generalData = {k: v for k,v in generalData.items() if v is not None}
        generalData = generalData if len(generalData) > 0 else None
        
        
        def getChild(child, refs = []):
            if isinstance(child,dict):
                if child.get('tag') == 'a':
                    refs.append(child)
                    return
                for kid in child.get('children', []):
                    getChild(kid, refs)
            else:
                # Terminal
                return
                #print(child)

        descriptionDOM = artistDescription.get('dom', {})
        childData = []
        if descriptionDOM.get('tag') == 'root':
            for child in descriptionDOM.get('children', []):
                getChild(child, childData)

        internal = {}
        external = []
        for cd in childData:
            cdAPI = cd.get('data', {}).get('api_path')
            cdChildren = cd.get('children', [])
            cdhref = cd.get('attributes', {}).get('href')
            if cdAPI is not None:
                internal[cdAPI] = cdChildren
            else:
                external.append(cdhref)
        
        
        extraData = {}
        extraData["Followers"] = artistFollowers
        extraData["API"] = artistAPIURL
        if len(internal) > 0:
            extraData["Related"] = internal
            
        externalData = external if len(external) > 0 else None
            
        data                = {}
        data["artist"]      = self.makeRawNameData(name=artistName, err=None)
        data["meta"]        = self.makeRawMetaData(title=None, url=artistURL)
        data["url"]         = self.makeRawURLData(url=artistURL)
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["pages"]       = self.makeRawPageData(ppp=1, tot=1, redo=False, more=False)
        data["profile"]     = self.makeRawProfileData(general=generalData, external=externalData, extra=extraData)
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)

        
    ##############################################################################################################################
    ## Parse Albums Data
    ##############################################################################################################################
    def getAlbumData(self, inputdata):
        self.getPickledData(inputdata)
        self.assertData()
        
        artistAlbums = self.bsdata.get("albums", [])
        mediaData = {}
        for item in artistAlbums:
            code             = item.get('id')
            albumTitle       = item.get('title')
            albumFullTitle   = item.get('full_title')
            albumURL         = item.get('url')
            albumAPIPath     = item.get('api_path')
            albumArtist      = item.get('artist')
            albumArtistNames = item.get('artistNames')
            albumYear        = None

            mediaName = "Song"

            amdc = self.makeRawMediaReleaseData(album=albumTitle, url=albumURL, artist={"Primary": albumArtist, "Full": albumArtistNames}, code=code, aformat=None, aclass={"FullTitle": albumFullTitle}, year=albumYear)
            if mediaData.get(mediaName) is None:
                mediaData[mediaName] = []
            mediaData[mediaName].append(amdc)
            
        data                = {}
        data["ID"]          = self.makeRawIDData(ID=self.bsdata.get("artistID"))
        data["media"]       = self.makeRawMediaData(media=mediaData)
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)