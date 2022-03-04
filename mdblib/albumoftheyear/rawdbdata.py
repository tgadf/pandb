""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from mdbbase import RawDataBase, RawTextData, RawLinkData
from .musicdbid import MusicDBID
import regex
from strUtils import fixName
from htmlUtils import removeTag

class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()
        self.debug = debug
        
        
    ##############################################################################################################################
    ## Parse Data
    ##############################################################################################################################
    def getArtistData(self, inputdata):
        self.getPickledHTMLData(inputdata)
        self.assertData()
        data                = {}
        data["artist"]      = self.getName()
        data["meta"]        = self.getMeta()
        data["url"]         = self.getURL()
        data["ID"]          = self.getID(data["url"])
        data["pages"]       = self.getPages()
        data["profile"]     = self.getProfile()
        data["media"]       = self.getMedia(data["artist"])
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)
    
    

    ###########################################################################################################################
    ## Artist Name
    ###########################################################################################################################
    def getName(self):
        h1 = self.bsdata.find("h1", {"class": 'artistHeadline'})
        artistName = h1.text if h1 is not None else None
        if artistName is not None:
            bracketValues = regex.findall(r'\[(.*?)\]+', artistName)
            if len(bracketValues) > 0:
                ignores = ['rap', '2', '3', '4', 'NOR', 'US', 'unknown Artist', 'CHE', 'email\xa0protected', '70s', '60s', '80s', '90s', 'BRA', 'SWE', 'France', 'FR', 'UK', 'JP', 'DE', 'USA', 'RUS', 'ARG', 'DEU']
                for ignore in ignores:
                    arg = " [{0}]".format(ignore)
                    if arg in artistName:
                        artistName = artistName.replace(arg, "")
                bracketValues = regex.findall(r'\[(.*?)\]+', artistName)
                
            artistName = " & ".join(bracketValues) if len(bracketValues) > 0 else artistName
            anc = self.makeRawNameData(name=artistName, err=None)
            return anc
        else:
            script = self.bsdata.find("script", {"type": "application/ld+json"})
            if script is None:
                anc = self.makeRawNameData(name=None, err = "NoJSON")
                return anc

            try:
                artist = json.loads(script.contents[0])["name"]
            except:
                anc = self.makeRawNameData(name=None, err = "CouldNotCompileJSON")
                return anc

            artistName = artist
            bracketValues = regex.findall(r'\[(.*?)\]+', artistName)
            if len(bracketValues) > 0:
                ignores = ['rap', '2', '3', '4', 'NOR', 'US', 'unknown Artist', 'CHE', 'email\xa0protected', '70s', '60s', '80s', '90s', 'BRA', 'SWE', 'France', 'FR', 'UK', 'JP', 'DE', 'USA', 'RUS', 'ARG', 'DEU']
                for ignore in ignores:
                    arg = " [{0}]".format(ignore)
                    if arg in artistName:
                        artistName = artistName.replace(arg, "")
                bracketValues = regex.findall(r'\[(.*?)\]+', artistName)
                
            artistName = " & ".join(bracketValues) if len(bracketValues) > 0 else artistName
            anc = self.makeRawNameData(name=artistName, err=None)
            return anc

    

    ###########################################################################################################################
    ## Meta Information
    ###########################################################################################################################
    def getMeta(self):
        metatitle = self.bsdata.find("meta", {"property": "og:title"})
        metaurl   = self.bsdata.find("meta", {"property": "og:url"})

        title = None
        err = None
        if metatitle is not None:
            try:
                title = metatitle.attrs['content']
            except:
                title = None
                err = "NoTitle"

        url = None
        if metatitle is not None:
            try:
                url = metaurl.attrs['content']
            except:
                url = None
                err = "NoURL"

        amc = self.makeRawMetaData(title=title, url=url, err=err)
        return amc
        

    ###########################################################################################################################
    ## Artist URL
    ###########################################################################################################################
    def getURL(self):
        metalink = self.bsdata.find("meta", {"property": "og:url"})
        if metalink is None:
            auc = self.makeRawURLData(err="NoLink")
            return auc
        
        try:
            url = metalink.attrs["content"]
        except:
            auc = self.makeRawURLData(err="NoContent")
            return auc

        auc = self.makeRawURLData(url=url)
        return auc

    

    ###########################################################################################################################
    ## Artist ID
    ###########################################################################################################################
    def getID(self, url):
        artistID = self.aid.getArtistID(url.url)
        aic = self.makeRawIDData(ID=artistID)
        return aic


    
    ###########################################################################################################################
    ## Artist Pages
    ###########################################################################################################################
    def getPages(self):
        tot = 1
        apc   = self.makeRawPageData(ppp=1, tot=tot, redo=False, more=False)
        return apc
    
    

    ###########################################################################################################################
    ## Artist Variations
    ###########################################################################################################################
    def getProfile(self):      
        generalData = {}
        genreData   = None
        extraData   = None
        tagsData    = None
        
        artistInfo = self.bsdata.find("div", {"class": "artistTopBox info"})
        detailRows = artistInfo.findAll("div", {"class": "detailRow"}) if artistInfo is not None else []
        for row in detailRows:
            span = row.find("span")    
            if span is None:
                continue
            key  = span.text.strip() if span.text is not None else None
            key  = key[1:].strip() if (isinstance(key,str) and key.startswith("/")) else key
            refs = row.findAll("a")
            if len(refs) == 0:
                continue
            vals = [self.makeRawLinkData(ref) for ref in refs] if (isinstance(refs, list) and len(refs) > 0) else None

            if key == "Genres":
                genreData = vals
            else:
                generalData[key] = vals
                
                
        relatedArtists = self.bsdata.find("div", {"class": "relatedArtists"})
        artistBlocks   = relatedArtists.findAll("div", {"class": "artistBlock"}) if relatedArtists is not None else None
        refs           = [artistBlock.find("a") for artistBlock in artistBlocks] if artistBlocks is not None else None
        if refs is not None:
            extraData = [self.makeRawLinkData(ref) for ref in refs if ref is not None]        
                
                
        generalData = generalData if len(generalData) > 0 else None
                
        apc = self.makeRawProfileData(general=generalData, genres=genreData, tags=tagsData, extra=extraData)
        return apc

    
    
    ###########################################################################################################################
    ## Artist Media
    ########################################################################################################################### 
    def getMedia(self, artist):
        media = {}
        artistName = artist.name
        albumBlocks = self.bsdata.findAll("div", {"class": "albumBlock"})
        for i,albumBlock in enumerate(albumBlocks):
            #print(i,'/',len(albumBlocks))
            blockData = {}
            for div in albumBlock.findAll("div"):
                attr = div.attrs.get("class")
                key  = attr[0] if isinstance(attr,list) else None
                ref  = div.find("a")
                val  = self.makeRawLinkData(ref) if ref is not None else self.makeRawTextData(div)
                blockData[key] = val

            urlData = blockData.get("image")
            url = urlData.href if isinstance(urlData, RawLinkData) else None

            titleData = blockData.get("albumTitle")
            title = titleData.text if isinstance(titleData, RawTextData) else None

            yearData = blockData.get("date")
            year = yearData.text if isinstance(yearData, RawTextData) else None

            mediaTypeData = blockData.get("type")
            mediaType = mediaTypeData.text if isinstance(mediaTypeData, RawTextData) else None

            code = self.aid.getAlbumID(name=title, url=url)
            amdc = self.makeRawMediaReleaseData(album=title, url=url, aclass=None, aformat=None, artist=artistName, code=code, year=year)
            
            if media.get(mediaType) is None:
                media[mediaType] = []
            media[mediaType].append(amdc)
            #if self.debug:
            #    print("\t\tAdding Media ({0} -- {1})".format(title, url))

        return self.makeRawMediaData(media=media)