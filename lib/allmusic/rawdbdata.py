""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from mdbbase import RawDataBase
from .musicdbid import MusicDBID
from hashlib import md5
from strUtils import fixName

class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()
        self.getArtistData      = self.getRawData
        self.getSongData        = self.getRawData
        self.getCreditData      = self.getRawData
        self.getCompositionData = self.getRawData
        
        
    ##############################################################################################################################
    ## Parse Data
    ##############################################################################################################################
    def getRawData(self, inputdata):
        self.getPickledHTMLData(inputdata)
        self.assertData()
        data                = {}
        data["artist"]      = self.getName()
        data["meta"]        = self.getMeta()
        data["url"]         = self.getURL()
        data["ID"]          = self.getID(data["url"])
        data["pages"]       = self.getPages()
        data["profile"]     = self.getProfile()
        data["media"]       = self.getMedia(data["url"])
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)
    

    ##############################################################################################################################
    ## Artist Name
    ##############################################################################################################################
    def getName(self):
        artistBios = self.bsdata.findAll("div", {"class": "artist-bio-container"})
        if len(artistBios) > 0:
            for div in artistBios:
                h1 = div.find("h1", {"class": "artist-name"})
                if h1 is not None:
                    artistName = h1.text.strip()
                    if len(artistName) > 0:
                        artist = fixName(artistName)
                        anc = self.makeRawNameData(name=artist, err=None)
                    else:
                        artist = "?"
                        anc = self.makeRawNameData(name=artist, err="Fix")
                else:
                    anc = self.makeRawNameData(err="NoH1")
        else:       
            anc = self.makeRawNameData(err=True)
            return anc
        
        return anc
    
    

    ##############################################################################################################################
    ## Meta Information
    ##############################################################################################################################
    def getMeta(self):
        metatitle = self.bsdata.find("meta", {"property": "og:title"})
        metaurl   = self.bsdata.find("meta", {"property": "og:url"})

        title = None
        if metatitle is not None:
            title = metatitle.attrs['content']

        url = None
        if metatitle is not None:
            url = metaurl.attrs['content']

        amc = self.makeRawMetaData(title=title, url=url)
        return amc
        

    ##############################################################################################################################
    ## Artist URL
    ##############################################################################################################################
    def getURL(self):
        result1 = self.bsdata.find("link", {"rel": "canonical"})
        result2 = self.bsdata.find("link", {"hreflang": "en"})
        if result1 and not result2:
            result = result1
        elif result2 and not result1:
            result = result2
        elif result1 and result2:
            result = result1
        else:        
            auc = self.makeRawURLData(err=True)
            return auc

        if result:
            url = result.attrs["href"]
            url = url.replace("https://www.allmusic.com", "")
            if url.find("/artist/") == -1:
                url = None
                auc = self.makeRawURLData(url=url, err="NoArtist")
            else:
                auc = self.makeRawURLData(url=url)
        else:
            auc = self.makeRawURLData(err="NoLink")

        return auc

    

    ##############################################################################################################################
    ## Artist ID
    ##############################################################################################################################
    def getID(self, suburl):
        discID = self.aid.getArtistID(suburl.url)
        aic = self.makeRawIDData(ID=discID)
        return aic


    
    ##############################################################################################################################
    ## Artist Pages
    ##############################################################################################################################
    def getPages(self):
        apc   = self.makeRawPageData(ppp=1, tot=1, redo=False, more=False)
        return apc
    
    

    ##############################################################################################################################
    ## Artist Variations
    ##############################################################################################################################
    def getProfile(self):
        generalData = None
        genreData   = None
        tagsData    = None
        extraData   = None

        content     = self.bsdata.find("meta", {"name": "title"})
        contentAttr = content.attrs if content is not None else None
        searchTerm  = contentAttr.get("content") if contentAttr is not None else None
        searchData  = [self.makeRawTextData(searchTerm)] if searchTerm is not None else None
        
        tabsul      = self.bsdata.find("ul", {"class": "tabs"})
        #print('tabsul',tabsul)
        refs        = tabsul.findAll("a") if tabsul is not None else None
        #print('refs',refs)
        tabLinks    = [self.makeRawLinkData(ref) for ref in refs] if refs is not None else None
        #print('tabLinks',tabLinks)
        #print('tabLinks',[x.get() for x in tabLinks])
        tabKeys = []
        if isinstance(tabLinks, list):
            for i,tabLink in enumerate(tabLinks):
                keyTitle = tabLink.title
                keyText  = tabLink.text
                if keyTitle is not None:
                    tabKeys.append(keyTitle)
                    continue
                if keyText is not None:
                    key = keyText.replace("\n", "").split()[0]
                    tabKeys.append(key)
                    continue
                tabKeys.append("Tab {0}".format(i))
        else:
            tabKeys = None
            
        tabsData    = dict(zip(tabKeys, tabLinks)) if (isinstance(tabKeys, list) and all(tabKeys)) else None
        #print('tabsData', tabsData)

        if searchData is not None:
            if extraData is None:
                extraData = {}
            extraData["Search"] = searchData
        if tabsData is not None:
            if extraData is None:
                extraData = {}
            extraData["Tabs"] = tabsData
        #print('extraData',extraData)


        basicInfo = self.bsdata.find("section", {"class": "basic-info"})
        if basicInfo is not None:
            for div in basicInfo.findAll("div"):
                attrs = div.attrs.get('class')
                if isinstance(attrs, list) and len(attrs) == 1:
                    attrKey = attrs[0]
                    if attrKey == "genre":
                        refs = div.findAll("a")
                        val  = [self.makeRawTextData(div)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]
                        genreData = val
                    elif attrKey == "styles":
                        refs = div.findAll("a")
                        val  = [self.makeRawTextData(div)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]
                        tagsData = val
                    else:
                        if generalData is None:
                            generalData = {}
                        refs = div.findAll("a")
                        val  = [self.makeRawTextData(div)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]
                        generalData[attrKey] = val

        apc = self.makeRawProfileData(general=generalData, tags=tagsData, genres=genreData, extra=extraData)
        return apc

    
    
    ##############################################################################################################################
    ## Artist Media
    ##############################################################################################################################
    def getMediaAlbum(self, td):
        for span in td.findAll("span"):
            attrs = span.attrs
            if attrs.get("class"):
                if 'format' in attrs["class"]:
                    albumformat = span.text
                    albumformat = albumformat.replace("(", "")
                    albumformat = albumformat.replace(")", "")
                    amac.format = albumformat
                    continue
            span.replaceWith("")

        ref = td.find("a")
        if ref:
            url    = ref.attrs['href']
            album  = ref.text
            retval = self.makeRawURLInfoData(url=url, name=album)
        else:
            retval = self.makeRawURLInfoData(url=None, name=None, err="NoText")

        return retval
    
    
    #def getMediaCredits(self):
    def getMediaSongs(self):
        mediaType = "Songs"
        media  = {}
        tables = self.bsdata.findAll("table")
        for table in tables:
            trs = table.findAll("tr")

            header  = trs[0]
            ths     = header.findAll("th")
            headers = [x.text.strip() for x in ths]
            if len(headers) == 0:
                continue
            for j,tr in enumerate(trs[1:]):
                tds  = tr.findAll("td")
                vals = [td.text.strip() for td in tds]

                tdTitle   = tr.find("td", {"class": "title-composer"})
                divTitle  = tdTitle.find("div", {"class": "title"}) if tdTitle is not None else None
                compTitle = tdTitle.find("div", {"class": "composer"}) if tdTitle is not None else None

                songTitle = divTitle.text if divTitle is not None else None
                songTitle = songTitle.strip() if songTitle is not None else None
                songURL   = divTitle.find('a') if divTitle is not None else None
                songURL   = self.makeRawLinkData(songURL) if songURL is not None else None
                
                if songTitle is None:
                    continue

                songArtists = compTitle.findAll("a") if compTitle is not None else None
                if songArtists is not None:
                    if len(songArtists) == 0:
                        songArtists = [self.makeRawTextData(compTitle.text)]
                    else:
                        songArtists = [self.makeRawLinkData(ref) for ref in songArtists]
                        
                m = md5()
                m.update(str(j).encode('utf-8'))
                if songTitle is not None:
                    m.update(songTitle.encode('utf-8'))
                code = str(int(m.hexdigest(), 16) % int(1e5))

                amdc = self.makeRawMediaReleaseData(album=songTitle, url=songURL, aclass=None, aformat=None, artist=songArtists, code=code, year=None)
                if media.get(mediaType) is None:
                    media[mediaType] = []
                media[mediaType].append(amdc)
                
        return media
        
        
    def getMediaCompositions(self):
        mediaType = "Composition"
        media = {}
        tables = self.bsdata.findAll("table")
        for table in tables:
            trs = table.findAll("tr")

            header  = trs[0]
            ths     = header.findAll("th")
            headers = [x.text.strip() for x in ths]
            if len(headers) == 0:
                continue
            for tr in trs[1:]:
                tds  = tr.findAll("td")
                vals = [td.text.strip() for td in tds]
                if len(vals) == len(headers):
                    albumData = dict(zip(headers,vals))

                    url   = None
                    year  = albumData.get('Year')
                    album = albumData.get('Title')
                    
                    if album is None:
                        continue

                    mediaType = "Composition"
                    for k,v in albumData.items():
                        if k.find("Genre") != -1:
                            mediaType = v

                    m = md5()
                    if year is not None:
                        m.update(year.encode('utf-8'))
                    if album is not None:
                        m.update(album.encode('utf-8'))
                    code = str(int(m.hexdigest(), 16) % int(1e5))

                    amdc = self.makeRawMediaReleaseData(album=album, url=url, aclass=None, aformat=None, artist=None, code=code, year=year)
                    if media.get(mediaType) is None:
                        media[mediaType] = []
                    media[mediaType].append(amdc)
              
        return media
            
                
    def getMedia(self, urlData):
        url  = urlData.url
        
        #print(url,'\t',end="")

        mediaData = {}
        if url is None:
            mediaType = "Unknown"
        else:
            mediaType = "Albums"
            if url.find("/credits") != -1:
                mediaType = "Credits"
            if url.find("/songs") != -1:
                mediaType = "Songs"
            if url.find("/compositions") != -1:
                mediaType = "Compositions"

        name = mediaType
        #print(mediaType)
                
        tables = self.bsdata.findAll("table")
        for table in tables:
            trs = table.findAll("tr")

            header  = trs[0]
            ths     = header.findAll("th")
            headers = [x.text.strip() for x in ths]
            if len(headers) == 0:
                continue

            for tr in trs[1:]:
                tds = tr.findAll("td")
                
                ## Name
                key = "Name"
                try:
                    if len(headers[1]) == 0:
                        idx = 1
                        mediaType = tds[idx].text.strip()
                        #print("From Text:",mediaType)
                        if len(mediaType) == 0:
                            mediaType = name
                            #print("From Name H:",mediaType)
                    else:
                        mediaType = name
                        #print("From Name:",mediaType)
                except:
                    #print("Error getting key: {0} from AllMusic media".format(key))
                    break

                ## Year
                key  = "Year"
                try:
                    idx  = headers.index(key)
                    year = tds[idx].text.strip()
                except:
                    #print("Error getting key: {0} from AllMusic media".format(key))
                    continue

                ## Title
                key   = "Album"
                try:
                    idx   = headers.index(key)
                    ref   = tds[idx].findAll("a")
                except:
                    #print("Error getting key: {0} from AllMusic media".format(key))
                    continue
                    
                try:
                    refdata = ref[0]
                    url     = refdata.attrs['href']
                    album   = refdata.text.strip()
                    
                    data = url.split("/")[-1]
                    pos  = data.rfind("-")
                    discIDurl = data[(pos+3):]       
                    discID = discIDurl.split("/")[0]

                    try:
                        int(discID)
                        code = discID
                    except:
                        code = None
                except:
                    url  = None
                    code = None
                    continue

                amdc = self.makeRawMediaReleaseData(album=album, url=url, aclass=None, aformat=None, artist=None, code=code, year=year)
                if mediaData.get(mediaType) is None:
                    mediaData[mediaType] = []
                mediaData[mediaType].append(amdc)

                
        compMedia = self.getMediaCompositions()
        for mediaType,mediaTypeData in compMedia.items():
            if mediaData.get(mediaType) is None:
                mediaData[mediaType] = []
            mediaData[mediaType] += mediaTypeData
            

        songMedia = self.getMediaSongs()
        for mediaType,mediaTypeData in songMedia.items():
            if mediaData.get(mediaType) is None:
                mediaData[mediaType] = []
            mediaData[mediaType] += mediaTypeData
                
        return self.makeRawMediaData(media=mediaData)