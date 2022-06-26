""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from base import RawDataBase
from .musicdbid import MusicDBID
from strUtils import fixName
from htmlUtils import removeTag

class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()
        
        
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
        data["ID"]          = self.getID(data["artist"])
        data["pages"]       = self.getPages()
        data["profile"]     = self.getProfile()
        data["media"]       = self.getMedia(data["artist"], data["url"])
        #print(data["media"].get())
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.getInfo()
        #return data
        return self.makeRawData(**data)
    

    ##############################################################################################################################
    ## Artist Name
    ##############################################################################################################################
    def getName(self):
        artistData = self.bsdata.find("h1", {"class": "artist_name_hdr"})
        if artistData is None:
            anc = self.makeRawNameData(err="No H1")
            return anc
        
        span = artistData.find("span")
        if span is None:
            artistName = artistData.text.strip()
            artistNativeName = artistName
        else:
            artistName = span.text.strip()
            artistData = removeTag(artistData, span)
            artistNativeName = artistData.text.strip() #.replace(artistName, "").strip()
            
        if len(artistName) > 0:
            artistName = fixName(artistName)
            artistNativeName = fixName(artistNativeName)
            
            if artistName.endswith("]"):
                artistName = artistName.split(" [")[0].strip()
            if artistNativeName.endswith("]"):
                artistNativeName = artistNativeName.split(" [")[0].strip()
            
            anc = self.makeRawNameData(name=artistName, native=artistNativeName, err=None)
        else:
            anc = self.makeRawNameData(name=artistName, err="Fix")
        
        return anc
    
    

    ##############################################################################################################################
    ## Meta Information
    ##############################################################################################################################
    def getMeta(self):
        metatitle = self.bsdata.find("meta", {"property": "og:title"})
        metaurl   = self.bsdata.find("meta", {"property": "og:url"})

        title = metatitle.attrs['content'] if metatitle is not None else None
        url = metaurl.attrs['content'] if metatitle is not None else None
            
        amc = self.makeRawMetaData(title=title, url=url)
        return amc
        

    ##############################################################################################################################
    ## Artist URL
    ##############################################################################################################################
    def getURL(self):
        artistData = self.bsdata.find("meta", {"property": "og:url"})
        if artistData is None:
            auc = self.makeRawURLData(err=True)
            return auc
        
        url = artistData.attrs["content"]
        if url.find("/artist/") == -1:
            url = None
            auc = self.makeRawURLData(url=url, err="NoArtist")
        else:
            auc = self.makeRawURLData(url=url)

        return auc

    

    ##############################################################################################################################
    ## Artist ID
    ##############################################################################################################################
    def getID(self, artist):
        discID = self.aid.getArtistID(self.bsdata)
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
    def getAlsoKnownAs(self, tag):
        if tag is None:
            return None
#        {'tag': <div class="info_content"><span class="rendered_text">Dwight David Turner [birth name], <a class="artist" href="/artist/dwight_david" title="[Artist864564]">Dwight David</a>, Spider Turner</span></div>}        
        span = tag.getTag().find("span", {"class": "rendered_text"})
        retval = []
        if span is not None:
            refs = span.findAll("a")
            for ref in refs:
                result = self.makeRawLinkData(ref)
                retval.append(result)
                span = removeTag(span, ref)
                
            for result in span.text.split(","):
                retval.append(self.makeRawTextData(result.strip()))
        else:
            refs = tag.getTag().findAll("a")
            if len(refs) == 0:
                try:
                    retval.append(self.makeRawTextData(tag.getTag().strip()))
                except:
                    pass
            else:
                for ref in refs:
                    result = self.makeRawLinkData(ref)
                    retval.append(result)
        return retval
    
    def getProfile(self):
        profile = self.bsdata.find("div", {"class": "artist_info"})
        if profile is None:
            apc = self.makeRawProfileData(err="NoInfo")
            return apc
        
        headers = profile.findAll("div", {"class": "info_hdr"})
        headers = [header.text for header in headers]
        content = profile.findAll("div", {"class": "info_content"})
        profileData = dict(zip(headers, content))

        generalData  = {}
        extraData    = None
        genreData    = None
        externalData = None
        
        rymList = self.bsdata.find("ul", {"class": "lists"})
        listInfos = rymList.findAll("div", {"class": "list_info"}) if rymList is not None else []
        userLists = []
        for userList in listInfos:
            listName = userList.find("div", {"class": "list_name"})
            listUser = userList.find("div", {"class": "list_author"})
            listRef  = listName.find("a")
            if listRef is not None:
                userLists.append(self.makeRawLinkData(listRef))
            continue
            listRef  = listName.find("a") if listName is not None else None
            listText = listRef.text if listRef is not None else None
            listRef  = listRef.attrs['href'] if listRef is not None else None
            userLists[listRef] = listText
        externalData = {"Lists": userLists} if len(userLists) > 0 else None

        
        if profileData.get("Formed") is not None:
            tag = profileData["Formed"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Formed"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Disbanded") is not None:
            tag = profileData["Disbanded"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Disbanded"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Members") is not None:
            tag = profileData["Members"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Members"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Also Known As"):
            tag = profileData["Also Known As"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Also Known As"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Member of"):
            tag = profileData["Member of"]
            if tag is not None:
                refs = tag.findAll("a")
                extraData = {}
                extraData["Member of"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Related Artists"):
            tag = profileData["Related Artists"]
            if tag is not None:
                refs = tag.findAll("a")
                extraData = {}
                extraData["Related Artists"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Born"):
            tag = profileData["Born"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Born"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Died"):
            tag = profileData["Died"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Died"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Currently"):
            tag = profileData["Currently"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Currently"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Genres") is not None:
            tag  = profileData["Genres"]
            if tag is not None:
                refs = tag.findAll("a")
                genreData  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        if profileData.get("Notes"):
            tag   = profileData["Notes"]
            if tag is not None:
                refs = tag.findAll("a")
                generalData["Notes"]  = [self.makeRawTextData(tag)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in refs]

        generalData = generalData if len(generalData) > 0 else None
                
        apc = self.makeRawProfileData(general=generalData, genres=genreData, extra=extraData, external=externalData)
        return apc
    
    
    ##############################################################################################################################
    ## Artist Media
    ##############################################################################################################################
    def getMediaAlbum(self, td):
        amac = self.makeRawMediaAlbumData()
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
            amac.url   = ref.attrs['href']
            amac.album = ref.text
        else:
            amac.err = "NoText"

        return amac
    
    
    def getCreditsMedia(self, artist, url):
        artistCredits = self.bsdata.find("div", {"class": "section_artist_credits"})
        if artistCredits is None:
            return {}

        discInfos = artistCredits.findAll("div", {"class": "disco_release"})
        media = {}
        mediaType = "Credits"

        for albumdata in discInfos:

            ## Code
            codedata = albumdata.attrs['id']
            code     = codedata.split("_")[-1]
            try:
                int(code)
            except:
                code = None

            ## Title
            mainline = albumdata.find("div", {"class": "disco_mainline"})
            ref = mainline.find('a')
            if ref is not None:
                maindata  = self.makeRawLinkData(ref)
                album     = maindata.text
                albumurl  = maindata.href
            else:
                maindata  = self.makeRawTextData(mainline)
                album     = maindata.text
                albumurl  = None
                    

            ## Year
            yeardata = albumdata.find("span", {"class": "disco_year_y"})
            if yeardata is None:
                yeardata = albumdata.find("span", {"class": "disco_year_ymd"})
            year = yeardata.text if yeardata is not None else None
                

            ## Artists        
            artistdata   = albumdata.findAll("span")[-1]
            ref = artistdata.find('a')
            albumartist  = self.makeRawURLInfoData(name=artist.name, url=url.url) if ref is None else self.makeRawLinkData(ref)


            amdc = self.makeRawMediaReleaseData(album=album, url=albumurl, aclass=None, aformat=None, artist=albumartist, code=code, year=year)
            if media.get(mediaType) is None:
                media[mediaType] = []
            media[mediaType].append(amdc)    
            
        return media
            
    
    def getClassicalMedia(self, artist, url):
        artistWorks = self.bsdata.find("div", {"class": "section_artist_works"})
        if artistWorks is None:
            return {}
        
        media = {}
        
        uls = artistWorks.findAll("ul")
        mediaType   = None
        for i,ul in enumerate(uls):
            for j,li in enumerate(ul.findAll("li")):
                if 'work_header' in li.attrs.get('class', []):
                    mediaType = li.text
                    continue

                ## Code
                codedata = li.find("span", {"class": "work_numbers"})
                code = None
                if codedata is not None:
                    code = codedata.text

                    
                ## Title
                mainline = li.find("span", {"class": "work_title"})
                ref = mainline.find('a')
                if ref is not None:
                    maindata  = self.makeRawLinkData(ref)
                    album     = maindata.text
                    albumurl  = maindata.href
                else:
                    maindata  = self.makeRawTextData(mainline)
                    album     = maindata.text
                    albumurl  = None


                ## Year
                yeardata = li.find("span", {"class": "work_date"})
                year = yeardata.text if yeardata is not None else None
                    

                ## Artists        
                albumartist  = self.makeRawURLInfoData(name=artist.name, url=url.url)
                
                #print(mediaType,'\t',code,'\t',album,'\t',year)
                
                amdc = self.makeRawMediaReleaseData(album=album, url=albumurl, aclass=None, aformat=None, artist=albumartist, code=code, year=year)
                if media.get(mediaType) is None:
                    media[mediaType] = []
                media[mediaType].append(amdc)
                
        return media
    
                
    def getMedia(self, artist, url):
        mediaData = {}
        n = 0

        mediadatas = self.bsdata.findAll("div", {"id": "discography"})
        for mediadata in mediadatas:
            h3s        = mediadata.findAll("h3", {"class": "disco_header_label"})
            categories = [x.text for x in h3s]
            
            sufs    = mediadata.findAll("div", {"class": "disco_showing"})
            spans   = [x.find("span") for x in sufs]
            ids     = [x.attrs['id'] for x in spans]
            letters = [x[-1] for x in ids]


            for mediaType,suffix in dict(zip(categories, letters)).items():
                categorydata = mediadata.find("div", {"id": "disco_type_{0}".format(suffix)})
                albumdatas   = categorydata.findAll("div", {"class": "disco_release"})
                for albumdata in albumdatas:
                    
                    ## Code
                    codedata = albumdata.attrs['id']
                    code     = codedata.split("_")[-1]
                    try:
                        int(code)
                    except:
                        code = None
                        
                        
                    ## Title
                    mainline  = albumdata.find("div", {"class": "disco_mainline"})
                    ref = mainline.find('a')
                    if ref is not None:
                        maindata  = self.makeRawLinkData(ref)
                        album     = maindata.text
                        albumurl  = maindata.href
                    else:
                        maindata  = self.makeRawTextData(mainline)
                        album     = maindata.text
                        albumurl  = None


                    ## Year
                    yeardata = albumdata.find("span", {"class": "disco_year_y"})
                    if yeardata is None:
                        yeardata = albumdata.find("span", {"class": "disco_year_ymd"})                    
                    year = yeardata.text if yeardata is not None else None
                        

                    ## Artists        
                    artistdata   = albumdata.findAll("span")[-1]
                    ref = artistdata.find('a')
                    albumartist  = self.makeRawURLInfoData(name=artist.name, url=url.url) if ref is None else self.makeRawLinkData(ref)
                    

                    amdc = self.makeRawMediaReleaseData(album=album, url=albumurl, aclass=None, aformat=None, artist=albumartist, code=code, year=year)
                    if mediaData.get(mediaType) is None:
                        mediaData[mediaType] = []
                    mediaData[mediaType].append(amdc)
                    #if self.debug:
                    #    print("Found Album: [{0}/{1}] : {2}  /  {3}".format(len(amc.media[mediaType]), mediaType, code, album, album))
        
        classicalMedia = self.getClassicalMedia(artist, url)
        if len(classicalMedia) > 0:
            mediaData.update(classicalMedia)
        creditsMedia = self.getCreditsMedia(artist, url)
        if len(creditsMedia) > 0:
            mediaData.update(creditsMedia)
        return self.makeRawMediaData(media=mediaData)

        return amc