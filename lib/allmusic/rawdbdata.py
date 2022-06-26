""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from base import RawDataBase
from .musicdbid import MusicDBID
import json
from bs4 import element

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
        
        self.getBasicData()
        #return self.bsdata
        
        data                = {}
        data["artist"]      = self.getName()
        data["meta"]        = self.getMeta()
        data["url"]         = self.getURL()
        data["ID"]          = self.getID()
        data["pages"]       = self.getPages()
        data["profile"]     = self.getProfile()
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)

    
    ##############################################################################################################################
    ## Basic Data
    ##############################################################################################################################
    def getBasicData(self):
        scriptData = self.bsdata.find("script", {"type": "application/ld+json"})
        if isinstance(scriptData, element.Tag):
            try:
                jData = json.loads(scriptData.text)
            except:
                jData = {}

            self.name   = jData.get('name')
            self.type   = jData.get('@type')
            self.url    = jData.get('url')
            self.image  = jData.get('image')
            self.descr  = jData.get('description')
            self.member = jData.get('member', [])
        else:
            ## Name
            h1 = self.bsdata.find("h1", {"class": "artist-name"})
            self.name = h1.text.replace("\n", "").strip() if isinstance(h1, element.Tag) else None
            
            ## Type
            self.type = None
            
            ## URL
            metaurl   = self.bsdata.find("meta", {"property": "og:url"})
            self.url   = metaurl.attrs['content'] if isinstance(metaurl, element.Tag) else None
            
            ## Image
            self.image = None
            
            ## Description
            self.descr  = None
            
            ## Member
            self.member = []
    

    ##############################################################################################################################
    ## Artist Name
    ##############################################################################################################################
    def getName(self):
        anc = self.makeRawNameData(name=self.name, err=None)
        return anc
    

    ##############################################################################################################################
    ## Meta Information
    ##############################################################################################################################
    def getMeta(self):
        metatitle = self.bsdata.find("meta", {"property": "og:title"})
        metaurl   = self.bsdata.find("meta", {"property": "og:url"})

        title = metatitle.attrs['content'] if isinstance(metatitle, element.Tag) else None
        url   = metaurl.attrs['content'] if isinstance(metaurl, element.Tag) else None
        amc   = self.makeRawMetaData(title=title, url=url)
        return amc
        

    ##############################################################################################################################
    ## Artist URL
    ##############################################################################################################################
    def getURL(self):
        auc = self.makeRawURLData(url=self.url)
        return auc
    

    ##############################################################################################################################
    ## Artist ID
    ##############################################################################################################################
    def getID(self):
        aic = self.makeRawIDData(ID=self.aid.getArtistID(self.url))
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
        generalData  = {}
        tagsData     = {}
        extraData    = {}
        genreData    = {}
        
        #### Tabs
        tabsUL = self.bsdata.find("ul", {"class", "tabs"})
        if isinstance(tabsUL, element.Tag):
            refs   = [tab.find('a') for tab in tabsUL.findAll("li", {"class", "tab"})]
            tabs   = [self.makeRawLinkData(ref) for ref in refs if isinstance(ref, element.Tag)]
            extraData["tabs"] = tabs
        
        #### Related Artists
        relatedSec   = self.bsdata.find("section", {"class": "related-artists"})
        if isinstance(relatedSec, element.Tag):
            relatedDivs  = [li.find('div', {"class": "artist"}) for li in relatedSec.findAll("li")]
            relatedRefs  = [div.find('a') for div in relatedDivs if isinstance(div, element.Tag)]
            relatedLinks = [self.makeRawLinkData(ref) for ref in relatedRefs if isinstance(ref, element.Tag)]
            extraData["related"] = relatedLinks
                
        #### Moods
        moodsSec  = self.bsdata.find("section", {"class": "moods"})
        moodLinks = []
        if isinstance(moodsSec, element.Tag):
            moodsRefs = [li.find('a') for li in moodsSec.findAll("li")]
            moodLinks = [self.makeRawLinkData(ref) for ref in moodsRefs if isinstance(ref, element.Tag)]
            generalData['moods'] = moodLinks
        
        #### Themes
        themesSec   = self.bsdata.find("section", {"class": "themes"})
        if isinstance(themesSec, element.Tag):
            themesRefs  = [li.find('a') for li in themesSec.findAll("li")]
            themesLinks = [self.makeRawLinkData(ref) for ref in themesRefs if isinstance(ref, element.Tag)]
            generalData['themes'] = themesLinks
        
        #### Basic
        basicSec  = self.bsdata.find("section", {"class": "basic-info"})
        basicDivs = basicSec.findAll("div") if isinstance(basicSec, element.Tag) else []
        for div in basicDivs:
            attrs = div.attrs.get('class')
            if isinstance(attrs, list) and len(attrs) == 1:
                attrKey = attrs[0]
                if attrKey == "genre":
                    genreData = [self.makeRawTextData(div)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in div.findAll("a")]
                elif attrKey == "styles":
                    tagsData = [self.makeRawTextData(div)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in div.findAll("a")]
                else:
                    generalData[attrKey]  = [self.makeRawTextData(div)] if len(refs) == 0 else [self.makeRawLinkData(ref) for ref in div.findAll("a")]
                
        apc = self.makeRawProfileData(general=generalData, tags=tagsData, genres=genreData, extra=extraData)
        return apc