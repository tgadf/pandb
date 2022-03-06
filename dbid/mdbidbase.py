""" Collection of classes to get db artist IDs """

__all__ = ["MusicDBIDBase", "MusicDBID"]
         
from masterArtistNameCorrection import masterArtistNameCorrection
from master import MasterParams
from . import MusicDBIDModVal
from listUtils import getFlatList

from bs4 import BeautifulSoup,element
from hashlib import md5
from numpy import power
from urllib.parse import urlparse,quote
import re


###########################################################################################################################################
## Artist ID I/O Class
###########################################################################################################################################
class MusicDBID:
    def __init__(self, db):
        #try:
        #    self.aid = eval("dbArtistID{0}()".format(db))
        #except:
        #    self.aid = eval("dbArtistIDSelf()")
        self.aid = None
        self.amv = MusicDBIDModVal()
        self.getModVal = self.amv.getModVal

    def getid(self, value):
        return self.aid.getArtistID(value)

    def getpsid(self, value):
        return self.aid.getArtistPseudoID(value)
    
    
###########################################################################################################################################
## Artist ID Base Class
###########################################################################################################################################
class MusicDBIDBase:
    def __init__(self, debug=False):
        self.debug = debug
        self.err   = None

    def extractID(self, sval):
        groups   = None if sval is None else sval.groups()
        artistID = None if groups is None else str(groups[0])
        return artistID

    def extractGroups(self, sval):
        groups   = None if sval is None else sval.groups()
        return groups

    def testFormat(self, s):
        self.err = None
        if s is None:
            self.err = "None"            
        elif not isinstance(s, str):
            self.err = type(s)            
            
    def quoteIt(self, name):
        retval = name if "%" in name else quote(name)
        return retval   

    def getErr(self):
        return self.err
    
    def getHashval(self, vals):
        if not isinstance(vals, list):
            raise ValueError("Must pass list of values. You passed [{0}]".format(vals))
        m = md5()
        for val in vals:
            try:
                enc = val.encode('utf-8')
            except:
                continue
            m.update(enc)
        hashval = m.hexdigest()
        return hashval    
    
    def getIDFromHash(self, hashval, expo):
        iHash    = int(hashval, 16)
        artistID = str(iHash) if expo < 1 else str(iHash % power(10,expo))
        return artistID
    
    def getArtistPseudoID(self, s):
        return s

    def getArtistIDFromPatterns(self, s, patterns):
        s = str(s)

        ######################################################    
        ## Test For Format
        ######################################################
        self.testFormat(s)
        if self.err is not None:
            return None

        ######################################################    
        ## Pattern Matching
        ######################################################
        for pattern in patterns:
            artistID = self.extractID(re.search(pattern, s))
            if artistID is not None:
                return artistID

        self.err = "NoMatch"
        return None

    
###########################################################################################################################################
## Self
###########################################################################################################################################
class MusicDBIDBaseDummy(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)

    def getArtistID(self, s):
        return s    

    
###########################################################################################################################################
## Discogs
###########################################################################################################################################
class dbArtistIDDiscogs(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://www.discogs.com/artist/([\d]+)-([^/?]+)']
        patterns += [r'https://www.discogs.com/artist/([\d]+)']
        patterns += [r'/artist/([\d]+)']
        patterns += [r'artist/([\d]+)']
        patterns += [r'([\d]+)-([^/?]+)']
        patterns += [r'([\d]+)']
        self.patterns = patterns

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    
    
###########################################################################################################################################
### AllMusic
###########################################################################################################################################
class dbArtistIDAllMusic(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://www.allmusic.com/artist/mn([\d]+)-([^/?]+)']
        patterns += [r'https://www.allmusic.com/artist/mn([\d]+)']
        patterns += [r'/artist/mn([\d]+)']
        patterns += [r'artist/mn([\d]+)']
        patterns += [r'mn([\d]+)-([^/?]+)']
        patterns += [r'mn([\d]+)']
        self.patterns = patterns

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    

###########################################################################################################################################
### RateYourMusic
###########################################################################################################################################
class dbArtistIDRateYourMusic(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'\[Artist([\d]+)\]']
        patterns += [r'Artist([\d]+)']
        self.patterns = patterns
        
    def getBS4Input(self, bsdata):
        ipt = bsdata.find("input", {"class": "rym_shortcut"})
        ipt = bsdata.find("input", {"class": "album_shortcut"}) if ipt is None else ipt
        value = ipt.get('value', "") if isinstance(ipt,element.Tag) else ""
        return value        

    def getArtistID(self, s):
        self.s = self.getBS4Input(s) if isinstance(s, BeautifulSoup) else str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    
    
###########################################################################################################################################
## Deezer
###########################################################################################################################################
class dbArtistIDDeezer(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://www.deezer.com/artist/([\d]+)']
        patterns += [r'/artist/([\d]+)']
        patterns += [r'artist/([\d]+)']
        patterns += [r'([\d]+)']
        self.patterns = patterns
        self.manc     = masterArtistNameCorrection()

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    
    def getArtistPseudoID(self, s):
        self.s = self.manc.clean(str(s))
        
        ######################################################    
        ## Get Hash
        ######################################################
        hashval  = self.getHashval([self.s])
        artistID = self.getIDFromHash(hashval, 12)
        return artistID
        
    
###########################################################################################################################################
## Spotify
###########################################################################################################################################
class dbArtistIDSpotify(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://www.deezer.com/artist/([\w]+)']
        patterns += [r'http://www.deezer.com/artist/([\w]+)']
        patterns += [r'/artist/([\w]+)']
        patterns += [r'artist/([\w]+)']
        patterns += [r'([\w]+)']
        self.patterns = patterns
        self.manc = masterArtistNameCorrection()

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    
    def getArtistPseudoID(self, s):
        self.s = self.manc.clean(str(s))
        
        ######################################################    
        ## Get Hash
        ######################################################
        hashval  = self.getHashval([self.s])
        artistID = self.getIDFromHash(hashval, 12)
        return artistID
    
    
###########################################################################################################################################
## Soundcloud
###########################################################################################################################################
class dbArtistIDSoundcloud(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://soundcloud.com/([\w]+)']
        patterns += [r'https://www.soundcloud.com/([\w]+)']
        patterns += [r'http://soundcloud.com/([\w]+)']
        patterns += [r'http://www.soundcloud.com/([\w]+)']
        patterns += [r'([\w]+)']
        self.patterns = patterns

    def getArtistID(self, s):
        self.s = str(s)
        
        ######################################################    
        ## Test For Format
        ######################################################
        self.testFormat(s)
        if self.err is not None:
            return None
        
        for pattern in self.patterns:
            groups = self.extractGroups(re.search(pattern, s))
            if isinstance(groups, (list,tuple)) and len(groups) > 0:
                return groups[0]
        return None
    
    
###########################################################################################################################################
## Tidal
###########################################################################################################################################
class dbArtistIDTidal(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://listen.tidal.com/artist/([\w]+)']
        patterns += [r'http://listen.tidal.com/artist/([\w]+)']
        patterns += [r'/artist/([\w]+)']
        patterns += [r'artist/([\w]+)']
        patterns += [r'([\w]+)']
        self.patterns = patterns

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    
    
###########################################################################################################################################
## AlbumOfTheYear
###########################################################################################################################################
class dbArtistIDAlbumOfTheYear(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)        
        patterns  = [r'https://www.albumoftheyear.org/artist/([\d]+)-([^/?]+)']
        patterns += [r'https://www.albumoftheyear.org/artist/([\d]+)']
        patterns += [r'/artist/([\d]+)-([^/?]+)']
        patterns += [r'artist/([\d]+)-([^/?]+)']
        patterns += [r'/artist/([\d]+)']
        patterns += [r'artist/([\d]+)']
        patterns += [r'([\d]+)']
        self.patterns = patterns

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    
    
###########################################################################################################################################
## MusicBrainz
###########################################################################################################################################
class dbArtistIDMusicBrainz(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://musicbrainz.org/artist/([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        patterns += [r'/artist/([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        patterns += [r'artist/([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        patterns += [r'([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        self.patterns = patterns
        
    def getArtistID(self, s):
        self.s = str(s)
        
        ######################################################    
        ## Test For Format
        ######################################################
        self.testFormat(s)
        if self.err is not None:
            return None

        ######################################################    
        ## Pattern Matching
        ######################################################
        for pattern in self.patterns:
            groups = self.extractGroups(re.search(pattern, s))
            if groups is not None:
                ######################################################    
                ## Get Hash
                ######################################################
                hashval  = self.getHashval(list(groups))
                artistID = self.getIDFromHash(hashval, 0)
                return artistID
    
    
###########################################################################################################################################
## LastFM
###########################################################################################################################################
class dbArtistIDLastFM(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        self.patterns = None
        self.manc     = masterArtistNameCorrection()
        
    def getArtistID(self, s):
        self.s = str(s)
            
        ######################################################    
        ## Test For Format
        ######################################################
        self.testFormat(s)
        if self.err is not None:
            return None
        
        up   = urlparse(s)
        if up.scheme not in ['http', 'https']:
            self.err = 'Not http(s)'
            return None
        if up.netloc not in ['www.last.fm']:
            self.err = 'Not URL'
            return None
        path = up.path
        if not path.startswith("/music/"):
            self.err = "Unknown path"
            return None
        
        try:
            artistName = path.split("/")[2]
        except:
            self.err = "Path split error"
            return None
        
        asciiArtistName = self.quoteIt(artistName)
        
        
        ######################################################    
        ## Get Hash
        ######################################################
        hashval  = self.getHashval([asciiArtistName])
        artistID = self.getIDFromHash(hashval, 11)
        return artistID
    
    def getArtistPseudoID(self, s):
        self.s = self.manc.clean(str(s))
        
        ######################################################    
        ## Get Hash
        ######################################################
        hashval  = self.getHashval([self.s])
        artistID = self.getIDFromHash(hashval, 13)
        return artistID
        
    def getAlbumID(self, s):
        self.s = str(s)
            
        ######################################################    
        ## Test For Format
        ######################################################
        self.testFormat(s)
        if self.err is not None:
            return None
        
        up   = urlparse(s)
        if up.scheme not in ['http', 'https']:
            self.err = 'Not http(s)'
            return None
        if up.netloc not in ['www.last.fm']:
            self.err = 'Not URL'
            return None
        path = up.path
        if not path.startswith("/music/"):
            self.err = "Unknown path"
            return None
        
        try:
            albumNames = path.split("/")[2:]
        except:
            self.err = "Path split error"
            return None
        
        asciiAlbumNames = [self.quoteIt(x) for x in albumNames]
        
        
        ######################################################    
        ## Get Hash
        ######################################################
        hashval  = self.getHashval(asciiAlbumNames)
        albumID = self.getIDFromHash(hashval, 11)
        return albumID
            
            
###########################################################################################################################################
## LastFM Webpage
###########################################################################################################################################
class dbArtistIDLastFMWebpage(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        self.baseURL = "https://www.last.fm/music/"
        self.relURL  = "/music/"
        self.patterns = None
        
    def quoteIt(self, href, debug=False):
        retval = href
        if href is not None:
            retval = href if "+" not in href else quote("+".join([quote(x) for x in href.split("+")]))
        return retval
        
    def getArtistID(self, s):
        self.s = str(s)
        
        ######################################################    
        ## Test For Format
        ######################################################
        self.testFormat(s)
        if self.err is not None:
            return None
        
        href   = self.s        
        if href.startswith(self.baseURL):
            href = href[len(self.baseURL):]        
        if href.startswith(self.relURL):
            href = href[len(self.relURL):]
        name = href.split("/+albums")[0]
        if name.endswith("/"):
            name = href[:-1]
        if name is None:
            return None
        name = "{0}{1}".format(self.baseURL,name)
        
        ######################################################    
        ## Get Hash
        ######################################################
        hashval  = self.getHashval(name.split(" "))
        artistID = self.getIDFromHash(hashval, 11)
        return artistID