""" Spotify Music DB ID """

__all__ = ["MusicDBID"]

from dbid import MusicDBIDBase
from utils import MusicDBArtistName
from urllib.parse import urlparse

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        self.patterns = None
        self.manc     = MusicDBArtistName()
        self.get      = self.getArtistID
        
    def getArtistID(self, s, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.s = str(s)
        if verbose: print("="*150)
        if verbose: print('  ==>',s)
            
        ######################################################    
        ## Test For Format
        ######################################################
        self.testFormat(s)
        if self.err is not None:
            return None
        
        up   = urlparse(s)
        #print('params   :',up.params)
        #print('fragment :',up.fragment)
        #print('hostname :',up.hostname)
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
            artistPathName = path.split("/")[2]
        except:
            self.err = "Path split error"
            return None
        
        artistNames = artistPathName.split(";")
        extraNames  = up.params.split(";")
        if verbose: print('  Name       :',artistNames)
        if verbose: print('  Extra      :',extraNames)
        asciiArtistNames = [self.quoteIt(artistName) for artistName in artistNames]
        extraArtistNames = [self.quoteIt(artistName) for artistName in extraNames]
        if verbose: print('  Ascii Name :',asciiArtistNames)
        if verbose: print('  Ascii Extra:',extraArtistNames)
        
        
        ######################################################    
        ## Get Hash
        ######################################################
        hashValData = asciiArtistNames+extraArtistNames
        if verbose: print('  HashData   :',hashValData)
        hashval  = self.getHashval(hashValData, addSize=True)
        artistID = self.getIDFromHash(hashval, 14)
        if verbose: print('  ArtistID   :',artistID)
        return artistID
    
    def getArtistIDName(self, s):
        retval = "".join([c for c in self.manc.clean(str(s)).upper() if c.isalnum()])
        return retval
        
    
    def getArtistPseudoID(self, s):
        self.s = self.getArtistIDName(s)
        hashval  = self.getHashval([self.s])
        artistID = self.getIDFromHash(hashval, 14)
        return artistID
    
    
    def getArtistPseudoIDOld(self, s):
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