""" Spotify Music DB ID """

__all__ = ["MusicDBID"]

from dbid import MusicDBIDBase
from utils import MusicDBArtistName

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        self.patterns = None
        self.manc     = MusicDBArtistName()
        self.get      = self.getArtistID
        
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

    def getArtistID(self, s):
        return s