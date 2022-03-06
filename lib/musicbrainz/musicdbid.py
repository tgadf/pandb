""" Discogs Music DB ID """

__all__ = ["MusicDBID"]

from dbid import MusicDBIDBase
import re

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://musicbrainz.org/artist/([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        patterns += [r'/artist/([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        patterns += [r'artist/([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        patterns += [r'([\w]+)-([\w]+)-([\w]+)-([\w]+)-([\w]+)']
        self.patterns = patterns
        self.get = self.getArtistID
        
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