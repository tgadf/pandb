""" Discogs Music DB ID """

__all__ = ["MusicDBID"]

from mdbid import MusicDBIDBase

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
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
        self.get = self.getArtistID

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
    
    def getAlbumID(self, **kwargs):
        return self.getHashval(list(kwargs.values()))