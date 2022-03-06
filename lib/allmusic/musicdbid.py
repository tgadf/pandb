""" Genius Music DB ID """

__all__ = ["MusicDBID"]

from dbid import MusicDBIDBase

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://www.allmusic.com/artist/mn([\d]+)-([^/?]+)']
        patterns += [r'https://www.allmusic.com/artist/mn([\d]+)']
        patterns += [r'/artist/mn([\d]+)']
        patterns += [r'artist/mn([\d]+)']
        patterns += [r'mn([\d]+)-([^/?]+)']
        patterns += [r'mn([\d]+)']
        self.patterns = patterns
        self.get = self.getArtistID

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)