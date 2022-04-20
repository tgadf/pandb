""" Discogs Music DB ID """

__all__ = ["MusicDBID"]

from dbid import MusicDBIDBase

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://www.discogs.com/artist/([\d]+)-([^/?]+)']
        patterns += [r'https://www.discogs.com/artist/([\d]+)']
        patterns += [r'/artist/([\d]+)']
        patterns += [r'artist/([\d]+)']
        patterns += [r'([\d]+)-([^/?]+)']
        patterns += [r'([\d]+)']
        self.patterns = patterns
        self.get = self.getArtistID
        self.short = "disc"

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)