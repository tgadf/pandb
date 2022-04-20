""" Genius Music DB ID """

__all__ = ["MusicDBID"]

from dbid import MusicDBIDBase

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'https://api.genius.com/artists/([\d]+)']
        patterns += [r'http://api.genius.com/artists/([\d]+)']
        patterns += [r'/artists/([\d]+)']
        patterns += [r'artists/([\d]+)']
        self.patterns = patterns
        self.get = self.getArtistID
        self.short = "gen"

    def getArtistID(self, s):
        self.s = str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)