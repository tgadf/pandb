""" Spotify Music DB ID """

__all__ = ["MusicDBID"]

from dbid import MusicDBIDBase

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        self.get = self.getArtistID

    def getArtistID(self, s):
        return s