""" RateYourMusic Music DB ID """

__all__ = ["MusicDBID"]

from mdbid import MusicDBIDBase
from bs4 import BeautifulSoup, element

###########################################################################################################################################
## Genius
###########################################################################################################################################
class MusicDBID(MusicDBIDBase):
    def __init__(self, debug=False):
        super().__init__(debug)
        patterns  = [r'\[Artist([\d]+)\]']
        patterns += [r'Artist([\d]+)']
        self.patterns = patterns
        self.get = self.getArtistID
        
    def getBS4Input(self, bsdata):
        ipt = bsdata.find("input", {"class": "rym_shortcut"})
        ipt = bsdata.find("input", {"class": "album_shortcut"}) if ipt is None else ipt
        value = ipt.get('value', "") if isinstance(ipt,element.Tag) else ""
        return value

    def getArtistID(self, s):
        self.s = self.getBS4Input(s) if isinstance(s, BeautifulSoup) else str(s)
        return self.getArtistIDFromPatterns(self.s, self.patterns)
