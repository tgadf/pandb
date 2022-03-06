""" Raw Data Storage Class """

__all__ = ["MusicDBArtistName"]

from strutils import NormalizeString
from pandas import isna,notna

class MusicDBArtistName:
    def __init__(self, debug=False):
        self.debug = debug
        self.cbs = NormalizeString()

        ## Test
        assert self.clean("3/3") == "3-3"
        assert self.clean("...Hello") == "Hello"
        

    def directoryName(self, x):
        if not isinstance(x,str):
            return x
        if "..." in x:
            x = x.replace("...", "")
        if "/" in x:
            x = x.replace("/", "-")
        return x


    def realName(self, x):
        if x is None:
            return [None,-1]

        lenx = len(x)
        if len(x) < 1:
            return [x,-1]

        if x[-1] != ")":
            return [x, None]


        if lenx >=5:
            if x[-3] == "(":
                try:
                    num = int(x[-2:-1])
                    val = x[:-3].strip()
                    return [val, num]
                except:
                    return [x, None]

        if lenx >= 6:
            if x[-4] == "(":
                try:
                    num = int(x[-3:-1])
                    val = x[:-4].strip()
                    return [val, num]
                except:
                    return [x, None]

        if lenx >= 7:
            if x[-4] == "(":
                try:
                    num = int(x[-3:-1])
                    val = x[:-4].strip()
                    return [val, num]
                except:
                    return [x, None]

        return [x, None]

    
    def discConv(self, x):
        if not isinstance(x,str):
            return ""
        x = x.replace("/", "-")
        x = x.replace("¡", "")
        while x.startswith(".") and len(x) > 1:
            x = x[1:]
        x = x.strip()
        x = self.cbs.convert(x)
        return x

    
    def cleanMB(self, x):
        pos = [x.rfind("(")+1, x.rfind(")")]
        if sum([p > 0 for p in pos]) != len(pos):
            return x
        parval = x[pos[0]:pos[1]]
        return x[:pos[0]-2].strip()  
        
        
    def clean(self, name):
        if self.debug:
            print("Pre Cleaning  [{0}]".format(name))
        name = self.discConv(name)
        if self.debug:
            print("Post Cleaning [{0}]".format(name))
        return name