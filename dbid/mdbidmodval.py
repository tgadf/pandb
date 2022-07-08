""" Classes to get db artist mod value """

__all__ = ["MusicDBIDModVal"]
         
from master import MasterParams
from hashlib import md5, sha1

###########################################################################################################################################
## Artist ID ModVal Class
###########################################################################################################################################
class MusicDBIDModVal:
    def __init__(self):
        self.maxModVal = MasterParams().getMaxModVal()
        
    def getGlobVal(self, dbid):
        if isinstance(dbid, str) and not dbid.isdigit():
            m = sha1()
            m.update(dbid.encode('utf-8'))
            hashval = m.hexdigest()
            iHash = int(hashval, 16)
            modValue = iHash % self.maxModVal
            return modValue
        else:
            raise TypeError("This can only be called for non-digit IDs")

    def get(self, dbid):
        if isinstance(dbid, str):
            if dbid.isdigit():
                modValue = int(dbid) % self.maxModVal
            else:
                m = md5()
                m.update(dbid.encode('utf-8'))
                hashval = m.hexdigest()
                iHash = int(hashval, 16)
                modValue = iHash % self.maxModVal
        elif isinstance(dbid, int):
            modValue = dbid % self.maxModVal
        elif dbid is None:
            modValue = None
        else:
            raise ValueError("Can not get mod value for [{0}]".format(dbid))

        return modValue