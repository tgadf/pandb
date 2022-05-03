""" Albums Selection Class """

__all__ = ["MatchReq", "AlbumReq", "NameReq"]

from pandas import Series
from typing import Union

class MatchReq:
    def __init__(self, *reqs):
        self.namereq = None
        self.albumreq = None
        for req in reqs:
            if isinstance(req,NameReq):
                self.namereq  = req
            elif isinstance(req,AlbumReq):
                self.albumreq = req
            else:
                raise TypeError("Unknown object [{0}] passed to MatchReq".format(req))
        
    def valid(self, names: Series = None, albums: Series = None, **kwargs):
        verbose = kwargs.get('verbose', False)
        cuts  = {}
        index = None
        if isinstance(albums,Series) and isinstance(self.albumreq,AlbumReq):
            albumIndex,albumCuts = self.albumreq.valid(albums)
            cuts.update(albumCuts)
            index = albumIndex if index is None else index.intersection(albumIndex)
        if isinstance(names,Series) and isinstance(self.namereq,NameReq):
            nameIndex,nameCuts = self.namereq.valid(names)
            cuts.update(nameCuts)
            index = nameIndex if index is None else index.intersection(nameIndex)
        assert index is not None, "Must have some matching requirements..."
            
        cuts["Final"] = (len(index), "", "")
        return index,cuts

                                                               
                                                               
class NameReq:
    def __init__(self, **kwargs):
        self.minLen = kwargs.get("min")
        self.maxLen = kwargs.get("max")
        
        assert(any([isinstance(x,int) for x in [self.minLen,self.maxLen]])), "Need to specify name quantity"
        

    def valid(self, names: Series) -> 'Series':
        cuts      = {}
        nameLen   = names.str.len()
        cuts["Names"] = (nameLen.shape[0], nameLen.min(), nameLen.max())
        maxLen = self.maxLen if isinstance(self.maxLen,int) else maxLen.max()+1
        minLen = self.minLen if isinstance(self.minLen,int) else minLen.min()

        idx = (nameLen < maxLen) & (nameLen >= minLen)

        select = nameLen[idx]
        retval = select.index
        cuts["NamesLen"] = (select.shape[0], select.min(), select.max())
        return retval,cuts
    
        
class AlbumReq:
    def __init__(self, **kwargs):
        self.minAlbums = kwargs.get("min")
        self.maxAlbums = kwargs.get("max")
        self.topAlbums = kwargs.get("top")
        self.rndAlbums = kwargs.get("rnd")
        
        assert(any([isinstance(x,int) for x in [self.minAlbums,self.maxAlbums,self.topAlbums,self.rndAlbums]])), "Need to specify album quantity"
        

    def valid(self, numAlbums: Series) -> 'Series':
        cuts      = {}
        albums    = numAlbums.sort_values(ascending=False)
        cuts["Media"] = (numAlbums.shape[0], numAlbums.min(), numAlbums.max())
        maxAlbums = self.maxAlbums if isinstance(self.maxAlbums,int) else numAlbums.max()+1
        minAlbums = self.minAlbums if isinstance(self.minAlbums,int) else numAlbums.min()

        idx = (albums < maxAlbums) & (albums >= minAlbums)

        select = albums[idx]
        retval = select.index
        cuts["MediaLen"] = (select.shape[0], select.min(), select.max())
        if isinstance(self.rndAlbums, int):
            n = min([self.rndAlbums, select.shape[0]])
            sample = select.sample(n=n)
            cuts["Final"] = (sample.shape[0], sample.min(), sample.max())
            retval = sample.index
        elif isinstance(self.topAlbums, int):
            top = select.head(self.topAlbums)
            cuts["Final"] = (top.shape[0], top.min(), top.max())
            retval = top.index
        return retval,cuts