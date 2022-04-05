""" Albums Selection Class """

__all__ = ["AlbumReq"]

from pandas import Series

class AlbumReq:
    def __init__(self, **kwargs):
        self.minAlbums = kwargs.get("min")
        self.maxAlbums = kwargs.get("max")
        self.topAlbums = kwargs.get("top")
        self.qntAlbums = kwargs.get("qnt")
        
        assert(any([x is not None for x in [self.minAlbums,self.maxAlbums,self.topAlbums,self.qntAlbums]])), "Need to specify album quantity"
        
        
    def validAlbums(self, numAlbums: Series) -> 'Series':
        if isinstance(self.topAlbums,int):
            idx = numAlbums.rank(ascending=False) <= self.topAlbums
            return idx
        if isinstance(self.minAlbums,int) or isinstance(self.maxAlbums,int):
            if (self.minAlbums,int) and isinstance(self.maxAlbums,int):
                idx = ((numAlbums < self.maxAlbums) & (numAlbums >= self.minAlbums))
                return idx
            elif (self.minAlbums,int):
                idx = numAlbums >= self.minAlbums
                return idx
            elif (self.maxAlbums,int):
                idx = numAlbums < self.maxAlbums
                return idx
        if isinstance(self.qntAlbums,float):
            idx = numAlbums >= numAlbums.quantile(self.qntAlbums)
            return idx
            
        raise TypeError("Could not determine how to select albums")