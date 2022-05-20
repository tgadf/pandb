""" Matching Parameters Class """

__all__ = ["MatchDBParams"]

from .req import MatchReq

class MatchDBParams:
    def __init__(self, reqs, **kwargs):
        self.verbose = kwargs.get('verbose', True)
        self.reqs    = reqs
            
        self.useDask = kwargs.get('dask', False)
        self.usePool = kwargs.get('pool', True)
        assert self.useDask | self.usePool, "dask or pool must be set"
            
        mask = reqs.get("Mask")
        assert isinstance(mask,(tuple,bool,str)), "Mask Req is not set."
        self.mask = mask
        
        mediaTypes = reqs.get("Media")
        assert isinstance(mediaTypes,list) or mediaTypes is None, "Media Req is not set."
        self.mediaTypes = mediaTypes
        
        mreqs = reqs.get("Reqs")
        assert isinstance(mreqs,dict), "Albums Req is not set."
        self.mreqs = mreqs
        
        self.nPart = reqs.get("NPart", 2)
        assert isinstance(self.nPart,int), "NPart Req is not set."
        
        chunkSize = reqs.get("ChunkSize", 1000)
        assert isinstance(chunkSize, int), "ChunkSize must be an integer"
        self.chunkSize = chunkSize
        
        self.matchReqs = reqs.get("Match")
        assert isinstance(self.matchReqs,dict), "Match Req is not set."
        assert isinstance(self.matchReqs.get('Artist'),float), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Medium'),int), "Artist match req is not set"
        assert isinstance(self.matchReqs.get('Tight'),int), "Artist match req is not set"
        
    def getChunkSize(self):
        return self.chunkSize
    
    def getMask(self):
        return self.mask
        
    def getPart(self):
        return self.nPart
        
    def getMatchNameReq(self):
        return self.matchReqs['Artist']
        
    def validDB(self, db):
        assert isinstance(self.mreqs.get(db),MatchReq), "Reqs does not have DB [{0}]".format(db)
        
    def getDBReq(self, db):
        return self.mreqs[db]