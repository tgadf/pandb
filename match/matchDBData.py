from masterDBGate import masterDBGate
from masterDBData import masterDBData

class matchDBData:
    def __init__(self, mmeDF, dtype, dbs=None):
        print("="*25,"matchDBData(dtype={0}, dbs={1})".format(dtype, dbs),"="*25)
        self.mmeDF   = mmeDF
        self.mdbGate = masterDBGate()
        
        if dtype not in ["User"]:
            if isinstance(dbs,list):
                dbsToMatch = dbs
            elif isinstance(dbs,str):
                dbsToMatch = [dbs]
            else:
                dbsToMatch = self.mdbGate.getDBs()
            self.dbsToMatch = [db for db in dbsToMatch if self.mdbGate.isValid(db)]
            self.dbs        = self.dbsToMatch
            self.mdbData    = masterDBData(dtype=dtype, dbs=self.dbs)
        else:
            if isinstance(dbs,str):
                dbs     = [dbs]
            else:
                raise ValueError("User db name must be a string")
            self.dbs = dbs
            self.mdbData = {db: {} for db in self.dbs}
            
            
        
    def setMDBData(self, mdbData):
        self.mdbData    = mdbData
        self.dbs        = mdbData.dbs
        self.dbsToMatch = mdbData.dbs
        self.mdbData.prepareSearchData(self.mmeDF)
        print("  Set mdbData with DBs: {0}".format(self.dbs))
            
    def loadArtists(self, **kwargs):
        numAlbumsReq = {db: 2 if db in ['Discogs', 'MusicBrainz', 'AllMusic'] else 1 for db in self.dbs}
        numAlbumsReq = {db: kwargs.get(db, numAlbumsReq[db]) for db in self.dbs}
        for db,num in numAlbumsReq.items():
            if num > 1:
                print("  Setting Min numAlbums[{0}] = {1}".format(db,num))
        self.mdbData.loadArtists(numAlbumsReq=numAlbumsReq)
        self.mdbData.prepareSearchData(self.mmeDF)
            
    def loadAlbums(self, idxReq={}):
        self.mdbData.loadAlbums(idxReq=idxReq)