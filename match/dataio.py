""" Data I/O Class Used For Matching DBs """

__all__ = ["MatchDBDataIO"]

from gate import MusicDBGate
from musicdb import MusicDBIO as PanDBIO
from .albumreq import AlbumReq
from pandas import DataFrame, concat

class MatchDBDataIO:
    def __init__(self, db, mediaTypes, **kwargs):
        self.mask    = kwargs.get("mask", False)
        assert isinstance(self.mask,(bool,str)), "MatchDBDataIO mask must be bool|str"
        self.verbose = kwargs.get("verbose", True)
        
        gate  = MusicDBGate()
        self.mdbio = gate.getIO(db=db)
        self.db = db
        
        assert isinstance(mediaTypes,list), "MediaTypes must be a list"
        self.mediaTypes = mediaTypes
        
        self.mediaMatchData = None
        
        if self.verbose: print("  MatchDBData({0}):".format(db))
        
        
    def loadData(self):
        if isinstance(self.mediaMatchData,DataFrame):
            return
        mdbio = self.mdbio
        if self.verbose: print("  ==> Loading Data")
        basicData = DataFrame(mdbio.data.getSearchNameData()).join(mdbio.data.getSummaryNumAlbumsData()).sort_values(by="NumAlbums", ascending=False)
        if self.verbose: print("   Found {0: >7} Artists Basic Data In {1} DB".format(basicData.shape[0], mdbio.db))
            
        countsData = mdbio.data.getSummaryCountsData()
        countsData = countsData[[name.replace("Media","") for name in self.mediaTypes]]
        countsData = countsData.rename(columns={col: "Num{0}".format(col) for col in countsData.columns})
        countsData["NumMedia"] = countsData.sum(axis=1)

        mediaData = []
        for mediaType in self.mediaTypes:
            mediaData.append(eval("mdbio.data.getSearch{0}Data()".format(mediaType)))
        mediaData = concat(mediaData, axis=1)
        #mediaData["NumMedia"] = concat([mediaData[mediaType].apply(lambda media: len(media) if isinstance(media,list) else 0) for mediaType in self.mediaTypes], axis=1).sum(axis=1)
        if self.verbose: print("   Found {0: >7} Artists Media Data In {1} DB".format(mediaData.shape[0], mdbio.db))
            
        if isinstance(self.mask,str) or (isinstance(self.mask,bool) and self.mask is True):
            if self.verbose: print("     ==> Masking Out Previously Found Artists")
            pdbio  = PanDBIO()
            pdbio.setData()
            if isinstance(self.mask,str):
                knownData = pdbio.getNotNaDBIDs(mdbio.db)
                knownIDs  = knownData[knownData[self.mask].notna()][mdbio.db]
            elif self.mask is True:
                knownData = pdbio.getNotNaDBIDs(mdbio.db)
                knownIDs  = knownData[mdbio.db]
            else:
                raise TypeError("Unknown type for mask")
            if self.verbose: print("   Found {0: >7} Previously Cross Matched Artists".format(knownIDs.shape[0]))
            
            basicData  = basicData[~basicData.index.isin(knownIDs)]
            if self.verbose: print("   Found {0: >7} Artists Basic Data In {1} DB".format(basicData.shape[0], mdbio.db))
            mediaData  = mediaData[~mediaData.index.isin(knownIDs)]
            if self.verbose: print("   Found {0: >7} Artists Media Data In {1} DB".format(basicData.shape[0], mdbio.db))
            
            del pdbio
            
        self.mediaMatchData = basicData.join(countsData).join(mediaData)
        
        
    def getData(self, **kwargs):
        albums   = kwargs.get("albums")
        if albums is not None:
            assert isinstance(albums,AlbumReq),"albums must be of class AlbumReq"
        ids      = kwargs.get("ids")
        if ids is not None:
            assert isinstance(ids,list),"ids must be a list"
        verbose  = kwargs.get("verbose", self.verbose)
        verbose  = True

        if isinstance(ids,list):
            if verbose: print("  MatchDBData({0}).getData(ids={1}):".format(self.db,len(ids)))
            self.loadData()
            retval = self.mediaMatchData.loc[ids]
        elif isinstance(albums,AlbumReq):
            if verbose: print("  MatchDBData({0}).getData(AlbumReq)".format(self.db))
            self.loadData()
            retval = self.mediaMatchData[albums.validAlbums(self.mediaMatchData["NumMedia"])]
        else:
            if verbose: print("  MatchDBData({0}).getData():".format(self.db))
            self.loadData()
            retval = self.mediaMatchData

        if verbose:
            print("      ==> {0: >7} Artists To Match <==".format(retval.shape[0]))
            print("      ==> {0: >7} Max Albums".format(retval["NumMedia"].max()))
            print("      ==> {0: >7} Min Albums".format(retval["NumMedia"].min()))
        
        return retval