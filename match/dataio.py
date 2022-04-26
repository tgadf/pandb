""" Data I/O Class Used For Matching DBs """

__all__ = ["MatchDBDataIO"]

from master import MasterMetas
from gate import IOStore
from musicdb import PanDBIO
from listUtils import getFlatList
from .albumreq import AlbumReq
from .utils import write
from pandas import DataFrame, concat, Index, Series
from typing import Union    

class MatchDBDataIO:
    def __init__(self, db, mediaTypes=None, mask=True, **kwargs):
        self.mask    = mask
        assert isinstance(self.mask,(bool,str)), "MatchDBDataIO mask must be bool|str"
        self.verbose = kwargs.get("verbose", True)
        self.dynamic = kwargs.get("dynamic", False)
        self.isBase  = kwargs.get("base", False)
        
        ios  = IOStore()
        self.mdbio = ios.get(db=db)
        self.db = db
        
        self.mediaTypes = mediaTypes if isinstance(mediaTypes,list) else list(MasterMetas().getMedias().values())
        assert isinstance(self.mediaTypes,list), "MediaTypes must be a list"
                
        self.possibleIDs    = None
        self.possibleIDDF   = None
        self.crossCheck     = False
        self.crossCheckIDs  = None
        self.crossCheckIDDF = None
        
        self.namesData      = None
        self.mediaData      = None
        self.mediaMatchData = None
        
        if self.verbose: print("  MatchDBData({0}):".format(db))
            
    ############################################################################################################################################
    # Utils
    ############################################################################################################################################
    def setCrossCheck(self, value: bool):
        self.crossCheck = value
        
        
    ############################################################################################################################################
    # Load Name, Counts, Media, DB Data
    ############################################################################################################################################
    def loadBasicData(self):
        self.basicData = DataFrame(self.mdbio.data.getSearchNameData()).join(self.mdbio.data.getSummaryNumAlbumsData()).sort_values(by="NumAlbums", ascending=False)
        if self.verbose: write(4, "Found {0: >7} Artists Basic Data In {1} DB", (self.basicData.shape[0], self.mdbio.db))
        
    def loadCountsData(self):
        countsData = self.mdbio.data.getSummaryCountsData()
        #countsData = countsData[[name.replace("Media","") for name in self.mediaTypes]]
        countsData = countsData.rename(columns={col: "Num{0}".format(col) for col in countsData.columns})
        countsData["NumMedia"] = countsData.sum(axis=1)
        self.countsData = countsData
        
    def loadMediaDataX(self):
        mediaData = []
        for mediaType in self.mediaTypes:
            mediaData.append(eval("self.mdbio.data.getSearch{0}MediaData()".format(mediaType)))  ## This does not work with list comprehension
        self.mediaData = concat(mediaData, axis=1)
        if self.verbose: write(4, "Found {0: >7} Artists Media Data In {1} DB", (self.mediaData.shape[0], self.mdbio.db))
            
            

    ############################################################################################################################################
    # Load Joined Names Data
    ############################################################################################################################################
    def loadNames(self):
        if self.verbose: write(2,"Loading Names Data")
        self.loadBasicData()
        self.loadCountsData()
        
        if isinstance(self.mask,str) or (isinstance(self.mask,bool) and self.mask is True):
            if self.verbose: write(6,"Masking Out Previously Found Artists")
            pdbio  = PanDBIO()
            pdbio.setData()
            if isinstance(self.mask,str):
                knownData = pdbio.getNotNaDBIDs(self.mdbio.db)
                knownIDs  = knownData[knownData[self.mask].notna()][self.mdbio.db]
            elif self.mask is True:
                knownData = pdbio.getNotNaDBIDs(self.mdbio.db)
                knownIDs  = knownData[self.mdbio.db]
            else:
                raise TypeError("Unknown type for mask")
            if self.verbose: write(4, "Found {0: >7} Previously Cross Matched Artists", knownIDs.shape[0])
            
            self.basicData  = self.basicData[~self.basicData.index.isin(knownIDs)]
            if self.verbose: write(4, "Found {0: >7} Artists In {1} DB", (self.basicData.shape[0], self.mdbio.db))
            
            del pdbio
            
        self.namesData = self.basicData.join(self.countsData)
        if self.verbose: write(4, "Found {0: >7} Available Artists In {1} DB", (self.namesData.shape[0], self.mdbio.db))
        
            
        del self.basicData
        del self.countsData
        
    def getNumNames(self):
        if self.crossCheck is False:
            assert isinstance(self.possibleIDs, (list,Index)), "loadNames() must be called"
            return len(self.possibleIDs)
        else:
            assert isinstance(self.crossCheckIDs, (list,Index)), "loadNames() must be called"
            return len(self.crossCheckIDs)
    
    def getAvailableNames(self):
        assert isinstance(self.namesData, DataFrame), "loadNames() must be called"
        if self.crossCheck is False:
            assert isinstance(self.possibleIDDF, DataFrame), "setAvailableNames() must be called"
            return self.possibleIDDF.join(self.namesData)["Name"]
        else:
            assert isinstance(self.crossCheckIDDF, DataFrame), "setAvailableNames() must be called"
            return self.crossCheckIDDF.join(self.namesData)["Name"]
        
    def setAvailableNames(self, req: Union[AlbumReq,Index,list]):
        assert isinstance(self.namesData, DataFrame), "loadNames() must be called"
        if self.crossCheck is False:
            if isinstance(req, AlbumReq):
                numMedia = self.namesData["NumMedia"]
                write(4, "{0: >7} Available Artists With [{1}/{2}] Min/Max Albums", (numMedia.count(), numMedia.min(), numMedia.max()))
                self.possibleIDs  = req.valid(self.namesData["NumMedia"])
                self.possibleIDDF = DataFrame(index=self.possibleIDs)
                numMedia = self.namesData.loc[self.possibleIDs]["NumMedia"]
                write(4, "{0: >7} Possible Artists With [{1}/{2}] Min/Max Albums", (numMedia.count(), numMedia.min(), numMedia.max()))
        else:
            if isinstance(req, AlbumReq):
                self.crossCheckIDs  = req.valid(self.namesData["NumMedia"])
                self.crossCheckIDDF = DataFrame(index=self.crossCheckIDs)
            elif isinstance(req, (Index,list)):
                self.crossCheckIDs  = req
                self.crossCheckIDDF = DataFrame(index=self.crossCheckIDs)
        
        
        
    ############################################################################################################################################
    # Load Joined Media Data
    ############################################################################################################################################
    def loadMedia(self, ids=None):
        def flattenMediaData(row: Series) -> 'dict':
            rowMediaValues = {mediaType: row.get("{0}Media".format(mediaType),[]) for mediaType in self.mediaTypes}
            rowMediaValues = getFlatList([mediaTypeData for mediaTypeData in rowMediaValues.values() if isinstance(mediaTypeData,list)])
            return rowMediaValues
        
        if isinstance(self.mediaData,DataFrame):
            return
        mediaData = None
        for mediaType in self.mediaTypes:
            searchMediaData = eval("self.mdbio.data.getSearch{0}MediaData()".format(mediaType))
            if isinstance(searchMediaData,Series):
                #print(mediaType,'\t',searchMediaData.shape)
                searchMediaData = searchMediaData[ids] if isinstance(ids, (list,Index)) else searchMediaData
                #print(mediaType,'\t',searchMediaData.shape)
                mediaData = DataFrame(searchMediaData) if mediaData is None else mediaData.join(searchMediaData)
                #print(mediaType,'\t',mediaData.shape)
                #print("")
        assert isinstance(mediaData, DataFrame),"Could not find any media!"
        #print(mediaData.head())
        self.mediaData = mediaData.apply(lambda row: flattenMediaData(row), axis=1)
        self.mediaData.name = "{0}Media".format(self.db)
        #self.mediaData = concat(mediaData, axis=1)
        if self.verbose: write(4, "Found {0: >7} Artists Media Data In {1} DB", (self.mediaData.shape[0], self.mdbio.db))
            
    
    def getAvailableMedia(self) -> 'DataFrame':
        assert isinstance(self.mediaData, Series), "loadMedia() must be called"
        #print("getAvailableMedia:",self.db)
        if self.crossCheck is False:
            #print("getAvailableMedia:",self.db,'possible',len(self.possibleIDs))
            retval = self.mediaData[self.possibleIDs] if self.isBase is True else self.mediaData
        else:
            #print("getAvailableMedia:",self.db,'crossCheck',len(self.crossCheckIDs))
            retval = self.mediaData if self.isBase is True else self.mediaData[self.crossCheckIDs]
        return retval
                
            

    ############################################################################################################################################
    # Load Joined Names Data
    ############################################################################################################################################
    def loadData(self):
        if isinstance(self.mediaMatchData,DataFrame):
            return
            
        if self.verbose: write(2,"Loading Data")
        self.loadBasicData()
        self.loadCountsData()
        self.loadMediaData()
            
        if isinstance(self.mask,str) or (isinstance(self.mask,bool) and self.mask is True):
            if self.verbose: write(6,"Masking Out Previously Found Artists")
            pdbio  = PanDBIO()
            pdbio.setData()
            if isinstance(self.mask,str):
                knownData = pdbio.getNotNaDBIDs(self.mdbio.db)
                knownIDs  = knownData[knownData[self.mask].notna()][self.mdbio.db]
            elif self.mask is True:
                knownData = pdbio.getNotNaDBIDs(self.mdbio.db)
                knownIDs  = knownData[self.mdbio.db]
            else:
                raise TypeError("Unknown type for mask")
            if self.verbose: write(4, "Found {0: >7} Previously Cross Matched Artists", knownIDs.shape[0])
            
            self.basicData  = self.basicData[~self.basicData.index.isin(knownIDs)]
            if self.verbose: write(4, "Found {0: >7} Artists In {1} DB", (self.basicData.shape[0], self.mdbio.db))
            
            del pdbio
            
        self.mediaMatchData = self.basicData.join(self.countsData).join(self.mediaData)
        
        
    ############################################################################################################################################
    # Get Data We Want
    ############################################################################################################################################
        
        if albums is not None:
            assert isinstance(albums,AlbumReq),"albums must be of class AlbumReq"
        ids      = kwargs.get("ids")
        if ids is not None:
            assert isinstance(ids,list),"ids must be a list"
        verbose  = kwargs.get("verbose", self.verbose)
        verbose  = True
        
    def getDataX(self, **kwargs):
        albums   = kwargs.get("albums")
        if albums is not None:
            assert isinstance(albums,AlbumReq),"albums must be of class AlbumReq"
        ids      = kwargs.get("ids")
        if ids is not None:
            assert isinstance(ids,list),"ids must be a list"
        verbose  = kwargs.get("verbose", self.verbose)
        verbose  = True

        if isinstance(ids,list):
            if verbose: write(2, "MatchDBData({0}).getData(ids={1}):", (self.db,len(ids)))
            self.loadData()
            retval = self.mediaMatchData.loc[ids]
        elif isinstance(albums,AlbumReq):
            if verbose: write(2, "MatchDBData({0}).getData(AlbumReq)", self.db)
            self.loadData()
            ids = albums.valid(self.mediaMatchData["NumMedia"])
            retval = self.mediaMatchData.loc[ids]
        else:
            if verbose: write(2, "MatchDBData({0}).getData():", self.db)
            self.loadData()
            retval = self.mediaMatchData

        if verbose:
            write(4, "{0: >7} Artists To Match <==", retval.shape[0])
            write(4, "{0: >7} Max Albums", retval["NumMedia"].max())
            write(4, "{0: >7} Min Albums", retval["NumMedia"].min())
        
        return retval