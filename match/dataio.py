""" Data I/O Class Used For Matching DBs """

__all__ = ["MatchDBDataIO"]

from master import MasterMetas
from gate import IOStore
from musicdb import PanDBIO
from listUtils import getFlatList
from .req import MatchReq
from .utils import write
from pandas import DataFrame, concat, Index, Series
from typing import Union    

class MatchDBDataIO:
    def __init__(self, db, mediaTypes=None, mask=True, **kwargs):
        self.mask    = mask
        assert isinstance(self.mask,(tuple,bool,str)), "MatchDBDataIO mask must be bool|str"
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
            
            

    ############################################################################################################################################
    # Load Joined Names Data
    ############################################################################################################################################
    def loadNames(self):
        if self.verbose: write(2,"Loading Names Data")
        self.loadBasicData()
        self.loadCountsData()
        
        if isinstance(self.mask,str) or (isinstance(self.mask,bool) and self.mask is True) or (isinstance(self.mask,tuple)):
            if self.verbose: write(6,"Masking Artists")
            pdbio  = PanDBIO()
            pdbio.setData()
            if isinstance(self.mask,tuple):
                # require match of 1st entry
                # require no match of 2nd entry
                knownData = pdbio.getNotNaDBIDs(self.mask[0])
                knownIDs  = knownData[self.mask[0]]
                if self.verbose: write(4, f"Found {knownIDs.shape[0]: >7} Previously Matched {self.mask[0]} Artists")
                self.basicData  = self.basicData[self.basicData.index.isin(knownIDs)]
                if self.verbose: write(4, f"Found {self.basicData.shape[0]: >7} Available Artists In {self.mask[0]} DB")
                knownIDs  = knownData[knownData[self.mask[1]].isna()][self.mdbio.db]
                if self.verbose: write(4, f"Found {knownIDs.shape[0]: >7} Previously Matched {self.mask[0]} Artists Without A {self.mask[1]} Match")
                self.basicData  = self.basicData[self.basicData.index.isin(knownIDs)]
            else:
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
        
    def setAvailableNames(self, req: Union[MatchReq,Index,list]):
        assert isinstance(self.namesData, DataFrame), "loadNames() must be called"
        if self.crossCheck is False:
            if isinstance(req, MatchReq):
                self.possibleIDs,cuts = req.valid(names=self.namesData["Name"], albums=self.namesData["NumMedia"])
                self.possibleIDDF = DataFrame(index=self.possibleIDs)
                numMedia = self.namesData.loc[self.possibleIDs]["NumMedia"]
                for cutkey,cutval in cuts.items():
                    write(4, "{0: <20}{1: >7} Artists With [{2}/{3}] Min/Max", ("Cut[{0}]".format(cutkey), cutval[0], cutval[1], cutval[2]))
            else:
                raise TypeError(f"Unknown req type: {type(req)}")
        else:
            if isinstance(req, MatchReq):
                self.crossCheckIDs,cuts = req.valid(names=self.namesData["Name"], albums=self.namesData["NumMedia"])
                self.crossCheckIDDF = DataFrame(index=self.crossCheckIDs)
                print(cuts)
            elif isinstance(req, (Index,list)):
                self.crossCheckIDs  = req
                self.crossCheckIDDF = DataFrame(index=self.crossCheckIDs)
            else:
                raise TypeError(f"Unknown req type: {type(req)}")
        
        
        
    ############################################################################################################################################
    # Load Joined Media Data
    ############################################################################################################################################
    def loadMedia(self, ids=None, **kwargs):
        allowMissing = kwargs.get('allowMissing', False)
        def flattenMediaData(row: Series) -> 'dict':
            rowMediaValues = {mediaType: row.get("{0}Media".format(mediaType),[]) for mediaType in self.mediaTypes}
            rowMediaValues = getFlatList([mediaTypeData for mediaTypeData in rowMediaValues.values() if isinstance(mediaTypeData,list)])
            return rowMediaValues
        
        if isinstance(self.mediaData,Series):
            return
        mediaData = None
        for mediaType in self.mediaTypes:
            searchMediaData = eval("self.mdbio.data.getSearch{0}MediaData()".format(mediaType))
            if isinstance(searchMediaData,Series):
                if allowMissing is False:  ## Nominal Mode
                    searchMediaData = searchMediaData[ids] if isinstance(ids, (list,Index)) else searchMediaData
                else:  ## Evaluation Mode
                    searchMediaData = searchMediaData[searchMediaData.index.isin(ids)] if isinstance(ids, (list,Index)) else searchMediaData
                mediaData = DataFrame(searchMediaData) if mediaData is None else mediaData.join(searchMediaData)
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
            ids,cuts = albums.valid(self.mediaMatchData["NumMedia"])
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