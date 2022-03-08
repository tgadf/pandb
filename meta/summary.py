""" MusicDB Summary Data Creater"""

__all__ = ["SummaryData"]

from base import MusicDBBaseData
from master import MasterParams
from dbid import MusicDBIDModVal
from timeutils import Timestat
from pandas import Series, DataFrame
from listUtils import getFlatList
from .base import SummaryDataBase

class SummaryData(SummaryDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        self.dbsums = {}
        if self.verbose: print("{0} SummaryData".format(self.db))
        for summaryType in self.summaryTypes:
            func = "make{0}SummaryData".format(summaryType)
            if hasattr(self.__class__, func) and callable(getattr(self.__class__, func)):
                self.dbsums[summaryType] = eval("self.{0}".format(func))
                if self.verbose: print("  ==> {0}".format(summaryType))
            
            
    ###########################################################################################################################################################
    # Master Maker
    ###########################################################################################################################################################
    def make(self):
        for summaryType,summaryTypeFunc in self.dbsums.items():
            summaryTypeFunc()
        
        
    ###########################################################################################################################################################
    # Artist ID => Name/URL Map
    ###########################################################################################################################################################
    def makeBasicSummaryData(self):
        summaryType = "Basic"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToName      = Series(dtype = 'object', name="Name")
        artistIDToRef       = Series(dtype = 'object', name="Ref")
        artistIDToNumAlbums = Series(dtype = 'object', name="NumAlbums")
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaBasicData(modVal) if self.mdbdata.getMetaBasicFilename(modVal).exists() else None
            if isinstance(modValMetaData,DataFrame):
                artistIDToName = artistIDToName.append(modValMetaData["ArtistName"].apply(self.manc.clean))
                artistIDToRef  = artistIDToRef.append(modValMetaData["URL"])
                artistIDToNumAlbums  = artistIDToNumAlbums.append(modValMetaData["NumAlbums"])

        print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(artistIDToName), "ID => Name", summaryType))
        self.mdbdata.saveSummaryNameData(data=artistIDToName)
        
        print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(artistIDToRef), "ID => Ref", summaryType))
        self.mdbdata.saveSummaryRefData(data=artistIDToRef)
        
        print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(artistIDToNumAlbums), "ID => Num Albums", summaryType))
        self.mdbdata.saveSummaryNumAlbumsData(data=artistIDToNumAlbums)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Media
    ###########################################################################################################################################################
    def makeMediaSummaryData(self):
        summaryType = "Media"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
                
        artistIDToMedia     = {rankedMediaType: Series(dtype = 'object', name=rankedMediaType) for rankedMediaType in MasterParams().getMedias().values()}
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaMediaData(modVal) if self.mdbdata.getMetaMediaFilename(modVal).exists() else None
            if isinstance(modValMetaData,DataFrame):
                for rankedMediaType,rankedMediaTypeData in modValMetaData["Media"].apply(Series).items():
                    rankedMediaTypeFlattenedData = rankedMediaTypeData.apply(lambda x: getFlatList(x.values()) if isinstance(x,dict) else None)
                    artistIDToMedia[rankedMediaType] = artistIDToMedia[rankedMediaType].append(rankedMediaTypeFlattenedData)
            
        for rankedMediaType,rankedMediaTypeData in artistIDToMedia.items():
            if len(rankedMediaTypeData) > 0:
                print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(rankedMediaTypeData), "ID => {0}".format(rankedMediaType), summaryType))
                cmd = "self.mdbdata.saveSummary{0}MediaData".format(rankedMediaType)
                eval(cmd)(data=rankedMediaTypeData)
            else:
                print("  ====> Not Saving {0} {1} Summary Data".format("ID => {0}".format(rankedMediaType), summaryType))
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Counts
    ###########################################################################################################################################################
    def makeCountsSummaryData(self):
        summaryType = "Counts"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToCounts     = Series(dtype = 'object', name="Counts")
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaMediaData(modVal) if self.mdbdata.getMetaMediaFilename(modVal).exists() else None
            if isinstance(modValMetaData,DataFrame):
                artistIDToCounts = artistIDToCounts.append(modValMetaData["Counts"])
            
        artistIDToCounts = artistIDToCounts.apply(Series)
        artistIDToCounts = artistIDToCounts.fillna(0).astype(int)
        print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToCounts.shape[0], "ID => {0}".format(summaryType)))
        self.mdbdata.saveSummaryCountsData(data=artistIDToCounts)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Genre
    ###########################################################################################################################################################
    def makeGenreSummaryData(self):
        summaryType = "Genre"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToGenre     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaGenreData(modVal) if self.mdbdata.getMetaGenreFilename(modVal).exists() else None            
            if isinstance(modValMetaData,DataFrame):
                artistIDToGenre = artistIDToGenre.append(modValMetaData) if artistIDToGenre is not None else modValMetaData
            
        if isinstance(artistIDToGenre,DataFrame):
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToGenre.shape[0], "ID => {0}".format(summaryType)))
            self.mdbdata.saveSummaryGenreData(data=artistIDToGenre)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Link
    ###########################################################################################################################################################
    def makeLinkSummaryData(self):
        summaryType = "Link"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToLink     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaLinkData(modVal) if self.mdbdata.getMetaLinkFilename(modVal).exists() else None
            if isinstance(modValMetaData,DataFrame):
                artistIDToLink = artistIDToLink.append(modValMetaData) if artistIDToLink is not None else modValMetaData
            
        if isinstance(artistIDToLink,DataFrame):
            artistIDToLink = artistIDToLink.apply(Series)
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToLink.shape[0], "ID => {0}".format(summaryType)))
            self.mdbdata.saveSummaryLinkData(data=artistIDToLink)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Bio
    ###########################################################################################################################################################
    def makeBioSummaryData(self):
        summaryType = "Bio"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToBio     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaBioData(modVal) if self.mdbdata.getMetaBioFilename(modVal).exists() else None            
            if isinstance(modValMetaData,DataFrame):
                artistIDToBio = artistIDToBio.append(modValMetaData) if artistIDToBio is not None else modValMetaData
            
        if isinstance(artistIDToBio,DataFrame):
            artistIDToBio = artistIDToBio.apply(Series)
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToBio.shape[0], "ID => {0}".format(summaryType)))
            self.mdbdata.saveSummaryBioData(data=artistIDToBio)
        
        if self.verbose: ts.stop()