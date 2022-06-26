""" MusicDB Summary Data Creater"""

__all__ = ["SummaryData"]

from base import MusicDBBaseData
from master import MasterParams
from dbid import MusicDBIDModVal
from timeutils import Timestat
from pandas import Series, DataFrame, concat
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
    def make(self, sumtype=None, **kwargs):
        self.verbose = kwargs.get('verbose', self.verbose)
        dbsums = {summaryType: summaryTypeFunc for summaryType,summaryTypeFunc in self.dbsums.items() if ((isinstance(sumtype,str) and summaryType == sumtype) or (sumtype is None))}
        for summaryType,summaryTypeFunc in dbsums.items():
            summaryTypeFunc()
        
        
    ###########################################################################################################################################################
    # Artist ID => Name/URL Map
    ###########################################################################################################################################################
    def makeBasicSummaryData(self):
        summaryType = "Basic"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToName      = Series(dtype = 'object')
        artistIDToRef       = Series(dtype = 'object')
        artistIDToNumAlbums = Series(dtype = 'object')
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaBasicData(modVal) if self.mdbdata.getMetaBasicFilename(modVal).exists() else None
            if isinstance(modValMetaData,DataFrame):
                artistIDToName       = concat([artistIDToName, modValMetaData["ArtistName"].apply(self.manc.clean)])
                artistIDToRef        = concat([artistIDToRef, modValMetaData["URL"]])
                artistIDToNumAlbums  = concat([artistIDToNumAlbums, modValMetaData["NumAlbums"]])

        print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(artistIDToName), "ID => Name", summaryType))
        artistIDToName.name = "Name"
        self.mdbdata.saveSummaryNameData(data=artistIDToName)
        
        print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(artistIDToRef), "ID => Ref", summaryType))
        artistIDToRef.name = "Ref"
        self.mdbdata.saveSummaryRefData(data=artistIDToRef)
        
        print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(artistIDToNumAlbums), "ID => Num Albums", summaryType))
        artistIDToNumAlbums.name = "NumAlbums"
        self.mdbdata.saveSummaryNumAlbumsData(data=artistIDToNumAlbums)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Media
    ###########################################################################################################################################################
    def makeMediaSummaryData(self):
        summaryType = "Media"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
                
        artistIDToMedia     = None
        artistIDToCounts    = None
        #{rankedMediaType: Series(dtype = 'object', name=rankedMediaType) for rankedMediaType in MasterParams().getMedias().values()}
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaMediaData(modVal) if self.mdbdata.getMetaMediaFilename(modVal).exists() else None
            if isinstance(modValMetaData,DataFrame):
                modValMetaData = modValMetaData.applymap(lambda x: getFlatList(x.values()) if isinstance(x,dict) else None)
                artistIDToMedia = concat([artistIDToMedia, modValMetaData]) if artistIDToMedia is not None else modValMetaData
            
        artistIDToCounts = artistIDToMedia.applymap(lambda x: len(x) if isinstance(x,list) else 0)
        artistIDToCounts = artistIDToCounts.fillna(0).astype(int)
        artistIDToCounts.name = "Counts"
        print("  ====> Saving [{0}] {1} Counts Summary Data".format(artistIDToCounts.shape[0], "ID => {0} Counts".format(summaryType)))
        self.mdbdata.saveSummaryCountsData(data=artistIDToCounts)
            
        for rankedMediaType,rankedMediaTypeData in artistIDToMedia.items():
            if len(rankedMediaTypeData) > 0 and rankedMediaTypeData.count() > 0:
                print("  ====> Saving [{0}] {1} {2} Summary Data".format(len(rankedMediaTypeData), "ID => {0}".format(rankedMediaType), summaryType))
                rankedMediaTypeData.name = rankedMediaType
                cmd = "self.mdbdata.saveSummary{0}MediaData".format(rankedMediaType)
                eval(cmd)(data=rankedMediaTypeData)
            else:
                print("  ====> Not Saving {0} {1} Summary Data".format("ID => {0}".format(rankedMediaType), summaryType))
        
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
                artistIDToGenre = concat([artistIDToGenre, modValMetaData]) if artistIDToGenre is not None else modValMetaData
            
        if isinstance(artistIDToGenre,DataFrame):
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToGenre.shape[0], "ID => {0}".format(summaryType)))
            artistIDToGenre.name = "Genre"
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
                artistIDToLink = concat([artistIDToLink, modValMetaData]) if artistIDToLink is not None else modValMetaData
            
        if isinstance(artistIDToLink,DataFrame):
            #artistIDToLink = artistIDToLink.apply(Series)
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
                artistIDToBio = concat([artistIDToBio, modValMetaData]) if artistIDToBio is not None else modValMetaData
            
        if isinstance(artistIDToBio,DataFrame):
            #artistIDToBio = artistIDToBio.apply(Series)
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToBio.shape[0], "ID => {0}".format(summaryType)))
            self.mdbdata.saveSummaryBioData(data=artistIDToBio)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Dates
    ###########################################################################################################################################################
    def makeDatesSummaryData(self):
        summaryType = "Dates"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToDates     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaDatesData(modVal) if self.mdbdata.getMetaDatesFilename(modVal).exists() else None            
            if isinstance(modValMetaData,DataFrame):
                artistIDToDates = concat([artistIDToDates, modValMetaData]) if artistIDToDates is not None else modValMetaData
            
        if isinstance(artistIDToDates,DataFrame):
            #artistIDToBio = artistIDToBio.apply(Series)
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToDates.shape[0], "ID => {0}".format(summaryType)))
            self.mdbdata.saveSummaryDatesData(data=artistIDToDates)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Metric
    ###########################################################################################################################################################
    def makeMetricSummaryData(self):
        summaryType = "Metric"
        if self.verbose: ts = Timestat("Making {0} {1} Summary Data".format(self.db, summaryType))
        
        artistIDToMetric     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaMetricData(modVal) if self.mdbdata.getMetaMetricFilename(modVal).exists() else None            
            if isinstance(modValMetaData,DataFrame):
                artistIDToMetric = concat([artistIDToMetric, modValMetaData]) if artistIDToMetric is not None else modValMetaData
            
        if isinstance(artistIDToMetric,DataFrame):
            #artistIDToBio = artistIDToBio.apply(Series)
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToMetric.shape[0], "ID => {0}".format(summaryType)))
            self.mdbdata.saveSummaryMetricData(data=artistIDToMetric)
        
        if self.verbose: ts.stop()