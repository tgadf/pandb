""" MusicDB Summary Data Creater"""

__all__ = ["SummaryData"]

from base import MusicDBBaseData
from master import MasterParams
from utils import MusicDBArtistName
from dbid import MusicDBIDModVal
from timeutils import Timestat
from pandas import Series, DataFrame
from listUtils import getFlatList

class SummaryData:
    def __init__(self, mdbdata, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("MusicDBSummaryData(mdbdata) is not of type MusicDBBaseData")
        self.mdbdata = mdbdata
        self.db      = mdbdata.db
        self.verbose = kwargs.get('debug', kwargs.get('verbose', False))
        self.manc    = MusicDBArtistName()
        self.modVals = MasterParams().getModVals(listIt=True)
        
        self.summaryTypes = ["Basic", "Media", "Counts", "Genre", "Link", "Bio"]
        self.summaryTypes = ["Basic", "Genre", "Link", "Bio"]
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
        if self.verbose: ts = Timestat("Making Basic {0} Summary Data".format(self.db))
        
        artistIDToName      = Series(dtype = 'object', name="Name")
        artistIDToRef       = Series(dtype = 'object', name="Ref")
        artistIDToNumAlbums = Series(dtype = 'object', name="NumAlbums")
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaBasicData(modVal)
            
            artistIDToName = artistIDToName.append(modValMetaData["ArtistName"].apply(self.manc.clean))
            artistIDToRef  = artistIDToRef.append(modValMetaData["URL"])
            artistIDToNumAlbums  = artistIDToNumAlbums.append(modValMetaData["NumAlbums"])
                
        print("  ====> Saving [{0}] {1} Basic Summary Data".format(len(artistIDToName), "ID => Name"))
        self.mdbdata.saveArtistIDToNameData(data=artistIDToName)
        
        print("  ====> Saving [{0}] {1} Basic Summary Data".format(len(artistIDToRef), "ID => Ref"))
        self.mdbdata.saveArtistIDToRefData(data=artistIDToRef)
        
        print("  ====> Saving [{0}] {1} Basic Summary Data".format(len(artistIDToNumAlbums), "ID => Num Albums"))
        self.mdbdata.saveArtistIDToNumAlbumsData(data=artistIDToNumAlbums)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Media
    ###########################################################################################################################################################
    def makeMediaSummaryData(self):
        if self.verbose: ts = Timestat("Making Media {0} Summary Data".format(self.db))
        
        artistIDToMedia     = Series(dtype = 'object', name="Media")
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaMediaData(modVal)
            
            artistIDToMedia = artistIDToMedia.append(modValMetaData["Media"].apply(lambda media: getFlatList(media.values())))
                
        print("  ====> Saving [{0}] {1} Summary Data".format(len(artistIDToMedia), "ID => Media"))
        self.mdbdata.saveArtistIDToMediaData(data=artistIDToMedia)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Counts
    ###########################################################################################################################################################
    def makeCountsSummaryData(self):
        if self.verbose: ts = Timestat("Making Counts {0} Summary Data".format(self.db))
        
        artistIDToCounts     = Series(dtype = 'object', name="Counts")
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaCountsData(modVal)
            
            artistIDToCounts = artistIDToCounts.append(modValMetaData["MediaCounts"])
            
        artistIDToCounts = artistIDToCounts.apply(Series)
        artistIDToCounts = artistIDToCounts.fillna(0).astype(int)
        print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToCounts.shape[0], "ID => Counts"))
        self.mdbdata.saveArtistIDToCountsData(data=artistIDToCounts)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Genre
    ###########################################################################################################################################################
    def makeGenreSummaryData(self):
        if self.verbose: ts = Timestat("Making Genre {0} Summary Data".format(self.db))
        
        artistIDToGenre     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaGenreData(modVal)
            
            if isinstance(modValMetaData,DataFrame):
                artistIDToGenre = artistIDToGenre.append(modValMetaData) if artistIDToGenre is not None else modValMetaData
            
        if isinstance(artistIDToGenre,DataFrame):
            print("  ====> Saving [{0}] {1} Summary Data".format(artistIDToGenre.shape[0], "ID => Genre"))
            self.mdbdata.saveArtistIDToGenreData(data=artistIDToGenre)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Link
    ###########################################################################################################################################################
    def makeLinkSummaryData(self):
        if self.verbose: ts = Timestat("Making Link {0} Summary Data".format(self.db))
        
        artistIDToLink     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaLinkData(modVal)
            
            if isinstance(modValMetaData,DataFrame):
                artistIDToLink = artistIDToLink.append(modValMetaData) if artistIDToLink is not None else modValMetaData
            
        if isinstance(artistIDToLink,DataFrame):
            artistIDToLink = artistIDToLink.apply(Series)
            print("  ====> Saving [{0}] {1} Link Summary Data".format(artistIDToLink.shape[0], "ID => Link"))
            self.mdbdata.saveArtistIDToLinkData(data=artistIDToLink)
        
        if self.verbose: ts.stop()
            
            

    ###########################################################################################################################################################
    # Artist ID => Bio
    ###########################################################################################################################################################
    def makeBioSummaryData(self):
        if self.verbose: ts = Timestat("Making Bio {0} Summary Data".format(self.db))
        
        artistIDToBio     = None
        for i,modVal in enumerate(self.modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(self.modVals))
            modValMetaData = self.mdbdata.getMetaLinkData(modVal)
            
            if isinstance(modValMetaData,DataFrame):
                artistIDToBio = artistIDToBio.append(modValMetaData) if artistIDToBio is not None else modValMetaData
            
        if isinstance(artistIDToBio,DataFrame):
            artistIDToBio = artistIDToBio.apply(Series)
            print("  ====> Saving [{0}] {1} Bio Summary Data".format(artistIDToBio.shape[0], "ID => Bio"))
            self.mdbdata.saveArtistIDToBioData(data=artistIDToBio)
        
        if self.verbose: ts.stop()