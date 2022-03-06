""" Classes to get db artist mod value """

__all__ = ["ParseRawData"]
         
from mdbmaster import MasterParams
from mdbbase import MusicDBBaseData, MusicDBBaseDirs
from mdbutils import ParseRawDataUtils
from mdbid import MusicDBIDModVal
from timeutils import Timestat

from .rawdbdata import RawDBData
from .musicdbid import MusicDBID
from pandas import Series
        
class ParseRawData:
    def __init__(self, mdbdata, mdbdir, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("ParseRawData(mdbdata) is not of type MusicDBBaseData")
        if not isinstance(mdbdir, MusicDBBaseDirs):
            raise ValueError("ParseRawData(mdbdir) is not of type MusicDBBaseDirs")
        self.rawio     = RawDBData()
        self.prdutils  = ParseRawDataUtils(mdbdata, mdbdir, self.rawio, **kwargs)
        self.fileTypes = ["Artist", "Album"]
        self.verbose   = kwargs.get('debug', kwargs.get('verbose', False))
        self.db        = mdbdir.db
        self.rawio     = RawDBData()
        self.mdbid     = MusicDBID()
        self.mv        = MusicDBIDModVal()
        self.badData   = {}


    def parse(self, modVal=None, expr='< 0 Days', force=False):
        self.parseArtistData(expr, force)
        self.parseAlbumData(expr, force)
        self.parseWorkData(expr, force)
        self.parseRecordingData(expr, force)

        
    #####################################################################################################################
    # Parse Artist Data
    #####################################################################################################################
    def parseArtistData(self, expr='< 0 Days', force=False):
        fileType = "Artist"
        if self.verbose: ts = Timestat("Parsing Raw {0} {1} Files(expr=\'{2}\', force={3})".format(fileType, self.db, expr, force))

        if self.verbose: tsSearch = Timestat("Get Artist Data, Joining URLs , and Computing ArtistID")
        searchData = self.prdutils.mdbdata.getSearchArtistData()
        searchData["MyArtistID"] = searchData['ArtistGID'].apply(self.mdbid.get)
        searchData["ModVal"]     = searchData['MyArtistID'].apply(self.mv.get)
        if self.verbose: tsSearch.stop()

        for modVal,artistModValData in searchData.groupby("ModVal"):
            modValData = {}
            N = artistModValData.shape[0]
            newData = 0
            if self.verbose: tsParse = Timestat("Parsing {0} ModVal={1} Entries".format(N, modVal))
            pModVal = self.prdutils.getPrintModValue(N)
            for i,(artistMBID,artistData) in enumerate(artistModValData.iterrows()):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                rData = self.rawio.getArtistData(artistData)
                artistID = rData.ID.ID
                if artistID is None:
                    self.badData[artistID] = artistMBID
                    continue
                modValData[artistID] = rData
                newData += 1
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}/{2}] {3} Entries".format(newData, len(modValData), len(self.badData), "DB Data"))
                self.prdutils.saveFileTypeModValData(modVal, fileType, modValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))

        
    #####################################################################################################################
    # Parse Album Data
    #####################################################################################################################
    def parseAlbumData(self, expr='< 0 Days', force=False):
        fileType = "Album"
        if self.verbose: ts = Timestat("Parsing Raw {0} {1} Files(expr=\'{2}\', force={3})".format(fileType, self.db, expr, force))

        if self.verbose: tsSearch = Timestat("Get ReleaseGroup Data")
        searchData = self.prdutils.mdbdata.getSearchArtistReleaseGroupData()
        artistReleaseGroupData = {}
        for artistGID,artistData in searchData.iteritems():
            artistID = self.mdbid.get(artistGID)
            modVal   = self.mv.get(artistID)
            if artistReleaseGroupData.get(modVal) is None:
                artistReleaseGroupData[modVal] = {}
            artistReleaseGroupData[modVal][artistID] = artistData
        if self.verbose: tsSearch.stop()

        for modVal,artistModValData in artistReleaseGroupData.items():
            modValData = {}
            N = len(artistModValData)
            newData = 0
            if self.verbose: tsParse = Timestat("Parsing {0} ModVal={1} Entries".format(N, modVal))
            pModVal = self.prdutils.getPrintModValue(N)
            for i,(artistID,artistData) in enumerate(artistModValData.items()):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                rData = self.rawio.getAlbumData((artistID,artistData))
                if artistID != rData.ID.ID:
                    self.badData[artistID] = True
                    continue
                modValData[artistID] = rData
                newData += 1
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}/{2}] {3} Entries".format(newData, len(modValData), len(self.badData), "DB Data"))
                self.prdutils.saveFileTypeModValData(modVal, fileType, modValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))

        
    #####################################################################################################################
    # Parse Recording Data
    #####################################################################################################################
    def parseRecordingData(self, expr='< 0 Days', force=False):
        fileType = "Recording"
        if self.verbose: ts = Timestat("Parsing Raw {0} {1} Files(expr=\'{2}\', force={3})".format(fileType, self.db, expr, force))

        if self.verbose: tsSearch = Timestat("Get Recording Data")
        searchData = self.prdutils.mdbdata.getSearchArtistRecordingData()
        artistRecordingData = {}
        for artistGID,artistData in searchData.iteritems():
            artistID = self.mdbid.get(artistGID)
            modVal   = self.mv.get(artistID)
            if artistRecordingData.get(modVal) is None:
                artistRecordingData[modVal] = {}
            artistRecordingData[modVal][artistID] = artistData
        if self.verbose: tsSearch.stop()

        for modVal,artistModValData in artistRecordingData.items():
            modValData = {}
            N = len(artistModValData)
            newData = 0
            if self.verbose: tsParse = Timestat("Parsing {0} ModVal={1} Entries".format(N, modVal))
            pModVal = self.prdutils.getPrintModValue(N)
            for i,(artistID,artistData) in enumerate(artistModValData.items()):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                rData = self.rawio.getRecordingData((artistID,artistData))
                if artistID != rData.ID.ID:
                    self.badData[artistID] = True
                    continue
                modValData[artistID] = rData
                newData += 1
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}/{2}] {3} Entries".format(newData, len(modValData), len(self.badData), "DB Data"))
                self.prdutils.saveFileTypeModValData(modVal, fileType, modValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))

        
    #####################################################################################################################
    # Parse Work Data
    #####################################################################################################################
    def parseWorkData(self, expr='< 0 Days', force=False):
        fileType = "Work"
        if self.verbose: ts = Timestat("Parsing Raw {0} {1} Files(expr=\'{2}\', force={3})".format(fileType, self.db, expr, force))

        if self.verbose: tsSearch = Timestat("Get Work Data")
        searchData = self.prdutils.mdbdata.getSearchArtistWorkData()
        artistWorkData = {}
        for artistGID,artistData in searchData.iteritems():
            artistID = self.mdbid.get(artistGID)
            modVal   = self.mv.get(artistID)
            if artistWorkData.get(modVal) is None:
                artistWorkData[modVal] = {}
            artistWorkData[modVal][artistID] = artistData
        if self.verbose: tsSearch.stop()

        for modVal,artistModValData in artistWorkData.items():
            modValData = {}
            N = len(artistModValData)
            newData = 0
            if self.verbose: tsParse = Timestat("Parsing {0} ModVal={1} Entries".format(N, modVal))
            pModVal = self.prdutils.getPrintModValue(N)
            for i,(artistID,artistData) in enumerate(artistModValData.items()):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                rData = self.rawio.getWorkData((artistID,artistData))
                if artistID != rData.ID.ID:
                    self.badData[artistID] = True
                    continue
                modValData[artistID] = rData
                newData += 1
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}/{2}] {3} Entries".format(newData, len(modValData), len(self.badData), "DB Data"))
                self.prdutils.saveFileTypeModValData(modVal, fileType, modValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))

        

    #####################################################################################################################
    # Merge Parsed Data
    #####################################################################################################################
    def mergeModValFileTypeData(self, modVal):
        if self.verbose: ts = Timestat("Merging ModVal={0} Raw {1} Files()".format(modVal, self.db))

        modValFileTypeData = [self.prdutils.getFileTypeModValData(modVal, fileType) for fileType in self.fileTypes]
        modValData = self.prdutils.mergeModValFileTypeData(*modValFileTypeData)

        if self.verbose: print("  ===> Saving [{0}] {1} Entries".format(len(modValData), "DB Data"))
        self.prdutils.saveModValData(modVal, modValData)

    def mergeModValData(self):
        for modVal in range(self.mv.maxModVal):
            self.mergeModValFileTypeData(modVal)