""" Base Utility For Parsing Raw Data """

__all__ = ["ParseRawDataUtils"]

from base import MusicDBBaseData, MusicDBBaseDirs
from fileutils import RecentFiles
from timeutils import Timestat
from pandas import Series
import tarfile
        
#########################################################################################################
# Parse Raw Data Utils
#########################################################################################################
class ParseRawDataUtils:
    def __init__(self, mdbdata, mdbdir, rawio, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("ParseRawData(mdbdata) is not of type MusicDBBaseData")
        if not isinstance(mdbdir, MusicDBBaseDirs):
            raise ValueError("ParseRawData(mdbdir) is not of type MusicDBBaseDirs")
        self.mdbdata = mdbdata
        self.mdbdir  = mdbdir
        self.db      = mdbdir.db
        self.rawio   = rawio
        self.debug   = kwargs.get('debug', kwargs.get('verbose', False))
        
        if self.debug:
            print("ParseRawDataUtils(mdbdata, mdbdir) [{0}]".format(self.db))

    
    ##########################################################################################
    # Input Files Utils
    ##########################################################################################
    def fileSelector(self, files, modVal, fileType, expr='< 0 Days', force=False):
        try:
            modValTimestampPath = eval("self.mdbdata.getModVal{0}Filename({1})".format(fileType, modVal))
        except:
            modValTimestampPath = eval("self.mdbdata.getModValFilename({0})".format(modVal))
        if force is True or not modValTimestampPath.exists():
            if self.debug: print("  ===> Returning all files")
            return files
        else:
            if self.debug: print("  ===> Using {0} as recent timestamp".format(modValTimestampPath.path))
            return RecentFiles(files=files).getFilesByModTime(expr, modValTimestampPath.path)
        
    def getNewFiles(self, modVal, fileType, expr='< 0 Days', force=False):
        fileTypeFunc = "" if fileType in [None, "Artist"] else fileType
        cmd = 'self.mdbdir.getRaw{0}ModValDataDir({1}).glob("*.*", debug=False)'.format(fileTypeFunc, modVal)
        if self.debug: ts = Timestat("Getting New {0} ModVal={1} Files".format(self.db, modVal))
        try:
            files = list(eval(cmd))
        except:
            raise TypeError("Couldn't call {0}".format(cmd))
        newFiles = self.fileSelector(files, modVal, fileType, expr, force)
        if self.debug: print("  ==> Found {0}/{1} New/All Files From {2}".format(len(newFiles),len(files),cmd))
        if self.debug: ts.stop()
        return newFiles
    
    
    ##########################################################################################
    # DB ModVal Data Utils
    ##########################################################################################
    def getModValData(self, modVal, force=False):
        fname = self.mdbdata.getModValFilename(modVal)
        modValData = {} if (force is True or not fname.exists()) else self.mdbdata.getModValData(modVal)
        modValData = modValData.to_dict() if isinstance(modValData,Series) else {}
        return modValData
    
    def saveModValData(self, modVal, modValData):
        modValData = Series(modValData) if isinstance(modValData,dict) else modValData
        self.mdbdata.saveModValData(data=modValData, modval=modVal)
    
    
    ##########################################################################################
    # DB FileType ModVal Data Utils
    ##########################################################################################
    def getFileTypeModValData(self, modVal, fileType, force=False):
        cmd = "self.mdbdata.getModVal{0}Data({1})".format(fileType, modVal)
        try:
            fname = eval("self.mdbdata.getModVal{0}Filename({1})".format(fileType, modVal))
            modValFileTypeData = eval(cmd) if (force is False and fname.exists()) else {}
        except:
            raise ValueError("Couldn't call {0}".format(cmd))
        return modValFileTypeData
    
    def saveFileTypeModValData(self, modVal, fileType, modValFileTypeData):
        modValFileTypeData = Series(modValFileTypeData) if isinstance(modValFileTypeData,dict) else modValFileTypeData
        cmd = "self.mdbdata.saveModVal{0}Data(modval='{1}', data=modValFileTypeData)".format(fileType, modVal)
        #if self.debug: print(cmd)
        try:
            eval(cmd)
        except:
            raise ValueError("Couldn't call {0}".format(cmd))
            
        
    ##########################################################################################
    # I/O Utils
    ##########################################################################################
    def getPrintModValue(self, N):
        base = 100
        return max([base*round((N*0.40)/base),50])
        
        base = 50
        if N > 25000:
            base = 10000
        elif N > 10000:
            base = 4000
        elif N > 2500:
            base = 1000
        elif N > 1250:
            base = 500
        elif N > 500:
            base = 200
        elif N > 250:
            base = 100
        return base

    
    ##########################################################################################
    # Extract Tar File And Report Contents
    ##########################################################################################
    def extractTarfile(self, ifile):
        x =1
        #cmd = 'self.mdbdir.getRaw{0}ModValDataDir({1}).glob("*.*", debug=False)'.format(fileTypeFunc, modVal)
        
    ##########################################################################################
    # Merging Utils
    ##########################################################################################            
    def mergeMediaData(self, prevMediaData, newMediaData):
        if len(prevMediaData) > 0:
            for mediaType,mediaTypeData in newMediaData.items():
                if prevMediaData.get(mediaType) is None:
                    prevMediaData[mediaType] = mediaTypeData
                    continue
                mtd  = {release.code: release for release in mediaTypeData}
                pmtd = {release.code: release for release in prevMediaData.get(mediaType,[])}
                pmtd.update(mtd)
                prevMediaData[mediaType] = list(pmtd.values())
        else:
            prevMediaData = newMediaData
            
        return prevMediaData
        
            
    def updateMediaCounts(self, media):
        counts = {mediaType: len(mediaTypeData) for mediaType,mediaTypeData in media.items()}
        return counts
        #artistIDData.mediaCounts = self.rawio.makeRawMediaCountsData(counts)

    def mergeModValFileTypeData(self, *fileTypeData):
        if self.debug: ts = Timestat("Creating ModValData From {0} Parsed Raw ModVal Data Files".format(len(fileTypeData)))
        if len(fileTypeData) == 0:
            return {}
            
        ### Assume 1st data contains bio data
        modValData = fileTypeData[0]
        for ftdata in fileTypeData[1:]:
            for artistID,artistIDData in ftdata.items():
                if artistID is None:
                    continue
                if modValData.get(artistID) is None:
                    modValData[artistID] = artistIDData
                else:
                    #print("ID:     {0}".format(artistID))
                    #print("Prev:   {0}".format(modValData[artistID].media.media))
                    #print("New:    {0}".format(artistIDData.media.media))
                    #print("Counts: {0}".format(modValData[artistID].mediaCounts.counts))
                    modValData[artistID].media.media = self.mergeMediaData(modValData[artistID].media.media, artistIDData.media.media)
                    #print("Post:   {0}".format(modValData[artistID].media.media))
                    modValData[artistID].mediaCounts.counts = self.updateMediaCounts(modValData[artistID].media.media)
                    #print("Counts: {0}".format(modValData[artistID].mediaCounts.counts))
                    #1/0
                        
        return modValData
        #if self.debug: print("  ====> Saving [{0}] ModVal={1} {2} Entries".format(len(modValData), modVal, "DB Data"))
        #self.saveModValData(modVal, modValData)
        #if self.debug: ts.stop()