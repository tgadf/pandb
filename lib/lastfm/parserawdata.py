""" Classes to get db artist mod value """

__all__ = ["ParseRawData"]
         
from master import MasterParams
from base import MusicDBBaseData, MusicDBBaseDirs
from utils import ParseRawDataUtils
from dbid import MusicDBIDModVal
from timeutils import Timestat
from .rawdbdata import RawDBData
from .musicdbid import MusicDBID
from fileutils import FileInfo
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


    #####################################################################################################################
    # Utility Functions
    #####################################################################################################################
    def parseArtistData(self, modVal, expr='< 0 Days', force=False):
        self.parseData("Artist", modVal, expr, force)
    def parseAlbumData(self, modVal, expr='< 0 Days', force=False):
        self.parseData("Album", modVal, expr, force)
    def parse(self, modVal, expr='< 0 Days', force=False):
        for fileType in self.fileTypes:
            self.parseData(fileType, modVal, expr, force)

        
    #####################################################################################################################
    # Primary Parser
    #####################################################################################################################
    def parseData(self, fileType, modVal, expr='< 0 Days', force=False):
        if fileType not in self.fileTypes:
            raise ValueError("fileType must be in {0}".format(self.fileTypes))
        if self.verbose: ts = Timestat("Parsing ModVal={0} Raw {1} {2} Files(expr=\'{3}\', force={4})".format(modVal, fileType, self.db, expr, force))
                
                
        ############################################
        # New Files Since Last ModValData Update
        ############################################
        newFiles = self.prdutils.getNewFiles(modVal, fileType, expr, force)
            
        N = len(newFiles)
        if N > 0:
            ############################################
            # Current ModValData
            ############################################
            modValData = self.prdutils.getFileTypeModValData(modVal, fileType, force)
            if self.verbose: print("  ===> Found {0} Previously Saved {1} ModVal Data Entries".format(len(modValData), fileType))

            ############################################
            # Loop Over Files And Save Results
            ############################################
            newData = 0
            badData = 0
            if self.verbose: tsParse = Timestat("Parsing {0} New {1} Files".format(N, fileType))
            pModVal = self.prdutils.getPrintModValue(N)
            for i,ifile in enumerate(newFiles):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                cmd = "self.rawio.get{0}Data(ifile)".format(fileType)
                rData = eval(cmd)
                try:
                    rData = eval(cmd)
                except:
                    print("Could not call {0}".format(cmd))
                    
                if fileType == "Artist":
                    if not isinstance(rData.ID.ID, str):
                        badData += 1
                        continue

                    fmodVal = self.mv.get(rData.ID.ID)                
                    if modValData.get(fmodVal) is None:
                        modValData[fmodVal] = {}
                    
                    modValData[fmodVal][rData.ID.ID] = rData
                    newData += 1
                    continue
                elif fileType == "Album":
                    for artistID,artistIDData in rData.items():
                        if not isinstance(artistID, str):
                            badData += 1
                            continue
                        fmodVal = self.mv.get(artistID)
                        if modValData.get(fmodVal) is None:
                            modValData[fmodVal] = {}
                        if modValData[fmodVal].get(artistID) is None:
                            modValData[fmodVal][artistID] = artistIDData
                            newData += 1
                        else:
                            self.prdutils.mergeMediaData(modValData[fmodVal][artistID].media.media, artistIDData.media.media)
                            newData += 1
                            
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}] {2} Entries".format(newData, len(modValData), "DB Data"))
                for trueModVal,trueModValData in modValData.items():
                    key = "{0}-{1}".format(modVal, trueModVal)
                    self.prdutils.saveFileTypeModValData(key, fileType, trueModValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))
                
        
        if self.verbose: ts.stop()

        
    #####################################################################################################################
    # Merge Parsed Data
    #####################################################################################################################
    def mergeModValData(self, modVal=None, **kwargs):
        mp           = MasterParams()
        modVals      = list(mp.getModVals()) if modVal is None else [modVal]
        maxMedia     = kwargs.get('maxMedia', 100)
        self.verbose = kwargs.get('verbose', False) if kwargs.get('verbose') is not None else self.verbose
        if self.verbose: ts = Timestat("Creating {0} ModVal Data".format(len(modVals)))
        for i,modVal in enumerate(modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))

            modValData = {}
            psModVals = list(mp.getModVals())
            fileType = self.fileTypes[0]
            if self.verbose: tsPS = Timestat("Loading ModVal={0} {1} Data".format(modVal,fileType))
            for j,psModVal in enumerate(psModVals):
                if (j) % 25 == 0 and j > 0:
                    if self.verbose: tsPS.update(n=j, N=len(psModVals))
                key = "{0}-{1}".format(psModVal,modVal)
                
                cmd = "self.prdutils.mdbdata.getModVal{0}Data(key)".format(fileType)
                modValFileTypeData = eval(cmd)
                if modValFileTypeData is None:
                    continue
                for artistID,artistIDData in modValFileTypeData.iteritems():
                    modValData[artistID] = artistIDData
                
                
            for fileType in self.fileTypes[1:]:
                if self.verbose: tsPS = Timestat("Loading ModVal={0} {1} Data".format(modVal,fileType))
                for j,psModVal in enumerate(psModVals):
                    if (j) % 25 == 0 and j > 0:
                        if self.verbose: tsPS.update(n=j, N=len(psModVals))
                    key = "{0}-{1}".format(psModVal,modVal)

                    cmd = "self.prdutils.mdbdata.getModVal{0}Data(key)".format(fileType)
                    modValFileTypeData = eval(cmd)
                    if modValFileTypeData is None:
                        continue
                    for artistID,artistIDData in modValFileTypeData.iteritems():
                        #print(key,'\t',artistID,'\t','album \t',artistIDData.artist.name,end="\t")
                        if modValData.get(artistID) is None:
                            #print(" ==> ADDED <==")
                            modValData[artistID] = artistIDData
                        else:
                            modValData[artistID].media.media = self.prdutils.mergeMediaData(modValData[artistID].media.media, artistIDData.media.media)
                            modValData[artistID].mediaCounts.counts = self.prdutils.updateMediaCounts(modValData[artistID].media.media)
                            #print(" ==> UPDATE MEDIA <==")
                                
            if self.verbose: tsPS.stop()
                
            ####################################################################################
            # Only keep top 200 of each media type (LastFM returns so many entries)
            ####################################################################################
            for artistID,artistIDData in modValData.items():
                artistMedia = artistIDData.media.media
                for mediaType in artistMedia.keys():
                    minCount = Series([x.aformat.get('Counts') for x in artistMedia[mediaType]]).astype(int).sort_values(ascending=False).head(maxMedia).min()
                    artistMedia[mediaType] = [item for item in artistMedia[mediaType] if int(item.aformat.get('Counts', 0)) >= minCount]
                    #print(artistID,'\t',mediaType,'\t',minCount,len(artistMedia[mediaType]))
                artistIDData.media.media = artistMedia
                
            if self.verbose: print("  ===> Saving [{0}] ModVal={1} {2} Entries".format(len(modValData), modVal, "DB Data"))
            self.prdutils.saveModValData(modVal=modVal, modValData=modValData)
           