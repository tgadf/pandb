""" Classes to get db artist mod value """

__all__ = ["ParseRawData"]
         
from master import MasterParams
from base import MusicDBBaseData, MusicDBBaseDirs
from utils import ParseRawDataUtils
from dbid import MusicDBIDModVal
from timeutils import Timestat
from pandas import DataFrame
from .rawdbdata import RawDBData
from .musicdbid import MusicDBID

        
class ParseRawData:
    def __init__(self, mdbdata, mdbdir, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("ParseRawData(mdbdata) is not of type MusicDBBaseData")
        if not isinstance(mdbdir, MusicDBBaseDirs):
            raise ValueError("ParseRawData(mdbdir) is not of type MusicDBBaseDirs")
        self.rawio     = RawDBData()
        self.prdutils  = ParseRawDataUtils(mdbdata, mdbdir, self.rawio, **kwargs)
        self.fileTypes = ["Artist"]
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
                for artistID,artistIDData in rData.items():
                    if not isinstance(artistID, str):
                        badData += 1
                        continue
                    trueModVal = self.mv.get(artistID)
                    if modValData.get(trueModVal) is None:
                        modValData[trueModVal] = {}
                    if modValData[trueModVal].get(artistID) is None:
                        modValData[trueModVal][artistID] = artistIDData
                        newData += 1
                    else:
                        self.prdutils.mergeMediaData(modValData[trueModVal][artistID].media.media, artistIDData.media.media)
                        newData += 1
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}] Pseudo ModVal={2} {3} Entries".format(newData, len(modValData), modVal, "DB Data"))
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
        mp = MasterParams()
        modVals = list(mp.getModVals()) if modVal is None else [modVal]
        self.verbose = kwargs.get('verbose', False) if kwargs.get('verbose') is not None else self.verbose
        if self.verbose: ts = Timestat("Creating {0} ModVal Data".format(len(modVals)))
            
            
        ##############################################################################
        # Get Extra Info Data
        ##############################################################################
        artistRelatedData = self.prdutils.mdbdata.getRelatedArtistsData()
        artistRelatedData.name = "RelatedArtists"
        artistRelatedData = DataFrame(artistRelatedData)
        artistInfoData    = self.prdutils.mdbdata.getArtistsInfoData()
        extraInfoData     = artistInfoData.join(artistRelatedData)
        
        
        modVals=[0]
        for i,modVal in enumerate(modVals):
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))

            modValData = {}
            psModVals = list(mp.getModVals())
            if self.verbose: tsPS = Timestat("Loading Pseudo ModVal={0} Data".format(modVal))
            for j,psModVal in enumerate(psModVals):
                if (j) % 25 == 0 and j > 0:
                    if self.verbose: tsPS.update(n=j, N=len(psModVals))
                key = "{0}-{1}".format(psModVal,modVal)
                
                fileType = self.fileTypes[0]
                cmd = "self.prdutils.mdbdata.getModVal{0}Data(key)".format(fileType)
                modValFileTypeData = eval(cmd)
                if modValFileTypeData is None:
                    continue                    
                for artistID,artistIDData in modValFileTypeData.iteritems():
                    if modValData.get(artistID) is None:
                        ###########################################################################
                        # Append Extra Info Data (if available)
                        ###########################################################################
                        if artistID in extraInfoData.index:
                            profile = artistIDData.profile
                            extraData = getattr(profile, 'extra') if hasattr(profile, 'extra') else {}
                            if isinstance(extraData,dict):
                                extraData['Image']   = extraInfoData.loc[artistID, 'picture']
                                extraData['Albums']  = extraInfoData.loc[artistID, 'albums']
                                extraData['Fans']    = extraInfoData.loc[artistID, 'fans']
                                extraData['Related'] = extraInfoData.loc[artistID, 'RelatedArtists']
                            if len(extraData) > 0:
                                artistIDData.profile.extra = extraData
                        modValData[artistID] = artistIDData
                
                for fileType in self.fileTypes[1:]:
                    cmd = "self.prdutils.mdbdata.getModVal{0}Data(key)".format(fileType)
                    modValFileTypeData = eval(cmd)
                    if modValFileTypeData is None:
                        continue
                    for artistID,artistIDData in modValFileTypeData.iteritems():
                        if modValData.get(artistID) is None:
                            modValData[artistID] = artistIDData
                        else:
                            modValData[artistID].media.media = self.prdutils.mergeMediaData(modValData[artistID].media.media, artistIDData.media.media)
                            modValData[artistID].mediaCounts.counts = self.prdutils.updateMediaCounts(modValData[artistID].media.media)
                                
            if self.verbose: tsPS.stop()
                
            if self.verbose: print("  ===> Saving [{0}] ModVal={1} {2} Entries".format(len(modValData), modVal, "DB Data"))
            self.prdutils.saveModValData(modVal=modVal, modValData=modValData)