""" Classes to get db artist mod value """

__all__ = ["ParseRawData"]
         
from master import MasterParams
from base import MusicDBBaseData, MusicDBBaseDirs
from utils import ParseRawDataUtils
from dbid import MusicDBIDModVal
from timeutils import Timestat
from .rawdbdata import RawDBData

        
class ParseRawData:
    def __init__(self, mdbdata, mdbdir, **kwargs):
        if not isinstance(mdbdata, MusicDBBaseData):
            raise ValueError("ParseRawData(mdbdata) is not of type MusicDBBaseData")
        if not isinstance(mdbdir, MusicDBBaseDirs):
            raise ValueError("ParseRawData(mdbdir) is not of type MusicDBBaseDirs")
        self.rawio     = RawDBData()
        self.prdutils  = ParseRawDataUtils(mdbdata, mdbdir, self.rawio, **kwargs)
        self.fileTypes = ["Artist", "Credit", "Song", "Composition"]
        self.verbose   = kwargs.get('debug', kwargs.get('verbose', False))
        self.db        = mdbdir.db
        self.rawio     = RawDBData()
        self.mv        = MusicDBIDModVal()
        self.badData   = {}

        
        
    #####################################################################################################################
    # Utility Functions
    #####################################################################################################################
    def parseArtistData(self, modVal, expr='< 0 Days', force=False):
        self.parseData("Artist", modVal, expr, force)
    def parseCreditData(self, modVal, expr='< 0 Days', force=False):
        self.parseData("Credit", modVal, expr, force)
    def parseSongData(self, modVal, expr='< 0 Days', force=False):
        self.parseData("Song", modVal, expr, force)
    def parseCompositionData(self, modVal, expr='< 0 Days', force=False):
        self.parseData("Composition", modVal, expr, force)
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
            if self.verbose: tsParse = Timestat("Parsing {0} New {1} Files".format(N, fileType))
            pModVal = self.prdutils.getPrintModValue(N)
            for i,ifile in enumerate(newFiles):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                cmd = "self.rawio.get{0}Data(ifile)".format(fileType)
                try:
                    rData = eval(cmd)
                except:
                    print("ifile = {0}".format(ifile))
                    raise ValueError("Could not call {0}".format(cmd))
                if isinstance(rData.ID.ID, str):
                    modValData[rData.ID.ID] = rData
                    newData += 1
                else:
                    self.badData[ifile] = True
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}] {2} Entries".format(newData, len(modValData), "DB Data"))
                self.prdutils.saveFileTypeModValData(modVal, fileType, modValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))
                
        
        if self.verbose: ts.stop()

        
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