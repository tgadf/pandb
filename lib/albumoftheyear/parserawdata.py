""" Classes to get db artist mod value """

__all__ = ["ParseRawData"]
         
from mdbmaster import MasterParams
from mdbbase import MusicDBBaseData, MusicDBBaseDirs, RawData
from mdbutils import ParseRawDataUtils
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
        self.fileTypes = ["Artist"]
        self.verbose   = kwargs.get('debug', kwargs.get('verbose', False))
        self.db        = mdbdir.db
        self.badData   = {}


    def parse(self, modVal, expr='< 0 Days', force=False):
        self.parseArtistData(modVal, expr, force)           

        
    #####################################################################################################################
    # Parse Album Data
    #####################################################################################################################
    def parseArtistData(self, modVal, expr='< 0 Days', force=False):
        fileType = "Artist"
        if self.verbose: ts = Timestat("Parsing ModVal={0} Raw {1} {2} Files(expr=\'{3}\', force={4})".format(modVal, fileType, self.db, expr, force))
                
                
        ############################################
        # New Files Since Last ModValData Update
        ############################################
        newFiles = self.prdutils.getNewFiles(modVal, fileType=None, expr=expr, force=force)
            
        N = len(newFiles)
        if N > 0:
            ############################################
            # Current ModValData
            ############################################
            modValData = self.prdutils.getModValData(modVal, force)
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

                self.rawio     = RawDBData()                        
                cmd = "self.rawio.get{0}Data(ifile)".format(fileType)
                rData = eval(cmd)
                try:
                    rData = eval(cmd)
                    #rData = RawData(**rData)
                except:
                    print("Could not call {0}".format(cmd))
                    self.badData[ifile] = True
                    continue
                if isinstance(rData.ID.ID, str):
                    modValData[rData.ID.ID] = rData
                    newData += 1
                else:
                    self.badData[ifile] = True
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}/{2}] {3} Entries".format(newData, len(modValData), len(self.badData), "DB Data"))
                self.prdutils.saveModValData(modVal, modValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))
                
        
        if self.verbose: ts.stop()