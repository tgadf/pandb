""" Classes to get db artist mod value """

__all__ = ["ParseRawData"]
         
from master import MasterParams
from base import ParseRawDataBase
from utils import ParseRawDataUtils
from dbid import MusicDBIDModVal
from timeutils import Timestat
from .rawdbdata import RawDBData
from .musicdbid import MusicDBID

        
class ParseRawData(ParseRawDataBase):
    def __init__(self, mdbdata, mdbdir, **kwargs):
        super().__init__(mdbdata, mdbdir, **kwargs)
        self.rawio     = RawDBData()
        self.mdbid     = MusicDBID()
        self.prdutils  = ParseRawDataUtils(mdbdata, mdbdir, self.rawio, **kwargs)
        self.fileTypes = ["Artist", "Album"]


    def parse(self, modVal, expr='< 0 Days', force=False):
        self.parseArtistData(modVal, expr, force)
        self.parseAlbumData(modVal, expr, force)

        
    #####################################################################################################################
    # Parse Artist Data
    #####################################################################################################################
    def parseArtistData(self, modVal=None, expr='< 0 Days', force=False):
        fileType = "Artist"
        if self.verbose: ts = Timestat("Parsing ModVal={0} Raw {1} {2} Files(expr=\'{3}\', force={4})".format(modVal, fileType, self.db, expr, force))
        searchData = self.prdutils.mdbdata.getSearchArtistData().reset_index()
        searchData['dbID']   = searchData['sid'].apply(self.mdbid.get)
        searchData['modVal'] = searchData['dbID'].apply(self.mv.get)
                
        for sModVal,df in searchData.groupby("modVal"):
            checkVal = str(modVal) == str(sModVal) if isinstance(modVal,(int,str)) else True
            if checkVal is False:
                continue
                
            ############################################
            # New Files Since Last ModValData Update
            ############################################
            N = df.shape[0]
            modValData = {}
            newData = 0
            badData = 0
            if self.verbose: tsParse = Timestat("Parsing {0} Searched For Artists With ModVal={1}".format(N,sModVal))
            pModVal = self.prdutils.getPrintModValue(10*N)
            for i,(idx,row) in enumerate(df.iterrows()):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                rData = self.rawio.getArtistData(row)
                if isinstance(rData.ID.ID, str):
                    modValData[rData.ID.ID] = rData
                    newData += 1
                else:
                    badData += 1
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("  ===> Saving [{0}/{1}] {2} Entries".format(newData, len(modValData), "DB Data"))
                self.prdutils.saveFileTypeModValData(sModVal, fileType, modValData)
            else:
                if self.verbose: print("  ===> Did not find any new data from {0} files".format(N))
        
        if self.verbose: ts.stop()        
            
            

        
    #####################################################################################################################
    # Parse Album Data
    #####################################################################################################################
    def parseAlbumData(self, modVal, expr='< 0 Days', force=False):
        fileType = "Album"
        if self.verbose: ts = Timestat("Parsing ModVal={0} Raw {1} {2} Files(expr=\'{3}\', force={4})".format(modVal, fileType, self.db, expr, force))
                
                
        ############################################
        # New Files Since Last ModValData Update
        ############################################
        newFiles = self.prdutils.getNewFiles(modVal, fileType=fileType, expr=expr, force=force)
            
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
                try:
                    rData = eval(cmd)
                except:
                    print("Could not call {0}".format(cmd))
                if isinstance(rData.ID.ID, str):
                    modValData[rData.ID.ID] = rData
                    newData += 1
                else:
                    badData += 1
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

    def mergeModValData(self, modVal=None, **kwargs):
        mp           = MasterParams()
        modVals      = list(mp.getModVals()) if modVal is None else [modVal]
        self.verbose = kwargs.get('verbose', False) if kwargs.get('verbose') is not None else self.verbose
        if self.verbose: ts = Timestat("Creating {0} ModVal Data".format(len(modVals)))
            
        for n,modVal in enumerate(modVals):
            if self.verbose:
                if (n+1) % 25 == 0 or (n+1) == 5:
                    ts.update(n=n,N=len(modVals))
            self.mergeModValFileTypeData(modVal)

        if self.verbose: ts.stop()