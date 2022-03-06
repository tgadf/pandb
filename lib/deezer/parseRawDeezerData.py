from parseRawDataBase import parseRawDataBase
from dbIOGate import dbIOGate
from timeUtils import timestat
from fsUtils import fsInfo
from pandas import Series

        
class parseRawDeezerData(parseRawDataBase):
    def __init__(self, verbose=True):
        super().__init__(db="Deezer", verbose=verbose)
        self.rms = []
    
    
    ##########################################################################################
    # DB ModVal Data Utils
    ##########################################################################################
    def getParseTracksModValDictData(self, modVal, force=False):
        modValData = {} if force is True else self.dbIO.getParseTracksModValData(modVal)
        modValData = modValData.to_dict() if isinstance(modValData,Series) else {}
        return modValData
    
    def saveParseTracksModValDictData(self, modVal, modValData):
        modValData = Series(modValData) if isinstance(modValData,dict) else modValData
        self.dbIO.saveParseTracksModValData(modVal, modValData)
        
        
    #####################################################################################################################
    # Parse Raw Album Data
    #####################################################################################################################
    def parseTracksData(self, modVal, expr='< 0 Days', force=False):
        if self.verbose: ts = timestat("Parsing ModVal={0} Raw {1} Tracks Files(expr=\'{2}\', force={3})".format(modVal, self.db, expr, force))
            
        ############################################
        # New Files Since Last ModValData Update
        ############################################
        newFiles = self.utils.getNewFiles(modVal, expr, force)
            
        N = len(newFiles)
        if N > 0:
            ############################################
            # Current ModValData
            ############################################
            parseModValData = self.getParseTracksModValDictData(modVal, force)
            if self.verbose: print("  ===> Found {0} ModVal Data Entries".format(len(parseModValData)))

            ############################################
            # Loop Over Files And Save Results
            ############################################
            newData = 0
            if self.verbose: tsParse = timestat("Parsing {0} New Files".format(N))
            pModVal = self.utils.getPrintModValue(N)
            for i,ifile in enumerate(newFiles):
                if (i+1) % pModVal == 0 or (i+1) == pModVal/2 or (i+1) == 100:
                    if self.verbose: tsParse.update(n=i+1, N=N)
                
                trackData = self.dbIO.rawIO.get(ifile)
                if len(trackData) == 0:
                    self.rms.append(ifile)
                    continue
                self.mergeParsedRawMediaData(parseModValData, trackData)
                newData += 1
            if self.verbose: tsParse.stop()

            if newData > 0:
                if self.verbose: print("Saving [{0}/{1}] {2} Entries".format(newData, len(parseModValData), "DB Data"))
                self.saveParseTracksModValDictData(modVal, parseModValData)
        
        if self.verbose: ts.stop()
            
            
    def mergeMediaData(self, prevMediaData, newMediaData):
        for mediaType,mediaTypeData in newMediaData.items():
            mtd  = {release.code: release for release in mediaTypeData}
            pmtd = {release.code: release for release in prevMediaData.get(mediaType,[])}
            pmtd.update(mtd)
            prevMediaData[mediaType] = list(pmtd.values())
            

    def mergeParsedRawMediaData(self, parseModValAlbumData, albumData):
        for artistID,artistIDData in albumData.items():
            if parseModValAlbumData.get(artistID) is None:
                parseModValAlbumData[artistID] = artistIDData
            else:
                self.mergeMediaData(parseModValAlbumData[artistID].media.media, artistIDData.media.media)
    
                            
    def createModValData(self, Noe=10, force=True):
        if self.verbose: ts = timestat("Creating ModValData From Parsed Raw ModValData Raw Using {0} Passes".format(Noe))
        for oe in range(Noe):
            if self.verbose: tsOE = timestat("Creating ModValData Pass {0}".format(oe))
            modValData = {modVal: self.utils.getModValData(modVal, force) for modVal in range(self.dbIO.aid.amv.getMaxModVal()) if modVal % Noe == oe}
            print({modVal: len(dbModValData) for modVal,dbModValData in modValData.items()})
            newData = {modVal: 0 for modVal in range(self.dbIO.aid.amv.getMaxModVal()) if modVal % Noe == oe}
            
            for i,modVal in enumerate(range(self.dbIO.aid.amv.getMaxModVal())):
                if (i+1) % 25 == 0 or (i+1) == 5:
                    tsOE.update(n=i+1, N=self.dbIO.aid.amv.getMaxModVal())
                parseTracksModValData = self.getParseTracksModValDictData(modVal)

                for artistID,artistIDData in parseTracksModValData.items():
                    if artistID is None:
                        continue
                    artistModVal = self.dbIO.aid.getModVal(artistID)
                    if artistModVal % Noe != oe:
                        continue
                    if modValData.get(artistModVal) is None:
                        modValData[artistModVal] = {}
                    if modValData[artistModVal].get(artistID) is None:
                        modValData[artistModVal][artistID] = artistIDData
                    else:
                        self.mergeMediaData(modValData[artistModVal][artistID].media.media, artistIDData.media.media)
                        
            if self.verbose: tsOE.stop()
                        
            for modVal,dbModData in modValData.items():
                if self.verbose: print("Saving [{0}] ModVal={1} {2} Entries".format(len(dbModData), modVal, "DB Data"))
                self.utils.saveModValData(modVal, dbModData)
            break
        if self.verbose: ts.stop()