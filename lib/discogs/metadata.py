""" DB-specific Metadata """

__all__ = ["MetaData"]

from meta import MetaDataBase, MediaTypeRankBase, MetaDataUtilsBase, MediaMetaData, UniversalMetaData
from pandas import DataFrame, Timestamp
from listUtils import getFlatList
from timeutils import Timestat

from .musicdbid import MusicDBID
from .rawdbdata import RawDBData


#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class MetaData(MetaDataBase):
    def __init__(self, mdbdata, **kwargs):
        super().__init__(mdbdata, **kwargs)
        self.utils = DiscogsMetaDataUtils()
        self.umd   = UniversalMetaData()
        self.mmd   = MediaMetaData(MediaTypeRank())

        if self.verbose: print("{0} ModValMetaData".format(self.db))
        self.dbmetas = {}
        for meta in self.mdbdata.metas:
            func = "get{0}MetaData".format(meta)
            if hasattr(self.umd.__class__, func) and callable(getattr(self.umd.__class__, func)):
                self.dbmetas[meta] = eval("self.umd.{0}".format(func))
                if self.verbose: print("  ==> {0} (Universal)".format(meta))
            elif hasattr(self.mmd.__class__, func) and callable(getattr(self.mmd.__class__, func)):
                self.dbmetas[meta] = eval("self.mmd.{0}".format(func))
                if self.verbose: print("  ==> {0} (Media)".format(meta))
            elif hasattr(self.__class__, func) and callable(getattr(self.__class__, func)):
                self.dbmetas[meta] = eval("self.{0}".format(func))
                if self.verbose: print("  ==> {0}".format(meta))
                
        
    def make(self, modVal=None, metatype=None):
        modVals = self.getModVals(modVal)
        if self.verbose: ts = Timestat("Making {0} {1} MetaData".format(len(modVals), self.db))
        
        for i,modVal in enumerate(modVals):            
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))
            modValData = self.mdbdata.getModValData(modVal)

            metas = {meta: metaFunc for meta,metaFunc in self.dbmetas.items() if ((isinstance(metatype,str) and meta == metatype) or (metatype is None))}
            for meta,metaFunc in metas.items():
                if self.verbose: print("  ==> {0} ... ".format(meta), end="")
                metaData = metaFunc(modValData)
                if self.verbose: print("{0}".format(metaData.shape))
                eval("self.mdbdata.saveMeta{0}Data".format(meta))(modval=modVal, data=metaData)                        
                    
        if self.verbose: ts.stop()

            
    ###############################################################################################################
    # Bio MetaData
    ###############################################################################################################
    def getBioMetaData(self, modValData): 
        artistRealName = modValData.apply(self.utils.getRealName)
        artistRealName.name = "RealName"

        metaData = DataFrame(artistRealName)
        return metaData

            
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getLinkMetaData(self, modValData): 
        artistAliases = modValData.apply(self.utils.getAliases)
        artistAliases.name = "Aliases"

        artistGroups = modValData.apply(self.utils.getGroups)
        artistGroups.name = "Groups"

        artistMembers = modValData.apply(self.utils.getMembers)
        artistMembers.name = "Members"

        metaData = DataFrame([artistAliases,artistGroups,artistMembers]).T
        metaData["Type"] = metaData.apply(lambda row: self.utils.getArtistType(row), axis=1)
        
        return metaData
        
    

#####################################################################################################################################
# Media Type Rank  Utils
#####################################################################################################################################
class MediaTypeRank(MediaTypeRankBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mediaRanking['A'] = [" + Album"]
        self.mediaRanking['B'] = [" + Single", " + EP"]
        self.mediaRanking['C'] = [" + Appearance", " + TrackAppearance"]
        self.mediaRanking['D'] = [" + Producer", " + Co-producer"]
        self.mediaRanking['E'] = [" + Remix", " + Mixed by", " + DJ Mix", " + Scratches"]
        self.mediaRanking['F'] = [" + UnofficialRelease", " + Unofficial"]
        self.mediaRanking['G'] = [" + Video", " + Misc"]
        
        
        
#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class DiscogsMetaDataUtils(MetaDataUtilsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mdbid   = MusicDBID()
            
            
    def getRealName(self, rData):
        retval = self.getGeneralData(rData, 'RealName')
        return retval

    def getAliases(self, rData):
        retval = self.getGeneralData(rData, 'Aliases')
        return retval

    def getGroups(self, rData):
        retval = self.getGeneralData(rData, 'Groups')
        return retval

    def getMembers(self, rData):
        retval = self.getGeneralData(rData, 'Members')
        return retval
    
    def getMedia(self, rData):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData}.values()) for mediaType,mediaTypeData in media.items()}
        return retval
    
    def getArtistType(self, row):
        artistTypes = []
        if isinstance(row["Groups"], list):
            artistTypes.append("Person")
        if isinstance(row["Members"], list):
            artistTypes.append("Group")

        n = len(artistTypes)
        if n == 0:
            retval = "Unknown"
        elif n == 1:
            retval = artistTypes[0]
        else:
            retval = "Multi"
        return retval
