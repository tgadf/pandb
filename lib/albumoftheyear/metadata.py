""" DB-specific Metadata """

__all__ = ["MetaData"]

from meta import MetaDataBase, MediaTypeRankBase, MetaDataUtilsBase, MediaMetaData, UniversalMetaData
from pandas import DataFrame
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
        self.utils = AlbumOfTheYearMetaDataUtils()
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
                
        
    def make(self, modVal=None):
        modVals = self.getModVals(modVal)
        if self.verbose: ts = Timestat("Making {0} {1} MetaData".format(len(modVals), self.db))
        
        for i,modVal in enumerate(modVals):            
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))
            modValData = self.mdbdata.getModValData(modVal)

            for meta,metaFunc in self.dbmetas.items():
                if self.verbose: print("  ==> {0} ... ".format(meta), end="")
                metaData = metaFunc(modValData)
                if self.verbose: print("{0}".format(metaData.shape))
                eval("self.mdbdata.saveMeta{0}Data".format(meta))(modval=modVal, data=metaData)                        
                    
        if self.verbose: ts.stop()

            
    ###############################################################################################################
    # Genre MetaData
    ###############################################################################################################
    def getGenreMetaData(self, modValData):
        artistGenre = modValData.apply(self.utils.getGenres)
        artistGenre.name = "Genre"
           
        metaData = DataFrame(artistGenre)
        return metaData
        
    

#####################################################################################################################################
# Media Type Rank  Utils
#####################################################################################################################################
class MediaTypeRank(MediaTypeRankBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mediaRanking['A'] = ['LP', "Compilation", "Box Set"]
        self.mediaRanking['B'] = ['Single', "EP"]
        self.mediaRanking['C'] = ['Cover']
        self.mediaRanking['D'] = ['Score', 'Soundtrack', 'Instrumental']
        self.mediaRanking['E'] = ['Mixtape', 'Remix', 'DJ Mix']
        self.mediaRanking['F'] = ['Live', 'Unofficial']
        self.mediaRanking['G'] = ['Video']
        
        

#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class AlbumOfTheYearMetaDataUtils(MetaDataUtilsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mdbid   = MusicDBID()
        
    def getItem(self, item):
        retval = None
        if self.rawio.isRawLinkData(item):
            retval = {item.text: self.mdbid.get(item.href)}
        elif isinstance(item,artistDBTextClass):
            retval = item.text
        return retval  
    
    def getTextItems(self, items):
        retval = [item.text for item in items] if isinstance(items,list) else None
        return retval
          
    def getItems(self, items):
        retval = [self.getItem(item) for item in items] if isinstance(items,list) else None
        return retval

    def getGenres(self, rData):
        retval = self.getTextItems(self.getGenresData(rData))
        return retval
    
    def getMedia(self, rData):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData}.values()) for mediaType,mediaTypeData in media.items()}
        return retval