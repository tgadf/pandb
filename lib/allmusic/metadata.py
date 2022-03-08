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
        self.utils = AllMusicMetaDataUtils()
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
    # Bio MetaData
    ###############################################################################################################
    def getBioMetaData(self, modValData): 
        artistActiveDates = modValData.apply(self.utils.getActiveDates)
        artistActiveDates = artistActiveDates.apply(self.utils.fixSplitText)
        artistActiveDates.name = "ActiveDates"

        artistBirthDeath = modValData.apply(self.utils.getBirthDeath)
        artistBirthDeath.name = "BirthDeath"

        metaData = DataFrame([artistActiveDates,artistBirthDeath]).T
        return metaData

            
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getLinkMetaData(self, modValData): 
        artistAliases = modValData.apply(self.utils.getAliases)
        artistAliases = artistAliases.apply(self.utils.fixSplitText)
        artistAliases.name = "AlsoKnownAs"

        artistMembers = modValData.apply(self.utils.getMembers)
        artistMembers = artistMembers.apply(self.utils.fixSplitText)
        artistMembers.name = "Members"

        artistMemberOf = modValData.apply(self.utils.getMemberOf)
        artistMemberOf = artistMembers.apply(self.utils.fixSplitText)
        artistMemberOf.name = "MemberOf"            

        metaData = DataFrame([artistAliases,artistMembers,artistMemberOf]).T
        return metaData

            
    ###############################################################################################################
    # Genre MetaData
    ###############################################################################################################
    def getGenreMetaData(self, modValData): 
        artistGenres = modValData.apply(self.utils.getGenres)
        artistGenres.name = "Genre"

        artistTags = modValData.apply(self.utils.getTags)
        artistTags.name = "Tag"

        metaData = DataFrame([artistGenres,artistTags]).T
        return metaData
        
    

#####################################################################################################################################
# Media Type Rank  Utils
#####################################################################################################################################
class MediaTypeRank(MediaTypeRankBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mediaRanking['A'] = ["Albums", "Album", "Comp"]
        self.mediaRanking['B'] = ["Single/EP", "Songs"]
        self.mediaRanking['D'] = ["Credits"]
        self.mediaRanking['G'] = ["Video", " + Other"]
        self.mediaRanking['H'] = ["Composition", "Electronic/Computer Music", "Keyboard Electronic/Computer Music", "Avant-Garde Music Electronic/Computer Music"]
        
        
        
#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class AllMusicMetaDataUtils(MetaDataUtilsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mdbid   = MusicDBID()
        
    def getItem(self, item):
        retval = None
        if self.isRawLinkData(item):
            retval = {item.text: self.mdbid.get(item.href)}
        elif self.isRawTextData(item):
            retval = item.text
        return retval
    
    def getTextItems(self, items):
        retval = [item.text for item in items] if isinstance(items,list) else None
        return retval
          
    def getItems(self, items):
        retval = [self.getItem(item) for item in items] if isinstance(items,list) else None
        return retval
    
    def getMedia(self, rData):
        media = self.getMediaData(rData, {})
        retval = {mediaType: list({release.code: release.album for release in mediaTypeData}.values()) for mediaType,mediaTypeData in media.items()}
        return retval

    def getBirthDeath(self, rData):
        formed = self.getGeneralData(rData, 'birth')
        formed = formed.year if isinstance(formed, Timestamp) else None
        disbanded = self.getGeneralData(rData, 'death')
        disbanded = disbanded.year if isinstance(disbanded, Timestamp) else None
        retval = [formed,disbanded]
        return retval
            
    def getActiveDates(self, rData):
        retval = self.getItems(self.getGeneralData(rData, "active-dates"))
        return retval
            
    def getAliases(self, rData):
        retval = self.getItems(self.getGeneralData(rData, "aliases"))
        return retval
            
    def getMembers(self, rData):
        retval = self.getItems(self.getGeneralData(rData, "group-members"))
        return retval
            
    def getMemberOf(self, rData):
        retval = self.getItems(self.getGeneralData(rData, "member-of"))
        return retval

    def getGenres(self, rData):
        retval = self.getTextItems(self.getGenresData(rData))
        return retval

    def getTags(self, rData):
        retval = self.getTextItems(self.getTagsData(rData))
        return retval

    def fixSplitText(self, item):
        retval = None
        if item is None:
            pass
        elif isinstance(item,list):
            if len(item) == 1 and isinstance(item[0],str):
                retval = item[0].split("\n")[-1].strip()
            elif all([isinstance(x,dict) for x in item]):
                retval = []
                for x in item:
                    items = list(x.items())[0]
                    key,value = items[0],items[1]
                    key = [y for y in key.split("\n") if len(y) > 0]
                    if len(key) == 1:
                        retval.append({key[0].strip(): value})
                    else:
                        raise ValueError("Can't fix item [{0}]".format(item))
            else:
                raise ValueError("Can't fix item [{0}]".format(item))
        else:
            raise ValueError("Can't fix item [{0}]".format(item))
        return retval