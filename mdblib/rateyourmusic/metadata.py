""" DB-specific Metadata """

__all__ = ["MetaData"]

from mdbmeta import MetaDataBase, MetaDataUtilsBase, UniversalMetaData
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
        self.utils = RateYourMusicMetaDataUtils()
        self.umd   = UniversalMetaData()

        if self.verbose: print("{0} ModValMetaData".format(self.db))
        self.dbmetas = {}
        for meta in self.mdbdata.metas:
            func = "get{0}MetaData".format(meta)
            if hasattr(self.umd.__class__, func) and callable(getattr(self.umd.__class__, func)):
                self.dbmetas[meta] = eval("self.umd.{0}".format(func))
                if self.verbose: print("  ==> {0}".format(meta))
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
                metaData = metaFunc(modValData)
                eval("self.mdbdata.saveMeta{0}Data".format(meta))(modval=modVal, data=metaData)                        
                    
        if self.verbose: ts.stop()

            
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getLinkMetaData(self, modValData):
        alsoKnownAs = modValData.apply(self.utils.getAlsoKnownAs)
        alsoKnownAs = alsoKnownAs.apply(self.utils.fixAlsoKnownAs)
        alsoKnownAs.name = "AlsoKnownAs"

        members = modValData.apply(self.utils.getMembers)
        members = members.apply(self.utils.fixMembers)
        members.name = "Members"

        memberOf = modValData.apply(self.utils.getMemberOf)
        memberOf.name = "MemberOf"            

        metaData = DataFrame([alsoKnownAs,members,memberOf]).T    
        return metaData      

            
    ###############################################################################################################
    # Genre MetaData
    ###############################################################################################################
    def getGenreMetaData(self, modValData):
        artistGenre = modValData.apply(self.utils.getGenres)
        artistGenre.name = "Genre"
           
        metaData = DataFrame(artistGenre)
        return metaData

            
    ###############################################################################################################
    # Metric MetaData
    ###############################################################################################################
    def getMetricMetaData(self, modValData): 
        artistLists = modValData.apply(self.utils.getLists)
        artistLists.name = "Lists"
           
        metaData = DataFrame(artistLists)
        return metaData
           

#####################################################################################################################################
# Base DB MetaData
#####################################################################################################################################
class RateYourMusicMetaDataUtils(MetaDataUtilsBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mdbid   = MusicDBID()        
        
    def getItem(self, item):
        retval = None
        if self.isRawLinkData(item):
            retval = {item.text: self.mdbid.get(item.title)}
        elif self.isRawTextData(item):
            retval = item.text
        return retval
    
    def getTextItems(self, items):
        retval = [item.text for item in items] if isinstance(items,list) else None
        return retval
          
    def getItems(self, items):
        retval = [self.getItem(item) for item in items] if isinstance(items,list) else None
        return retval
        
    def getAlsoKnownAs(self, rData):
        retval = self.getItems(self.getGeneralData(rData, "Also Known As"))
        return retval

    def getMembers(self, rData):
        retval = self.getItems(self.getGeneralData(rData, "Members"))
        return retval

    def getMemberOf(self, rData):
        retval = self.getItems(self.getExtraData(rData, "Members"))
        return retval

    def getGenres(self, rData):
        retval = self.getTextItems(self.getGenresData(rData))
        return retval

    def getLists(self, rData):
        retval = len(self.getExternalData(rData, "Lists", []))
        return retval
           
    
    def fixMembers(self, item):
        retval = None
        if item is None:
            pass
        elif isinstance(item,list):
            if all([isinstance(x,dict) for x in item]):
                retval = item
            elif all([isinstance(x,str) for x in item]) and len(item) == 1:
                retval = [y+")" for y in item[0].split("), ")]
            else:
                raise ValueError("Can't fix item [{0}]".format(item))
        else:
            raise ValueError("Can't fix item [{0}]".format(item))

        return retval

    def fixAlsoKnownAs(self, item):
        retval = None
        if item is None:
            pass
        elif isinstance(item,list):
            if all([isinstance(x,dict) for x in item]):
                retval = item
            elif all([isinstance(x,str) for x in item]) and len(item) == 1:
                retval = [y for y in item[0].split(", ")]
            else:
                raise ValueError("Can't fix item [{0}]".format(item))
        else:
            raise ValueError("Can't fix item [{0}]".format(item))

        return retval