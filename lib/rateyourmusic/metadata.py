""" DB-specific Metadata """

__all__ = ["MetaData", "RateYourMusicMetaDataUtils"]

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
        self.utils = RateYourMusicMetaDataUtils()
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
                
        
    def make(self, modVal=None, metatype=None, **kwargs):
        self.verbose = kwargs.get('verbose', self.verbose)
        modVals = self.getModVals(modVal)
        if self.verbose: ts = Timestat("Making {0} {1} MetaData".format(len(modVals), self.db))
        
        metas = {meta: metaFunc for meta,metaFunc in self.dbmetas.items() if ((isinstance(metatype,str) and meta == metatype) or (metatype is None))}
        if len(metas) == 0:
            if self.verbose: ts.stop()
            return
            
        for i,modVal in enumerate(modVals):            
            if (i+1) % 25 == 0 or (i+1) == 5:
                if self.verbose: ts.update(n=i+1, N=len(modVals))
            modValData = self.mdbdata.getModValData(modVal)

            for meta,metaFunc in metas.items():
                if self.verbose: print("  ==> {0} ... ".format(meta), end="")
                metaData = metaFunc(modValData)
                if self.verbose: print("{0}".format(metaData.shape))
                eval("self.mdbdata.saveMeta{0}Data".format(meta))(modval=modVal, data=metaData)                        
                    
        if self.verbose: ts.stop()

            
    ###############################################################################################################
    # Link MetaData
    ###############################################################################################################
    def getLinkMetaData(self, modValData):
        alsoKnownAs = modValData.apply(self.utils.getAlsoKnownAs)
        #alsoKnownAs = alsoKnownAs.apply(self.utils.fixAlsoKnownAs)
        alsoKnownAs.name = "AlsoKnownAs"

        members = modValData.apply(self.utils.getMembers)
        #members = members.apply(self.utils.fixMembers)
        members.name = "Members"

        memberOf = modValData.apply(self.utils.getMemberOf)
        memberOf.name = "MemberOf"

        related = modValData.apply(self.utils.getRelatedArtists)
        related.name = "RelatedArtists"

        metaData = DataFrame([alsoKnownAs,members,memberOf,related]).T
        metaData["Type"] = metaData.apply(lambda x: "Group" if isinstance(x["Members"],dict) else "Artist", axis=1)
        
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
    # Bio MetaData
    ###############################################################################################################
    def getBioMetaData(self, modValData): 
        artistBorn = modValData.apply(self.utils.getBorn)
        artistBorn.name = "Born"
        
        artistFormed = modValData.apply(self.utils.getFormed)
        artistFormed.name = "Formed"
           
        artistCurrently = modValData.apply(self.utils.getCurrently)
        artistCurrently.name = "Currently"
           
        artistDisbanded = modValData.apply(self.utils.getDisbanded)
        artistDisbanded.name = "Disbanded"
           
        artistNotes = modValData.apply(self.utils.getNotes)
        artistNotes.name = "Notes"
           
        metaData = DataFrame([artistBorn,artistFormed,artistCurrently,artistDisbanded,artistNotes]).T
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
        artistLists.name = "NumLists"
           
        metaData = DataFrame(artistLists)
        return metaData
    

#####################################################################################################################################
# Media Type Rank  Utils
#####################################################################################################################################
class MediaTypeRank(MediaTypeRankBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mediaRanking['A'] = ["Album", "Compilation"]
        self.mediaRanking['B'] = ["Single", "EP"]
        self.mediaRanking['C'] = ['Appears On', "V/A Compilation"]
        self.mediaRanking['D'] = ["Credits", "Solo Instrument"]
        self.mediaRanking['E'] = ["DJ Mix", "Mixtape"]
        self.mediaRanking['F'] = ["Bootleg / Unauthorized"]
        self.mediaRanking['G'] = ["Video", "Music video", "Stage"]
           
            

#####################################################################################################################################
# MetaData Utils
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
        akas = self.getGeneralData(rData, "Also Known As", [])
        retval = {}
        for item in akas:
            if self.isRawLinkData(item):
                retval.update({item.text: self.mdbid.get(item.title)})
            elif self.isRawTextData(item):
                retval.update({name: None for name in [y for y in item.text.split(", ")]})
        retval = retval if (isinstance(retval,dict) and len(retval) > 0) else None
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
        return retval

    def getFormed(self, rData):
        formed = self.getTextItems(self.getGeneralData(rData, "Formed", []))
        retval = self.splitDatePlace(formed[0]) if len(formed) == 1 else None
        return retval

    def getBorn(self, rData):
        formed = self.getTextItems(self.getGeneralData(rData, "Born", []))
        retval = self.splitDatePlace(formed[0]) if len(formed) == 1 else None
        return retval

    def getDisbanded(self, rData):
        formed = self.getTextItems(self.getGeneralData(rData, "Disbanded", []))
        retval = self.splitDatePlace(formed[0]) if len(formed) == 1 else None
        return retval

    def getCurrently(self, rData):
        formed = self.getTextItems(self.getGeneralData(rData, "Currently", []))
        retval = self.splitDatePlace(formed[0]) if len(formed) == 1 else None
        return retval

    def getNotes(self, rData):
        retval = self.getGeneralData(rData, "Notes")
        return retval

    def getMembers(self, rData):
        members = self.getGeneralData(rData, "Members", [])
        retval = {}
        for item in members:
            if self.isRawLinkData(item):
                retval.update({item.text: self.mdbid.get(item.title)})
            elif self.isRawTextData(item):
                retval.update({name: None for name in [y+")" for y in item.text.split("), ")]})
        retval = retval if (isinstance(retval,dict) and len(retval) > 0) else None
        return retval

    def getMemberOf(self, rData):
        memberOfs = self.getExtraData(rData, "Member of", [])
        retval = {}
        for item in memberOfs:
            if self.isRawLinkData(item):
                retval.update({item.text: self.mdbid.get(item.title)})
            elif self.isRawTextData(item):
                retval.update({name: None for name in [y for y in item.text.split(", ")]})
        retval = retval if (isinstance(retval,dict) and len(retval) > 0) else None
        return retval
    
    def getType(self, rData):
        members  = self.getMembers(rData)
        memberof = self.getMemberOf(rData)
        retval = "Group" if isinstance(members,list) else "Artist"        
        return retval

    def getRelatedArtists(self, rData):
        retval = self.getItems(self.getExtraData(rData, "Related Artists"))
        return retval

    def getGenres(self, rData):
        retval = self.getTextItems(self.getGenresData(rData))
        return retval

    def getLists(self, rData):
        retval = len(self.getExternalData(rData, "Lists", []))
        return retval
    
    def splitDatePlace(self, text):
        vals = [val.strip() for val in text.split(",")] if isinstance(text,str) else []
        dateinfo = []
        locinfo  = vals
        for i,val in enumerate(vals):
            if val.isdigit():
                dateinfo = vals[:(i+1)]
                locinfo  = vals[(i+1):]
                break
        formedDate  = self.getDateInfo(dateinfo)
        formedPlace = self.getPlaceInfo(locinfo)
        return {**formedDate, **formedPlace}
    
    def getDateInfo(self, x):
        if not isinstance(x,list):
            return {"Date": None}
        if len(x) == 0:
            return {"Date": None}
        elif len(x) == 1:
            return {"Date": x[0]}
        else:
            return {"Date": x[-1]}

    def getPlaceInfo(self, x):
        if not isinstance(x,list):
            return {"City": None, "State": None, "Country": None}
        if len(x) == 0:
            return {}
        elif len(x) == 1:
            return {"City": None, "State": None, "Country": x[0]}
        elif len(x) == 2:
            return {"City": None, "State": x[0], "Country": x[1]}
        elif len(x) == 3:
            return {"City": x[0], "State": x[1], "Country": x[2]}
        else:
            return {"City": x[-3], "State": x[-2], "Country": x[-1]}