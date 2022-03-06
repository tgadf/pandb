""" Music DB Raw Data Bases Class """

__all__ = ["RawData", "RawDataBase", "RawNameData", "RawMetaData", "RawIDData", "RawURLData", "RawURLInfoData", 
           "RawPageData", "RawProfileData", "RawMediaData", "RawMediaReleaseData", "RawMediaAlbumData", 
           "RawMediaCountsData", "RawFileInfoData", "RawTextData", "RawLinkData", "RawTagData"]

from utils import MusicDBArtistName
from ioutils import FileIO,HTMLIO
from fileutils import FileInfo

from datetime import datetime
from math import ceil, floor
from copy import copy, deepcopy
from bs4 import BeautifulSoup,element
from pandas import Series, DataFrame

def printAll():
    for rdbclass in __all__:
        print("    ###################################################################################")
        print("    def make{0}(self, *args, **kwargs): return {0}(*args, **kwargs)".format(rdbclass))
        print("    def is{0}(self, arg): return isinstance(arg, {0})".format(rdbclass))
        print("")


#####################################################################################################################
# Artist DB Base Classes
#####################################################################################################################
class RawIDData:
    def __init__(self, ID=None, err=None):
        self.ID=ID
        self.err=err
        
    def get(self):
        return self.__dict__
    

class RawLinkData:
    def __init__(self, link, err=None):
        self.href  = None
        self.title = None
        self.text  = None
        self.err   = None
        
        if isinstance(link, element.Tag):
            self.href  = link.attrs.get('href')
            self.title = link.attrs.get('title')
            self.text  = link.text
        else:
            self.err = "NoLink"
        
    def get(self):
        return self.__dict__
    

class RawTagData:
    def __init__(self, tag, err=None):
        self.bstag = None
        self.err   = None
        
        if isinstance(tag, element.Tag):
            self.bstag = deepcopy(tag)
        else:
            self.err = "NoTag"
            
    def getTag(self):
        return self.bstag
        
    def get(self):
        return self.__dict__
    

class RawTextData:
    def __init__(self, text, err=None):        
        self.err   = None
        self.text = deepcopy(text.text.strip()) if isinstance(text, element.Tag) else text.strip()
            
    def get(self):
        return self.__dict__
    
            
class RawURLData:
    def __init__(self, url=None, err=None):
        self.url = url
        self.err = err
        
    def get(self):
        return self.__dict__
        
        
class RawNameData:
    def __init__(self, name=None, native=None, err=None):
        self.name = name
        if native is None:
            self.native = name
        else:
            self.native = native
        self.err  = err
        
    def get(self):
        return self.__dict__
        
        
class RawMetaData:
    def __init__(self, title=None, url=None, err=None):
        self.title = title
        self.url   = url
        self.err   = err
                
    def get(self):
        return self.__dict__
    

class RawMediaData:
    def __init__(self, media={}, err=None):
        self.media = media
        self.err   = err
        
    def get(self):
        return self.__dict__
    

class RawMediaReleaseData:
    def __init__(self, album=None, url=None, aclass=None, aformat=None, artist=None, code=None, year=None, err=None):
        self.album   = album
        self.url     = url
        self.aclass  = aclass
        self.aformat = aformat
        self.artist  = artist
        self.code    = code
        self.year    = year
        self.err     = err
        
    def get(self):
        return self.__dict__
    

class RawMediaAlbumData:
    def __init__(self, url=None, album=None, aformat=None, err=None):
        self.url     = url
        self.album   = album
        self.aformat = aformat
        self.err     = err        
        
    def get(self):
        return self.__dict__

    
class RawMediaCountsData:
    def __init__(self, counts={}, err=None):
        self.counts = counts
        self.err    = err
        
    def get(self):
        return self.__dict__
    

class RawPageData:
    def __init__(self, ppp = None, tot = None, more=None, redo=None, err=None):
        self.ppp   = ppp
        self.tot   = tot
        if isinstance(ppp, int) and isinstance(tot, int):
            self.pages = int(ceil(tot/ppp))
        else:
            self.pages = None

        self.err   = err

        self.more  = more
        self.redo  = redo
        
    def get(self):
        return self.__dict__
    

class RawProfileData:
    def __init__(self, general=None, genres=None, tags=None, external=None, extra=None, err=None):
        self.general  = general
        self.genres   = genres
        self.tags     = tags
        self.external = external
        self.extra    = extra
        self.err      = err
        
    def get(self):
        return self.__dict__
    

class RawURLInfoData:
    def __init__(self, name=None, url=None, ID=None, err=None):
        self.name = name
        self.url  = url
        self.ID   = ID
        self.err  = err
        
    def get(self):
        return self.__dict__
    

class RawFileInfoData:
    def __init__(self, info=None, err=None):
        self.called = datetime.now()        
        if info is not None:
            self.created  = FileInfo(info).time()
            self.filename = info
        else:
            self.created  = None
            self.filename = None
            self.err      = "NoFileInfo"
        
    def get(self):
        return self.__dict__
    

#####################################################################################################################
# Raw Data Class
#####################################################################################################################
class RawData:
    def __init__(self, artist=None, meta=None, url=None, ID=None, pages=None, profile=None, media=None, mediaCounts=None, info=None, err=None):
        self.artist      = artist if isinstance(artist,RawNameData) else RawNameData()
        self.meta        = meta if isinstance(meta,RawMetaData) else RawMetaData()
        self.url         = url if isinstance(url,RawURLData) else RawURLData()
        self.ID          = ID if isinstance(ID,RawIDData) else RawIDData()
        self.pages       = pages if isinstance(pages,RawPageData) else RawPageData()
        self.profile     = profile if isinstance(profile,RawProfileData) else RawProfileData()
        self.media       = media if isinstance(media,RawMediaData) else RawMediaData()
        self.mediaCounts = mediaCounts if isinstance(mediaCounts,RawMediaCountsData) else RawMediaCountsData()
        self.info        = info if isinstance(info,RawFileInfoData) else RawFileInfoData()
        
        
    def show(self):
        print("Artist Data Class")
        print("-------------------------")
        if self.artist.native != self.artist.name:
            print("Artist:  {0} ({1})".format(self.artist.name, self.artist.native))
        else:
            print("Artist:  {0}".format(self.artist.name))
        print("Meta:    {0}".format(self.meta.title))
        print("         {0}".format(self.meta.url))
        print("Info:    {0}".format(self.info.filename))
        print("         {0}".format(self.info.created))
        print("         {0}".format(self.info.called))
        print("URL:     {0}".format(self.url.url))
        print("ID:      {0}".format(self.ID.ID))
        print("Profile: {0}".format(self.profile.get()))
        print("Pages:   {0}".format(self.pages.get()))
        print("Media:   {0}".format(self.mediaCounts.get()))
        for mediaType,mediaTypeAlbums in self.media.media.items():
            print("   {0}".format(mediaType))
            for album in mediaTypeAlbums:
                print("      {0}".format(album.album))     
        
    def get(self):
        return self.__dict__
    
    

class RawDataBase:
    def __init__(self):
        self.bsdata = None
        manc = MusicDBArtistName()
        self.clean = manc.clean

    ###################################################################################
    def makeRawData(self, *args, **kwargs): return RawData(*args, **kwargs)
    def isRawData(self, arg): return isinstance(arg, RawData)

    ###################################################################################
    def makeRawDataBase(self, *args, **kwargs): return RawDataBase(*args, **kwargs)
    def isRawDataBase(self, arg): return isinstance(arg, RawDataBase)

    ###################################################################################
    def makeRawNameData(self, *args, **kwargs): return RawNameData(*args, **kwargs)
    def isRawNameData(self, arg): return isinstance(arg, RawNameData)

    ###################################################################################
    def makeRawMetaData(self, *args, **kwargs): return RawMetaData(*args, **kwargs)
    def isRawMetaData(self, arg): return isinstance(arg, RawMetaData)

    ###################################################################################
    def makeRawIDData(self, *args, **kwargs): return RawIDData(*args, **kwargs)
    def isRawIDData(self, arg): return isinstance(arg, RawIDData)

    ###################################################################################
    def makeRawURLData(self, *args, **kwargs): return RawURLData(*args, **kwargs)
    def isRawURLData(self, arg): return isinstance(arg, RawURLData)

    ###################################################################################
    def makeRawURLInfoData(self, *args, **kwargs): return RawURLInfoData(*args, **kwargs)
    def isRawURLInfoData(self, arg): return isinstance(arg, RawURLInfoData)

    ###################################################################################
    def makeRawPageData(self, *args, **kwargs): return RawPageData(*args, **kwargs)
    def isRawPageData(self, arg): return isinstance(arg, RawPageData)

    ###################################################################################
    def makeRawProfileData(self, *args, **kwargs): return RawProfileData(*args, **kwargs)
    def isRawProfileData(self, arg): return isinstance(arg, RawProfileData)

    ###################################################################################
    def makeRawMediaData(self, *args, **kwargs): return RawMediaData(*args, **kwargs)
    def isRawMediaData(self, arg): return isinstance(arg, RawMediaData)

    ###################################################################################
    def makeRawMediaReleaseData(self, *args, **kwargs): return RawMediaReleaseData(*args, **kwargs)
    def isRawMediaReleaseData(self, arg): return isinstance(arg, RawMediaReleaseData)

    ###################################################################################
    def makeRawMediaAlbumData(self, *args, **kwargs): return RawMediaAlbumData(*args, **kwargs)
    def isRawMediaAlbumData(self, arg): return isinstance(arg, RawMediaAlbumData)

    ###################################################################################
    def makeRawMediaCountsData(self, *args, **kwargs): return RawMediaCountsData(*args, **kwargs)
    def isRawMediaCountsData(self, arg): return isinstance(arg, RawMediaCountsData)

    ###################################################################################
    def makeRawFileInfoData(self, *args, **kwargs): return RawFileInfoData(*args, **kwargs)
    def isRawFileInfoData(self, arg): return isinstance(arg, RawFileInfoData)

    ###################################################################################
    def makeRawTextData(self, *args, **kwargs): return RawTextData(*args, **kwargs)
    def isRawTextData(self, arg): return isinstance(arg, RawTextData)

    ###################################################################################
    def makeRawLinkData(self, *args, **kwargs): return RawLinkData(*args, **kwargs)
    def isRawLinkData(self, arg): return isinstance(arg, RawLinkData)

    ###################################################################################
    def makeRawTagData(self, *args, **kwargs): return RawTagData(*args, **kwargs)
    def isRawTagData(self, arg): return isinstance(arg, RawTagData)  
    

    def getSeriesData(self, ifile):
        self.ifile  = None
        self.bsdata = ifile if isinstance(ifile,Series) else None
        
    def getDataFrameData(self, ifile):
        self.ifile  = None
        self.bsdata = ifile if isinstance(ifile,DataFrame) else None
        
    def getDictData(self, ifile):
        self.ifile  = ifile
        self.bsdata = ifile if isinstance(ifile,dict) else None
        
    def getTupleData(self, ifile):
        self.ifile  = ifile
        self.bsdata = ifile if isinstance(ifile,tuple) else None
        
    def getPickledData(self, ifile):
        self.ifile  = ifile
        try:
            self.bsdata = FileIO().get(ifile)
        except:
            raise ValueError("Could not open data: [{0}]".format(ifile))
        
    def getPickledHTMLData(self, ifile):
        self.ifile  = ifile
        self.getPickledData(ifile)
        self.bsdata = HTMLIO().get(self.bsdata)
        
    def getPickledBytesData(self, ifile):
        self.ifile  = ifile
        self.getPickledData(ifile)
        self.bsdata = HTMLIO().get(FileIO.get(ifile))
                
    def getInfo(self):
        return self.makeRawFileInfoData(self.ifile)

    def assertData(self):
        if self.bsdata is None:
            raise ValueError("There is no BS4 or DB data!")