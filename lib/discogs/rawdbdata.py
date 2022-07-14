""" Raw Data Storage Class """

__all__ = ["RawDBData"]

from base import RawDataBase
from .musicdbid import MusicDBID
from .rawapidata import DiscogsReleaseData
from pathlib import PosixPath

class RawDBData(RawDataBase):
    def __init__(self, debug=False):
        super().__init__()
        self.aid = MusicDBID()

        
    ##############################################################################################################################
    ## Parse Artist Data
    ##############################################################################################################################
    def getArtistData(self, inputdata):
        self.getSeriesData(inputdata)
        self.assertData()
                    
        artistData = self.bsdata
        artistID   = artistData["index"]
        artistName = artistData["name"]
        artistURL  = "https://www.discogs.com/artist/{0}".format(artistID)

        generalData = {}
        generalData["RealName"]   = artistData["realname"]
        generalData["Aliases"]    = artistData["MasterAliases"]
        generalData["Groups"]     = artistData["MasterGroups"]
        generalData["Members"]    = artistData["MasterMembers"]
        generalData["Variations"] = artistData["MasterNameVariations"]
        generalData = {k: v for k,v in generalData.items() if v is not None}
        generalData = generalData if len(generalData) > 0 else None
            
        data                = {}
        data["artist"]      = self.makeRawNameData(name=artistName, err=None)
        data["meta"]        = self.makeRawMetaData(title=None, url=artistURL)
        data["url"]         = self.makeRawURLData(url=artistURL)
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["pages"]       = self.makeRawPageData(ppp=1, tot=1, redo=False, more=False)
        data["profile"]     = self.makeRawProfileData(general=generalData)
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)
    
    
    ##############################################################################################################################
    ## Parse Albums Data
    ##############################################################################################################################
    def getAlbumData(self, inputdata):
        self.getDictData(inputdata)
        self.assertData()
        
        ########################################################################
        # Get Releases
        ########################################################################
        mediaData = {}
        albumsData = self.bsdata
        if isinstance(self.ifile, PosixPath):
            artistID = self.ifile.stem  ## I wish this weren't the way
        else:
            raise TypeError("Unsure how to extract artist ID from self.ifile of type [{0}]".format(type(self.ifile)))
            
        for item in albumsData:
            code        = item.get('id')
            albumType   = item.get('type')
            albumFormat = item.get('format')
            albumLabel  = item.get('label')
            albumName   = item.get('name')
            albumURL    = item.get('url')
            albumRole   = item.get('role')
            albumArtist = item.get('artist')
            albumYear   = item.get('year')
            albumMain   = item.get('main_release')

            mediaName = self.getMediaType(item)
            
            amdc = self.makeRawMediaReleaseData(album=albumName, url=albumURL, artist=albumArtist, code=code, aformat=albumFormat, aclass={"Label": albumLabel, "Main": albumMain}, year=albumYear)
            if mediaData.get(mediaName) is None:
                mediaData[mediaName] = []
            mediaData[mediaName].append(amdc)
            
        data                = {}
        data["ID"]          = self.makeRawIDData(ID=artistID)
        data["media"]       = self.makeRawMediaData(media=mediaData)
        data["mediaCounts"] = self.makeRawMediaCountsData(counts={mediaType: len(mediaTypeData) for mediaType,mediaTypeData in data["media"].media.items()})
        data["info"]        = self.getInfo()
        return self.makeRawData(**data)
        
        
    ##############################################################################################################################
    ## Parse Master Release Data
    ##############################################################################################################################
    def getMasterData(self, inputdata):
        self.getDictData(inputdata)
        drd  = DiscogsReleaseData(self.bsdata)
        amdc = self.makeRawMediaReleaseData(album=drd.title, url=drd.url, artist={"Primary": drd.artists, "Extra": drd.extraArtists}, code=str(drd.id), aformat={"Genres": drd.genres, "Styles": drd.styles, "Release": drd.main, "NumTracks": drd.numTracks}, aclass=None, year=drd.year)
        return amdc
    
    

        
        
    ##############################################################################################################################
    ## Media Type Name
    ##############################################################################################################################
    def getMediaType(self, row):
        recRole = row['role']
        recType = row['type']
        recFmat = row['format']

        recRole = "Unknown" if recRole is None else recRole
        recType = "Unknown" if recType is None else recType.title()
        recFmat = "Unknown" if recFmat is None else recFmat

        subType    = None
        videos     = ["VHS", "DVD", "NTSC", "PAL", "Blu-ray"]
        misc       = ["CD-ROM", "CDr", "Transcription"]
        eps        = ["EP"]
        singles    = ["Single", "Maxi"]
        unofficial = ["Unofficial"]
        if sum([x in recFmat for x in videos]) > 0:
            subType = "Video"
        elif sum([x in recFmat for x in misc]) > 0:
            subType = "Misc"
        elif sum([x in recFmat for x in eps]) > 0:
            subType = "EP"
        elif sum([x in recFmat for x in singles]) > 0:
            subType = "Single"
        elif sum([x in recFmat for x in unofficial]) > 0:
            subType = "Unofficial"
        else:
            subType = "Album"

        return " + ".join([recType,recRole,subType])