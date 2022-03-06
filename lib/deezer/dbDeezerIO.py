from dbBaseData import dbIODataBase
from dbBaseIO import dbIOBase
from rawDeezerDataIO import rawDeezerDataIO

class dbDeezerIO(dbIOBase):
    def __init__(self):
        super().__init__("Deezer")
        self.getdbID = self.getArtistPSID
        self.rawIO   = eval("raw{0}DataIO()".format(self.db))
        
        self.parseTracksDir = self.dir.getParseDBDir().join("tracks")
        self.parseTracksDir.mkDir()

        
    ##############################################################################################################
    # Directory IO
    ##############################################################################################################
    def getParseTracksDir(self):
        return self.parseTracksDir
        
        
    ##############################################################################################################
    # File IO
    ##############################################################################################################
    
    ########### (Parse) TrackModVal ###########
    def getParseTracksModValData(self, modVal):
        parseTracksDataIO = dbIODataBase(path=self.getParseTracksDir(), suffix="DB")
        return parseTracksDataIO.get(modVal)
    
    def saveParseTracksModValData(self, modVal, parseTracksData):
        parseTracksDataIO = dbIODataBase(path=self.getParseTracksDir(), suffix="DB")
        parseTracksDataIO.save(parseTracksData, modVal)
                
    
    ########### (Perm) ArtistSearch ###########
    def getArtistSearchSavename(self, dbID):
        permData = dbIODataBase(path=self.getPermModValDir(dbID))
        return permData.getFilename(dbID)
    
    def isArtistSearchKnown(self, dbID):
        return self.getArtistAlbumsSavename(dbID).exists()
    
    def saveArtistSearch(self, dbID, artistSearch):
        permData = dbIODataBase(path=self.getPermModValDir(dbID))
        permData.save(artistSearch, dbID)