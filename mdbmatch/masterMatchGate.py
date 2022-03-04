from timeUtils import timestat
from masterDBGate import masterDBGate
from pandas import DataFrame, Series, concat

class masterMatchGate:
    def __init__(self, mdbDataDBToUse, dbToUse, debug=False):
        print("="*25,"masterMatchGate(mdbDataDBToUse, dbToUse={0})".format(dbToUse),"="*25)
        self.mdbData    = mdbDataDBToUse
        self.mdbGate    = masterDBGate()
        self.dbToUse    = dbToUse
        self.debug      = debug
        
        self.ignoreData = {}
        self.knownData  = {}
        
        self.artistToMatchData      = None
        
        self.matchedArtistData      = []
        self.matchedArtistIDXReq    = {}
        self.matchedArtistAlbumData = Series()
    
    
    ################################################################################################
    # Matched Data I/O
    ################################################################################################
    def getMatchedArtistData(self):
        return self.matchedArtistData
        
    def setMatchedArtistData(self, matchedArtistData):
        self.matchedArtistData = matchedArtistData
        if not isinstance(matchedArtistData,Series):
            raise ValueError("ArtistData Results must be a Series with ID: {ArtistID, ArtistName, Results}")
            
        df = matchedArtistData.apply(lambda x: x["Results"])
        self.artistToMatchData["MatchedArtistData"] = df
        dfDB = df.apply(Series)
        self.matchedArtistIDXReq = {db: concat([DataFrame(idxMatches).T for idx,idxMatches in dbData.iteritems()]) for db,dbData in dfDB.iteritems()}
        
        
    def getMatchedArtistAlbumData(self):
        df = Series({item["ArtistID"]: item["Results"] for item in self.matchedArtistAlbumData})
        return df.apply(Series)
        
    def setMatchedArtistAlbumData(self, matchedArtistAlbumData):
        if not isinstance(matchedArtistAlbumData,Series):
            print("Input")
            print(type(matchedArtistAlbumData))
            print(matchedArtistAlbumData.shape)
            print(matchedArtistAlbumData)
            raise ValueError("ArtistAlbumData Results must be a Series with ID: {ArtistID, ArtistName, Results}")
        self.matchedArtistAlbumData = self.matchedArtistAlbumData.append(matchedArtistAlbumData)
        
        #if isinstance(matchedArtistAlbumData,list) and len(matchedArtistAlbumData) == 0:
        #    return
        #self.matchedArtistAlbumData += matchedArtistAlbumData
    

    ################################################################################################
    # I/O
    ################################################################################################
    def getRunData(self):
        return self.runData
        

    ################################################################################################
    # Set Full Artist/Albums Data
    ################################################################################################
    def setArtistToMatchData(self, artistToMatchData):
        self.artistToMatchData = artistToMatchData
        self.artistToMatchData["HasRunArtist"] = False
        self.artistToMatchData["HasRunAlbum"]  = False
        print("  Setting Artist/Album Match Data")
        print("\t{0: <20}: {1}".format("Artists",self.artistToMatchData.shape[0]))
        print("\t{0: <20}: {1}".format("Max Albums",self.artistToMatchData["NumAlbums"].max()))
        print("\t{0: <20}: {1}".format("Min Albums",self.artistToMatchData["NumAlbums"].min()))
        
        
    ################################################################################################
    # Set Match Run Artist/Albums Data
    ################################################################################################
    def updateArtistDataArtistRunStatus(self):
        if isinstance(self.runData,DataFrame):
            self.artistToMatchData.loc[self.runData.index, "HasRunArtist"] = True
        
    def updateArtistDataAlbumRunStatus(self):
        if isinstance(self.runData,DataFrame):
            self.artistToMatchData.loc[self.runData.index, "HasRunAlbum"] = True

        
    def setArtistRunData(self, maxArtists=None):
        self.runData = None
        if self.debug:
            print("  Setting Artist Match Run Data(maxArtists={0})".format(maxArtists))

        
        numArtistsToMatch = {"ForRun": None, "ForSubRun": None}
        
        cuts = {}
        cuts["Total"] = self.artistToMatchData.shape[0]
        runData = self.artistToMatchData
        
        runData = runData[runData["HasRunArtist"] == False]
        cuts["Has Run == False"] =  runData.shape[0]
        numArtistsToMatch["ForRun"] = runData.shape[0]
        if isinstance(maxArtists,int):
            runData = runData.head(maxArtists)
            cuts["<= {0} Artists".format(maxArtists)] = runData.shape[0]
        numArtistsToMatch["ForSubRun"] = runData.shape[0]

        if self.debug:
            for cut,count in cuts.items():
                print("\t{0: <20}: {1}".format(cut,count))
                
        self.runData = runData.copy(deep=True) if numArtistsToMatch["ForSubRun"] > 0 else None
        return numArtistsToMatch
        
        
    def setAlbumRunData(self, maxAlbums, minAlbums, maxArtists):
        self.runData = None        
        if self.debug:
            print("  Setting Artist/Album Match Run Data(maxAlbums={0}, minAlbums={1}, maxArtists={2})".format(maxAlbums,minAlbums,maxArtists))

        numArtistsToMatch = {"ForRun": None, "ForSubRun": None}
        
        cuts = {}
        cuts["Total"] = self.artistToMatchData.shape[0]
        runData = self.artistToMatchData
        
        runData = runData[runData["HasRunArtist"] == True]
        cuts["Artist Run == True"] =  runData.shape[0]
        runData = runData[runData["HasRunAlbum"] == False]
        cuts["Album Run == False"] =  runData.shape[0]
        runData = runData[runData["NumAlbums"] < maxAlbums]
        cuts["< {0} Albums".format(maxAlbums)] = runData.shape[0]
        runData = runData[runData["NumAlbums"] >= minAlbums]
        cuts[">= {0} Albums".format(minAlbums)] = runData.shape[0]
        numArtistsToMatch["ForRun"] = runData.shape[0]
        runData = runData.head(maxArtists)
        cuts["<= {0} Artists".format(maxArtists)] = runData.shape[0]
        numArtistsToMatch["ForSubRun"] = runData.shape[0]

        if self.debug:
            for cut,count in cuts.items():
                print("\t{0: <20}: {1}".format(cut,count))
                
        if runData.shape[0] > 0:
            self.runData = runData.copy(deep=True)
            albums = Series({idx: Series(self.mdbData.getArtistDBAlbumsFromID(self.dbToUse, idx)) for idx in self.runData.index}, name="Albums")
            self.runData = self.runData.join(albums)            
        return numArtistsToMatch