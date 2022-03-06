from pandas import DataFrame, Series
from time import perf_counter
from collections import Counter


class matchArtistToDB:
    def __init__(self, mdbData, debug=False, detail=0):
        ## print("mdbData ==> {0}".format(type(mdbData)))
        ## print("mdbData ==> {0}".format(mdbData))
        
        self.mdbData = mdbData
        self.debug   = debug
        self.detail  = detail
        
        self.matchNumArtistName     = None
        self.matchArtistNameCutoff  = None
        self.matchArtistAlbumCutoff = None
        self.matchNumArtistAlbums   = None
        self.matchScore             = None

        self.artistData             = None
        self.artistSearchResults    = {}
        self.albumSearchResults     = {}
        
        self.setThresholds()
        
        self.times = Counter()
        
        self.mm = masterMatch()
        
        
    #############################################################################################################################
    # I/O
    #############################################################################################################################
    def getArtistMatchResults(self):
        matchResults = {}
        for db,dbResult in self.artistSearchResults.items():
            if dbResult.shape[0] == 0:
                matchResults[db] = None
                continue
            dbMatchResult = dbResult.sort_values(by="Levenshtein", ascending=False)
            if dbMatchResult.shape[0] == 0:
                matchResults[db] = None
                continue
                
            matchResults[db] = dbMatchResult.T.to_dict()
        return {"ArtistID": self.artistID, "ArtistName": self.artistName, "Results": matchResults}
    
    
    def getAlbumMatchResults(self):
        matchResults = {}
        for db,dbResult in self.albumSearchResults.items():
            if dbResult.shape[0] == 0:
                matchResults[db] = None
                continue
            dbMatchResult = dbResult.sort_values(by=["Match", "Tight"], ascending=False)
            if dbMatchResult.shape[0] == 0:
                matchResults[db] = None
                continue
                
            dbMatchResult["ArtistName"] = dbMatchResult.apply(lambda x: self.mdbData.getArtistDBNameFromID(db,x.name), axis=1)
            #dbID      = list(dbMatchResult.index)[0]
            #matchName = self.mdbData.getArtistDBNameFromID(db,dbID)
            #matchResults[db] = {"ID": dbID, "ArtistName": matchName}
            matchResults[db] = dbMatchResult.T.to_dict()
        return {"ArtistID": self.artistID, "ArtistName": self.artistName, "ArtistNumAlbums": self.artistNumAlbums, "Results": matchResults}
        
        
    #############################################################################################################################
    # Search Inputs
    #############################################################################################################################
    def setArtistInfo(self, artistData):
        self.artistData      = artistData
        self.artistName      = artistData["ArtistName"]
        self.artistID        = artistData.name
        self.artistAlbums    = artistData["Albums"][artistData["Albums"].notna()] if artistData.get("Albums") is not None else []
        self.artistNumAlbums = len(self.artistAlbums)
        if "MatchedArtistData" in artistData:
            self.artistSearchResults = {db: DataFrame(Series(dbResult)) for db,dbResult in artistData['MatchedArtistData'].items()}
            
        
    def setThresholds(self, **kwargs):
        self.matchNumArtistName     = kwargs.get("numArtists", 2)
        self.matchArtistNameCutoff  = kwargs.get("artistCutoff", 0.8)
        self.matchNumArtistAlbums   = kwargs.get("numAlbums", 2)
        self.matchArtistAlbumCutoff = kwargs.get("albumCutoff", 0.9)
        self.matchScore             = kwargs.get("score", 1.8)
        
        
    #############################################################################################################################
    # Search Functions For Artists
    #############################################################################################################################
    def findArtistMatches(self):
        _ = [self.findArtistMatchesByDB(db) for db in self.mdbData.dbs]
    
    def findArtistMatchesByDB(self, db):
        searchArtists = self.mdbData.getDBSearchArtistNames(db)
        if not isinstance(searchArtists, Series):
            raise ValueError("self.mdbData.getDBSearchArtistNames({0}) returned [{1}] in findArtistMatchesByDB".format(db, type(searchArtists)))
        
        ####### Only Look At Names Within Range Of Artist
        try:
            tmp = searchArtists.apply(len)
        except:
            raise ValueError("Could not apply 'len' to search artists for db [{0}]".format(db))
        artistNameUpper = self.artistName.upper()
        lenArtistName   = len(artistNameUpper)
        
        idxs = abs(tmp - lenArtistName) < 5
        searchArtists = searchArtists.loc[idxs]
        #searchResults = self.mm.matchNames(tomatch=searchResults, value=artistNameUpper)
        searchResults = searchArtists.apply(self.mm.getLevenshtein, x2=artistNameUpper)
        searchResults.name = "Levenshtein"
        idxs   = searchResults >= self.matchArtistNameCutoff
        result = DataFrame(searchArtists[idxs]).join(searchResults[idxs])
        self.artistSearchResults[db] = result
        

    #############################################################################################################################
    # Search Functions For Artists + Albums
    #############################################################################################################################
    def findArtistAlbumMatches(self):
        _ = [self.findArtistAlbumMatchesByDB(db) for db in self.mdbData.dbs]
    
    def findArtistAlbumMatchesByDB(self, db):
        ### 1st: Find Artists That Match By Name
        #ts = timestat("Finding Artists Match For DB [{0}]".format(db))
        if self.artistSearchResults.get(db) is None:
            print("Rerunning Artist Match!!!")
            1/0
            self.findArtistMatchesByDB(db)
        #ts.stop()
        
        
        ### 2nd: Score Each Match Using Albums
        idxResults = {}
        #ts = timestat("Finding Albums For Match For DB [{0}] Using [{1}] Artists".format(db,self.artistSearchResults[db].shape[0]))
        for idx,artistMatchData in self.artistSearchResults[db].iterrows():
            artistMatchAlbums = [str(x).upper() for x in self.mdbData.getArtistDBAlbumsFromID(db,idx) if x is not None]
            numAlbums         = len(artistMatchAlbums)
            idxResults[idx]   = {"NumAlbums": numAlbums, "Loose": -1.0, "Match": -1.0, "Tight": -1.0, "Exact": -1.0}
            idxResults[idx]["M"]      = 0
            idxResults[idx]["Albums"] = artistMatchAlbums if self.detail >= 2 else 0
            if numAlbums > 0:
                try:
                    M    = self.artistAlbums.apply(lambda artistAlbum: Series([self.mm.getLevenshtein(matchAlbum, artistAlbum.upper()) for matchAlbum in artistMatchAlbums]))
                except:
                    print("ERROR Producing M matrix")
                    print("  MatchAlbums:",artistMatchAlbums)
                    print(" ArtistAlbums:",self.artistAlbums)
                    continue
                    
                try:
                    Max0 = M.max(axis=0)
                    Max1 = M.max(axis=1)
                except:
                    print("ERROR Producing M.max() matrix")
                    print("  MatchAlbums:",artistMatchAlbums)
                    print(" ArtistAlbums:",self.artistAlbums)
                    idxResults[idx]["M"]      = M if self.detail >= 1 else 0
                    continue

                try:
                    idxResults[idx]["Loose"]  = min([(Max0 >= 0.775).sum(), (Max1 >= 0.775).sum()])
                    idxResults[idx]["Match"]  = min([(Max0 >= 0.825).sum(), (Max1 >= 0.825).sum()])
                    idxResults[idx]["Tight"]  = min([(Max0 >= 0.875).sum(), (Max1 >= 0.875).sum()])
                    idxResults[idx]["Exact"]  = min([(Max0 >= 0.925).sum(), (Max1 >= 0.925).sum()])
                    idxResults[idx]["M"]      = M if self.detail >= 1 else 0
                    idxResults[idx]["Albums"] = artistMatchAlbums if self.detail >= 2 else 0
                except:
                    print("ERROR Producing Loose/Match/Tight/Exact")
                    print("  MatchAlbums:",artistMatchAlbums)
                    print(" ArtistAlbums:",self.artistAlbums)
                    idxResults[idx]["M"]      = M if self.detail >= 1 else 0
                    continue
            idxResults[idx] = Series(idxResults[idx]).fillna(0)
        result = Series(idxResults).apply(Series) if len(idxResults) > 0 else DataFrame()
        #ts.stop()
        self.albumSearchResults[db] = result