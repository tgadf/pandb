#from matchAlbums import matchAlbums
from listUtils import getFlatList
from searchUtils import findNearest
from pandas import Series
import math
from difflib import SequenceMatcher


class matchArtistAlbums():    
    def __init__(self, name="Dummy", n=1, cutoff=0.7, earlyScoreCutoff=None, earlyMatchCutoff=None, debug=False):
        self.name      = name
        self.minAlbums = n
        self.cutoff    = cutoff
        self.debug     = debug

        self.exact   = None
        self.near    = None
        self.score   = None
        self.ratio   = None
        self.thresh  = None
        self.maxval  = None
        self.albums  = None
        self.nearest = None
        self.mapping = {}
        self.bestmap = {}
        
        self.earlyScoreCutoff = earlyScoreCutoff
        self.earlyMatchCutoff = earlyMatchCutoff
        self.earlyStop = False
        
        
        self.asyms  = Series({1000: 0.98, 500: 0.975, 250: 0.96, 100: 0.95, 50: 0.925, 25: 0.9, 0: 999.9})
        self.rTypes = Series({100: 1, 0: 0})

        
        
    def getBestMatch(self, album):
        return self.bestmap.get(album)
        
        
    def show(self, debug=False):
        print("Best Matches of\n\t[{0}]\n\t[{1}]\nAlbum Lists".format(self.albums[0], self.albums[1]))
        print("\tExact Matches:          {0}".format(self.exact))
        print("\tNear Matches:           {0}".format(self.near))
        print("\tMax Matches:            {0}".format(self.maxval))
        print("\tTotal Score:            {0}".format(self.score))
        print("\tScore/Albums:           {0}".format(round(self.score/len(self.albums[0]), 3)))
        print("\tThreshold Score:        {0}".format(self.thresh))
        print("\tBest Match:             {0}".format(self.bestmap))
        if self.near > 0:
            print("\tThreshold Score/Albums: {0}".format(round(self.thresh/self.near), 3))
        if debug:
            print("\tFull Mapping")
            for albumA, albumAmapping in self.mapping.items():
                print("\t  {0}".format(albumA))
                for albumB, ratio in albumAmapping.items():
                    print("\t\t{0: <30}{1}".format(albumB, ratio))
        
        
    def compare(self, album1,album2,rType):
        if rType == 0:
            retval = SequenceMatcher(None, album1, album2).ratio()
        elif rType == 1:
            retval = SequenceMatcher(None, album1, album2).quick_ratio()
        elif rType == 2:
            retval = SequenceMatcher(None, album1, album2).real_quick_ratio()
        else:
            raise ValueError("rType {0} not known".format(rType))
        return retval
        
        
    def match2(self, albums1, albums2):
        if self.debug:
            print("\tFinding Best Matches of [{0}] and [{1}] Album Lists".format(len(albums1), len(albums2)))
        self.exact   = len(set(albums1).intersection(set(albums2)))
        self.near    = 0
        self.ratio   = 0.0
        self.score   = 0.0
        #self.albums  = [albums1, albums2]
        #self.nearest = []
        #self.bestmap = {}
        
        s1 = len(albums1)
        s2 = len(albums2)
        asymCutoff = self.asyms[s1 >= self.asyms.index].head(1).values[0]
        rType      = self.rTypes[s1 >= self.rTypes.index].head(1).values[0]
        
        asym = abs((s1-s2)/(s1+s2))

        ## Compute After Asym Cutoff
        if asym < asymCutoff:
            near = {j: sum([1 if self.compare(album1,album2,rType) > self.cutoff else 0 for album1 in albums1]) for j,album2 in enumerate(albums2)}
            self.near = sum(near.values())
            #self.near = sum([sum([1 if self.compare(album1,album2,rType) > self.cutoff else 0 for album1 in albums1]) >=1 else 0 for album2 in enumerate(albums2)])
            self.score = self.near

        self.ratio  = round(self.score/len(albums1),3)
        self.score  = round(self.score,3)
        
        if self.debug:
            print("\tMatch Results: [Near={0}  ,  Ratio={1}  ,  Asym={2}  ,  rType={3}]".format(self.score, self.ratio, asym, rType))
        
        
    def match(self, albums1, albums2):
        if self.debug:
            print("\tFinding Best Matches of [{0}] and [{1}] Album Lists".format(len(albums1), len(albums2)))
            
        if albums1 is None:
            raise ValueError("1st set of albums is NULL! for {0}".format(self.name))
        if albums2 is None:
            raise ValueError("2nd set of albums is NULL! for {0}".format(self.name))


        albums2map = None
        if isinstance(albums2, dict):
            albums2map = {v: k for k, v in albums2.items()}
            albums2 = list(albums2.values())
        elif isinstance(albums2, list):
            pass
        else:
            raise ValueError("Albums to match type of [{0}] is unknown".format(type(albums2)))
            
        self.exact   = len(set(albums1).intersection(set(albums2)))
        self.near    = 0
        self.ratio   = 0.0
        self.score   = 0.0
        self.thresh  = 0.0
        self.maxval  = 0.0
        self.albums  = [albums1, albums2]
        self.nearest = []
        self.bestmap = {}

        
        for i,albumA in enumerate(albums1):
            self.bestmap[albumA] = None
            
            nearest = {"Album": None, "Ratio": 0.0}
            self.mapping[albumA] = {}
            for j,albumB in enumerate(albums2):
                s     = SequenceMatcher(None, albumA, albumB)
                ratio = round(s.ratio(),3)
                self.mapping[albumA][albumB] = ratio
                if ratio > nearest["Ratio"]:
                    nearest = {"Album": albumB, "Ratio": ratio}
                    
            self.nearest.append(nearest)
            self.score += nearest["Ratio"]
            self.maxval = max([self.maxval, nearest["Ratio"]])
            if nearest["Ratio"] >= self.cutoff:
                self.near   += 1
                self.thresh += nearest["Ratio"]
                if albums2map is None:
                    self.bestmap[albumA] = {"Name": nearest["Album"], "Code": None}
                else:
                    self.bestmap[albumA] = {"Name": nearest["Album"], "Code": albums2map[nearest["Album"]]}

                    
            ### Test for early match cutoff
            if self.earlyMatchCutoff is not None:
                if self.near >= self.earlyMatchCutoff:
                    if self.debug:
                        print("Stopping early because Near [{0}] > [{1}] Cutoff".format(self.near, self.earlyMatchCutoff))
                    self.earlyStop = True
                    break

            ### Test for early score cutoff
            if self.earlyScoreCutoff is not None:
                if self.thresh >= self.earlyScoreCutoff:
                    if self.debug:
                        print("Stopping early because Score [{0}] > [{1}] Cutoff".format(self.thresh, self.earlyScoreCutoff))
                    self.earlyStop = True
                    break
        
        self.ratio  = round((self.score/len(albums1),3))
        self.score  = round(self.score,3)
        self.thresh = round(self.thresh,3)
        self.maxval = round(self.maxval,3)
        
        if self.debug:
            print("\tMatch Results: [Near={0}  ,  Score={1}  ,  Max={2}]".format(self.near, self.score, self.maxval))



class matchClass:
    def __init__(self, artistID, artistName, db, match):
        self.artistID = artistID
        self.artistName = artistName
        self.db = db
        if isinstance(match, dict):
            self.matchScore = match["Score"]
            self.matchID    = match["ID"]
            self.matchN     = match["Matches"]
        else:
            self.matchScore = None
            self.matchID    = None
            self.matchN     = None
            
    def show(self):
        if self.matchID is not None:
            print("{0} is matched to {1}. Found {2} matches with a max score of {3}".format(self.artistName, self.matchID, self.matchN, self.matchScore))
        else:
            print("{0} is unmatched".format(self.artistName)) 
            


class matchDBArtist:
    def __init__(self, maindb, debug=False):
        self.debug     = debug
        self.dbaccess  = maindb.dbdata
        self.dbdatamap = maindb.dbArtistNameIDMap
        self.dbs       = list(self.dbdatamap.keys())
        
        self.chartType = None
        
        self.matchNumArtistName     = None
        self.matchArtistNameCutoff  = None
        self.matchArtistAlbumCutoff = None
        self.matchNumArtistAlbums   = None
        self.matchScore             = None
        
        self.earlyScoreCutoff = None
        self.earlyMatchCutoff = None
        self.earlyStop = False
        
        self.clean()
        self.setThresholds()
        

    def clean(self):
        self.setArtistInfo(None, None, None)
        
        
    def setChartType(self, chartType):
        self.chartType = chartType
        
    def setArtistInfo(self, artistName, artistID, artistAlbums):
        self.artistName   = artistName
        self.artistID     = artistID
        if artistAlbums is not None:
            self.artistAlbums = [x for x in artistAlbums if x is not None]
        else:
            self.artistAlbums = []
        
        
    #############################################################################
    # Matching Thresholds
    #############################################################################
    def setThresholds(self, matchNumArtistName=10, matchArtistNameCutoff=0.7, 
                      matchNumArtistAlbums=2, matchArtistAlbumCutoff=0.9, matchScore=1.5):
        self.matchNumArtistName     = matchNumArtistName
        self.matchArtistNameCutoff  = matchArtistNameCutoff
        self.matchNumArtistAlbums   = matchNumArtistAlbums
        self.matchArtistAlbumCutoff = matchArtistAlbumCutoff
        self.matchScore             = matchScore
        
        
    #############################################################################
    # Early Stopping Thresholds
    #############################################################################
    def setEarlyStopThresholds(self, earlyScoreCutoff=None, earlyMatchCutoff=None):
        self.earlyScoreCutoff = earlyScoreCutoff
        self.earlyMatchCutoff = earlyMatchCutoff
        self.earlyStop = True
        

    #############################################################################
    # Get List of Possible IDs for DBs
    #############################################################################
    def findPotentialArtistNameMatchesByDB(self, db):
        artistIDs = self.dbdatamap[db].getArtistIDs(artistName=self.artistName,
                                                    chartType=self.chartType,
                                                    num=self.matchNumArtistName,
                                                    cutoff=self.matchArtistNameCutoff, 
                                                    debug=self.debug)
        return artistIDs
    
    def findPotentialArtistNameMatches(self):
        if self.debug:
            print("  Getting DB Artist IDs for ArtistName: {0}".format(self.artistName))
        artistIDs = {db: self.findPotentialArtistNameMatchesByDB(db) for db in self.dbs}
        return artistIDs
    
    def findPotentialArtistNameMatchesWithoutAlbums(self):
        ### Step 1: Get Artist IDs
        artistIDs = self.findPotentialArtistNameMatches()
        
        ### Step 2: Get Match For Each Pair
        dbMatches = {}
        for db, artistDBIDPairs in artistIDs.items():
            bestMatch = None
            if len(artistDBIDPairs) == 1:
                for artistDBName,artistDBIDs in artistDBIDPairs.items():
                    if len(artistDBIDs) == 1:
                        bestMatch = {"ID": artistDBIDs[0], "Matches": 0, "Score": 0}

            if bestMatch is None:
                mc = matchClass(self.artistID, self.artistName, db, None)
            else:
                mc = matchClass(self.artistID, self.artistName, db, bestMatch)
            dbMatches[db] = mc
        
        return dbMatches
    
    
    
        

    #############################################################################
    # Get List of Possible Matches for DBs (if Albums Are Available)
    #############################################################################
    def findPotentialArtistAlbumMatchesByDB(self, db):
        if self.debug:
            print("findPotentialArtistAlbumMatchesByDB(db={0})".format(db))
        if self.artistName is None:
            raise ValueError("Artist Name is not set")
        if self.artistAlbums is None:
            raise ValueError("Artist Albums is not set")
        
        ### Step 1: Get Artist IDs
        artistDBIDPairs = self.findPotentialArtistNameMatchesByDB(db)
        if self.debug:
            print("  DBID Pairs: {0}".format(artistDBIDPairs))

        
        ### Step 2: Get Match For Each Pair
        dbMatches = {}
        for artistDBName,artistDBIDs in artistDBIDPairs.items():
            for artistDBID in artistDBIDs:
                ### Step 2a: Get Albums
                dbArtistAlbums = self.dbdatamap[db].getArtistAlbums(artistDBID, flatten=True)
                dbArtistAlbums = [x for x in dbArtistAlbums if x is not None]
                
                ### Step 2b: Match
                if self.earlyStop:
                    ma = matchArtistAlbums(cutoff=self.matchArtistAlbumCutoff,
                                     earlyScoreCutoff=self.earlyScoreCutoff,
                                     earlyMatchCutoff=self.earlyMatchCutoff, debug=self.debug)
                else:
                    ma = matchArtistAlbums(cutoff=self.matchArtistAlbumCutoff, debug=self.debug)
                ma.match2(self.artistAlbums, dbArtistAlbums)
                dbMatches[artistDBID] = ma
                if self.debug:
                    print("    {0: <30}{1: <30}{2: <5}{3: <5}".format(artistDBName, artistDBID, ma.near, ma.score))
                
                

        ### Step 3: Find Best Match
        bestMatch = {"ID": None, "Matches": 0, "Score": 0.0}
        for artistDBID,ma in dbMatches.items():
            if self.earlyStop is True:
                if ma.near < self.earlyMatchCutoff:
                    continue
                if ma.score < self.earlyScoreCutoff:
                    continue
            if self.earlyStop is False:
                if ma.near < self.matchNumArtistAlbums:
                    continue
                if ma.score < self.matchScore:
                    continue
                
            if ma.near > bestMatch["Matches"]:
                bestMatch = {"ID": artistDBID, "Matches": ma.near, "Score": ma.score}
            elif ma.near == bestMatch["Matches"]:
                if ma.score > bestMatch["Score"]:
                    bestMatch = {"ID": artistDBID, "Matches": ma.near, "Score": ma.score}

        if bestMatch["ID"] is not None:
            if self.debug:
                print("  Best Match: {0}".format(bestMatch))
            mc = matchClass(self.artistID, self.artistName, db, bestMatch)
        else:
            if self.debug:
                print("  Best Match: None")
            mc = matchClass(self.artistID, self.artistName, db, None)
            
        return mc
    
    
    def findPotentialArtistAlbumMatches(self):
        if self.debug:
            print("  Getting DB Artist IDs for ArtistName: {0}".format(self.artistName))
        artistIDs = {db: self.findPotentialArtistAlbumMatchesByDB(db) for db in self.dbs}
        return artistIDs
    
    
    def findPotentialArtistAlbumMatchesByDBList(self, dbs):
        if self.debug:
            print("  Getting DB Artist IDs for ArtistName: {0}".format(self.artistName))
        artistIDs = {db: self.findPotentialArtistAlbumMatchesByDB(db) for db in dbs}
        return artistIDs