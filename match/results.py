""" Match Results """

__all__ = ["PrimaryMatchResults", "CrossMatchResults"]

from master import MusicDBPermDir
from ioutils import FileIO
from .dataio import MatchDBDataIO
from .matchlev import getLevenshtein
from pandas import Series

class MatchQuality:
    def __init__(self, rank: dict, baseName: str, compName: str):
        ################################################################################################
        ## Name Quality
        ################################################################################################        
        similarity = getLevenshtein(baseName, compName)
        self.baseName = baseName
        self.compName = compName
        quality = None
        if similarity >= 1.0:
            quality = "Pure"
        elif similarity >= 0.95:
            quality = "Great"
        elif similarity >= 0.9:
            quality = "Good"
        elif similarity >= 0.85:
            quality = "Near"
        else:
            quality = "Low"
            
        self.nameQuality = quality
        
        
        ################################################################################################
        ## Media Quality
        ################################################################################################
        quality = None
        if rank["Medium"] == 0:
            quality = "None"
        else:
            if rank["Exact"] == 0:
                if rank["Tight"] >= 2:
                    quality = "Great"
                elif rank["Tight"] == 1:
                    quality = "Loose"
                    if rank["Medium"] == 1:
                        quality = "Near"
                    if rank["Medium"] >= 2:
                        quality = "Good"
                elif rank["Tight"] == 0:
                    quality = "Poor"
                    if rank["Medium"] >= 2:
                        quality = "Low"
                    if rank["Medium"] >= 3:
                        quality = "Loose"
                    if rank["Medium"] >= 4:
                        quality = "Near"
            elif rank["Exact"] == 1:
                quality = "Good"
                if rank["Tight"] >= 2:
                    quality = "Great"
                if rank["Medium"] == 1:
                    quality = "Sole"
            elif rank["Exact"] >= 2:
                quality = "Great"
                if rank["Tight"] >= 3 or rank["Medium"] >= 5:
                    quality = "Pure"
            else:
                raise ValueError(f"Unsure how to handle rank: {rank}")
            
        self.mediaQuality = quality
        
        
    def getNameQuality(self):
        return self.nameQuality
    
    def getMediaQuality(self):
        return self.mediaQuality
        
        

class MatchResultsBase:
    def __init__(self, matchType, **kwargs):
        self.dbs        = []
        self.matches    = {}
        self.nameLookup = {}
        self.matchType  = matchType
        self.qmap = {"Pure": 8, "Great": 7, "Good": 6, "Sole": 5, "Near": 4, "Loose": 3, "Low": 2, "Poor": 1}
        
        
    def show(self):
        if len(self.matches) > 0:
            dbMatches = Series(self.matches).reset_index()
            dbMatches.columns = ["DB", "MediaQuality", "NameQuality", "BaseID", "CompareID", "Match"]
            print("==== Match Results ====")
            print(dbMatches["MediaQuality"].value_counts())
        else:
            print("No Matches To Show")
        
        
    def get(self):
        if len(self.matches) > 0:
            print("==== Match Results ====")
            dbMatches = Series(self.matches).reset_index()
            dbMatches.columns = ["DB", "MediaQuality", "NameQuality", "BaseID", "CompareID", "Match"]

            print("By DB:")
            print(dbMatches["DB"].value_counts())
            print("By NameQuality:")
            vc = dbMatches["NameQuality"].value_counts()
            for qName,qRank in self.qmap.items():
                print(f"{qName: <7}{vc.get(qName, 0)}")
            print("By MediaQuality:")
            vc = dbMatches["MediaQuality"].value_counts()
            for qName,qRank in self.qmap.items():
                print(f"{qName: <7}{vc.get(qName, 0)}")
        else:
            dbMatches = Series(self.matches).reset_index()
            dbMatches.columns = ["Index", "Match"]
            print("No Matches To Show")
        
        return dbMatches
    
    def save(self):
        io    = FileIO()
        mdbpd = MusicDBPermDir()
        
        savename = mdbpd.getMatchPermPath().join(f"{self.matchType.lower()}Match.p")
        print(f"Saving {self.matchType} Match Results To {savename.str}")
        io.save(idata=self.get(), ifile=savename)
        
        savename = mdbpd.getMatchPermPath().join(f"{self.matchType.lower()}MatchNames.p")
        print(f"Saving {self.matchType} Match Names To {savename.str}")
        io.save(idata=self.nameLookup, ifile=savename)
    
        
        
class PrimaryMatchResults(MatchResultsBase):
    def __init__(self, baseIO: MatchDBDataIO, **kwargs):
        super().__init__("Primary")
        self.baseIO = baseIO
        self.db = baseIO.db
        
    def addResult(self, db: str, dbio: MatchDBDataIO, result: dict):
        for baseid,baseidResults in result.items():
            baseName = self.baseIO.namesData.loc[baseid,"Name"]
            self.nameLookup[(self.db,baseid)] = baseName
            for compareid,matchResult in baseidResults.items():
                compName = dbio.namesData.loc[compareid,"Name"]
                rank = matchResult["Rank"].max(axis=1)
                if rank["Medium"] == 0:
                    continue
                mq = MatchQuality(rank, baseName, compName)
                self.nameLookup[(db,compareid)] = compName
                mediaQuality = mq.getMediaQuality()
                nameQuality  = mq.getNameQuality()

                key   = (db,mediaQuality,nameQuality,baseid,compareid)
                info  = dbio.namesData.loc[compareid].to_dict()
                media = list(matchResult["Raw"]["BestBaseMatch"].keys())
                self.matches[key] = {"BaseName": baseName, "CompareName": compName, "Quality": mq, "Info": info, "Media": media}
                
        self.show()
        


class CrossMatchResults(MatchResultsBase):
    def __init__(self, **kwargs):
        super().__init__("Cross")

    def setBaseNames(self, baseNames):
        self.baseNames = baseNames
        
    def addResult(self, db: str, dbio: MatchDBDataIO, result: dict):
        for baseid,baseidResults in result.items():
            baseName = self.baseNames[baseid]
            self.nameLookup[baseid] = baseName
            for compareid,matchResult in baseidResults.items():
                compName = dbio.namesData.loc[compareid,"Name"]
                rank = matchResult["Rank"].max(axis=1)
                if rank["Medium"] == 0:
                    continue
                mq = MatchQuality(rank, baseName, compName)
                self.nameLookup[(db,compareid)] = compName
                mediaQuality = mq.getMediaQuality()
                nameQuality  = mq.getNameQuality()

                key   = (db,mediaQuality,nameQuality,baseid,compareid)
                info  = dbio.namesData.loc[compareid].to_dict()
                media = list(matchResult["Raw"]["BestBaseMatch"].keys())
                self.matches[key] = {"CompareName": compName, "Quality": mq, "Info": info, "Media": media}