""" Match Results """

__all__ = ["MatchResults", "CrossMatchResults"]

from master import MusicDBPermDir
from ioutils import FileIO
from .dataio import MatchDBDataIO
from pandas import Series

class MatchResults:
    def __init__(self, baseIO: MatchDBDataIO, **kwargs):
        self.baseIO = baseIO
        self.db = baseIO.db
        self.dbs = []
        self.matches = {}
        
        self.nameLookup = {}
        
    def addResult(self, db: str, dbio: MatchDBDataIO, result: dict):
        for baseid,baseidResults in result.items():
            baseName = self.baseIO.namesData.loc[baseid,"Name"]
            self.nameLookup[(self.db,baseid)] = baseName
            for compareid,matchResult in baseidResults.items():
                rank = matchResult["Rank"].max(axis=1)
                if rank["Medium"] == 0:
                    continue
                quality = None
                if rank["Tight"] >= 4:
                    quality = "Pure"
                elif rank["Tight"] >= 2 and rank["Medium"] >= 3:
                    quality = "Great"
                elif rank["Tight"] >= 1 and rank["Medium"] >= 2:
                    quality = "Good"
                elif rank["Tight"] >= 1:
                    quality = "Near"
                elif rank["Medium"] >= 1:
                    quality = "Loose"
                else:
                    print(rank)

                compName = dbio.namesData.loc[compareid,"Name"]
                self.nameLookup[(db,compareid)] = compName

                self.matches[(db,quality,baseid,compareid)] = {"BaseName": baseName, "CompareName": compName, "Rank": rank, "Info": dbio.namesData.loc[compareid].to_dict(), "Media": list(matchResult["Raw"]["BestBaseMatch"].keys())}
        
    def get(self):
        print("==== Match Results ====")
        dbMatches = Series(self.matches).reset_index()
        dbMatches.columns = ["DB", "Quality", "BaseID", "CompareID", "Match"]
        
        print("By DB:")
        print(dbMatches["DB"].value_counts())
        print("By Quality:")
        print(dbMatches["Quality"].value_counts())
        
        return dbMatches
    
    def save(self):
        io    = FileIO()
        mdbpd = MusicDBPermDir()
        
        savename = mdbpd.getMatchPermPath().join("primaryMatch.p")
        print("Saving Primary Match Results To {0}".format(savename.str))
        io.save(idata=self.get(), ifile=savename)
        
        savename = mdbpd.getMatchPermPath().join("primaryMatchNames.p")
        print("Saving Primary Match Names To {0}".format(savename.str))
        io.save(idata=self.nameLookup, ifile=savename)
    
        


class CrossMatchResults:
    def __init__(self, **kwargs):
        self.dbs = []
        self.matches = {}
        self.nameLookup = {}

    def setBaseNames(self, baseNames):
        self.baseNames = baseNames
        
    def addResult(self, db, dbio, result):
        for baseid,baseidResults in result.items():
            baseName = self.baseNames[baseid]
            self.nameLookup[baseid] = baseName
            for compareid,matchResult in baseidResults.items():
                rank = matchResult["Rank"].max(axis=1)
                if rank["Medium"] == 0:
                    continue
                quality = None
                if rank["Tight"] >= 4:
                    quality = "Pure"
                elif rank["Tight"] >= 2 and rank["Medium"] >= 3:
                    quality = "Great"
                elif rank["Tight"] >= 1 and rank["Medium"] >= 2:
                    quality = "Good"
                elif rank["Tight"] >= 1:
                    quality = "Near"
                elif rank["Medium"] >= 1:
                    quality = "Loose"
                else:
                    print(rank)

                compName = dbio.namesData.loc[compareid,"Name"]
                self.nameLookup[(db,compareid)] = compName

                self.matches[(db,quality,baseid,compareid)] = {"CompName": compName, "Rank": rank, "Info": dbio.namesData.loc[compareid].to_dict(), "Media": list(matchResult["Raw"]["BestBaseMatch"].keys())}        
        
    def get(self):
        print("==== Match Results ====")
        dbMatches = Series(self.matches).reset_index()
        dbMatches.columns = ["DB", "Quality", "BaseID", "CompareID", "Match"]
        
        print("By DB:")
        print(dbMatches["DB"].value_counts())
        print("By Quality:")
        print(dbMatches["Quality"].value_counts())
        
        return dbMatches
        
    def save(self):
        io    = FileIO()
        mdbpd = MusicDBPermDir()
        savename = mdbpd.getMatchPermPath().join("crossMatch.p")
        print("Saving Primary Match Results To {0}".format(savename.str))
        io.save(idata=self.get(), ifile=savename)
        
        savename = mdbpd.getMatchPermPath().join("crossMatchNames.p")
        print("Saving Primary Match Names To {0}".format(savename.str))
        io.save(idata=self.nameLookup, ifile=savename)
    