""" Match Results """

__all__ = ["PrimaryMatchResults", "CrossMatchResults", "MatchQualityNames", "MatchQuality"]

from master import MusicDBPermDir
from ioutils import FileIO
from .dataio import MatchDBDataIO
from .matchlev import getLevenshtein
from pandas import Series

class MatchQuality:
    def __init__(self, rank: dict, baseName: str, compName: str):
        self.mqnames  = MatchQualityNames()
        self.baseName = baseName
        self.compName = compName
        self.rank     = rank
        
        ## Name Quality
        self.setNameQuality(baseName, compName)
        
        ## Media Quality
        self.setMediaQuality(rank)
        
        
    def getNameQuality(self):
        return self.nameQuality
    
    def getMediaQuality(self):
        return self.mediaQuality
        
        
    def setNameQuality(self, baseName, compName):
        similarity = getLevenshtein(baseName, compName)
        if isinstance(similarity,float):
            for qualityName,qualityValue in self.mqnames.nameQualityValues.items():
                if similarity >= qualityValue:
                    self.nameQuality = qualityName
                    break
        else:
            self.nameQuality = None
        
                                    
        
    ####################################################################################################################
    ## Media Quality
    ####################################################################################################################
    def setMediaQuality(self, rank):
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
        
        
class MatchQualityNames:
    def __init__(self, **kwargs):
        self.nameQualityValues  = {"Pure": 1.0, "Great": 0.95, "Good": 0.90, "Near": 0.85, "Low": 0.0}
        self.mediaQualityValues = {"Pure": 8, "Great": 7, "Good": 6, "Sole": 5, "Near": 4, "Loose": 3, "Low": 2, "Poor": 1}
        self.qmap = self.mediaQualityValues
        self.mediaMatchValues   = {"Loose": 0.7, "Medium": 0.8, "Tight": 0.9, "Exact": 0.95}    
                    
    def getNameQualityValues(self):
        return self.nameQualityValues
        
    def getMediaQualityValues(self):
        return self.mediaQualityValues
        
    def getMediaMatchValues(self):
        return self.mediaMatchValues
    
        
class MatchResultsBase:
    def __init__(self, matchType, **kwargs):
        self.dbs        = []
        self.matches    = {}
        self.nameLookup = {}
        self.matchType  = matchType
        self.mqnames    = MatchQualityNames()
                                    
        
    def show(self):
        if len(self.matches) > 0:
            dbMatches = Series(self.matches).reset_index()
            dbMatches.columns = ["DB", "MediaQuality", "NameQuality", "BaseID", "CompareID", "Match"]
            self.showDBQuality(dbMatches)
            self.showNameQuality(dbMatches)
            self.showMediaQuality(dbMatches)
        else:
            print("No Matches To Show")
            
            
    def showDBQuality(self, dbMatches):
        print("By DB:")
        vc = dbMatches["DB"].value_counts()
        for db,dbCount in vc.iteritems():
            print(f"  {db: <20}{dbCount}")
            
    def showNameQuality(self, dbMatches):
        print("By NameQuality:")
        vc = dbMatches["NameQuality"].value_counts()
        for qName,qRank in self.mqnames.nameQualityValues.items():
            print(f"  {qName: <7}{vc.get(qName, 0)}")
            
    def showMediaQuality(self, dbMatches):
        print("By MediaQuality:")
        vc = dbMatches["MediaQuality"].value_counts()
        for qName,qRank in self.mqnames.mediaQualityValues.items():
            print(f"{qName: <7}{vc.get(qName, 0)}")
        
        
    def get(self):
        if len(self.matches) > 0:
            print("==== Match Results ====")
            dbMatches = Series(self.matches).reset_index()
            dbMatches.columns = ["DB", "MediaQuality", "NameQuality", "BaseID", "CompareID", "Match"]
            self.showDBQuality(dbMatches)
            self.showNameQuality(dbMatches)
            self.showMediaQuality(dbMatches)

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