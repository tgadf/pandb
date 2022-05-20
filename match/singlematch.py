""" Single DB Matching Classes """

__all__ = ["SingleMatchDB", "SingleCrossMatchDB"]

from timeutils import Timestat
from .dataio import MatchDBDataIO
from .base import MatchDBBase
from .results import PrimaryMatchResults, CrossMatchResults
from .utils import printIntro
from .pool import poolMatchNames
from pandas import DataFrame, Series, concat
from numpy import array_split, ceil


class SingleMatchDB(MatchDBBase):
    def __init__(self, baseDB: str, compareDB: str, reqs: dict, **kwargs):
        super().__init__(reqs, **kwargs)        
        print(f"| ChunkSize ==> {self.params.getChunkSize()}")
        
        mask = self.params.getMask()
        mask = (baseDB,compareDB)
        self.baseIO = MatchDBDataIO(db=baseDB, mediaTypes=self.params.mediaTypes, mask=mask, verbose=True, base=True)
        self.validDB(self.baseIO.db)
        
        mask = compareDB
        self.compareIO = MatchDBDataIO(db=compareDB, mediaTypes=self.params.mediaTypes, mask=mask, verbose=True, base=False)
        self.validDB(self.compareIO.db)
        
        self.mres = PrimaryMatchResults(self.baseIO)
        self.save = self.mres.save
        

    def match(self, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)
        tsMatch = Timestat(f"Matching [{self.baseIO.db}] Against [{self.compareIO.db}]", ind=0)
        
        ###################################################################################################################################################
        ## Load Base DB Names
        ###################################################################################################################################################
        baseIO = self.baseIO
        printIntro(baseIO.db, delimiter='-')
        if verbose: ts = Timestat(f"Loading {baseIO.db} Artist Names", ind=2)
        baseIO.loadNames()
        baseIO.setAvailableNames(self.getDBReq(baseIO.db))
        if verbose: ts.stop()

        ###################################################################################################################################################
        ## Load Compare DB Names
        ###################################################################################################################################################
        compareIO = self.compareIO
        printIntro(compareIO.db, delimiter='-')
        if verbose: ts = Timestat(f"Loading {compareIO.db} Artist Names", ind=2)
        compareIO.loadNames()
        compareIO.setAvailableNames(self.getDBReq(compareIO.db))
        if verbose: ts.stop()


        ###################################################################################################################################################
        ## Serialize Jobs Into 1000 Artist Chunks
        ###################################################################################################################################################
        numNames = baseIO.getNumNames()
        n = int(ceil(numNames / self.params.getChunkSize()))
        print(f"Splitting {baseIO.db} Names Into {numNames}/{self.params.getChunkSize()} ==> {n} Chunks")
        splitAvailableNames = array_split(baseIO.getAvailableNames(), n)
        for i,availableNames in enumerate(splitAvailableNames):
            printIntro(f"Chunk {i+1}/{n}", delimiter='.')

            ########################################################################################################################################
            ## 1) Match Artist Names
            ########################################################################################################################################
            if verbose: ts = Timestat(f"String Matching {len(availableNames)} [{baseIO.db}] x {compareIO.getNumNames()} [{compareIO.db}] Artist Names", ind=2)
            artistMatchResults = poolMatchNames(baseNames=availableNames, compNames=compareIO.getAvailableNames(), nCores=self.getPart(), progress=True, cutoff=self.getMatchNameReq())
            if verbose: ts.stop()

            artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults)
            if artistNameMatches.shape[0] > 0:
                mediaData          = self.prepareMediaData(artistNameMatches, baseIO, compareIO)
                del artistMatchResults
                del artistNameMatches


                ########################################################################################################################################
                ## 2) Match Artist Albums Names
                ########################################################################################################################################
                if verbose: ts = Timestat(f"String Matching {mediaData.shape[0]} [{baseIO.db}] Album Names", ind=2)
                albumMatchResults = self.matchMediaDataPool(mediaData)
                self.mres.addResult(compareIO.db, compareIO, albumMatchResults)
                del albumMatchResults
                del mediaData
                compareIO.mediaData = None
                if verbose: self.mres.show()
                if verbose: ts.stop()

        tsMatch.stop()
    
    
    ################################################################################################################################################
    ## Prepare Media Data For Match
    ################################################################################################################################################
    def prepareMediaData(self, artistNameMatches: Series, baseIO: MatchDBDataIO, compareIO: MatchDBDataIO) -> 'DataFrame':
        nameMatchValues = {}
        for baseid,compareValues in artistNameMatches.iteritems():
            for compareid,value in compareValues.items():
                key   = (baseid,compareid)
                nameMatchValues[key] = value
        baseids = [baseid for baseid,_ in Series(nameMatchValues).groupby(level=0)]
        compids = [compid for compid,_ in Series(nameMatchValues).groupby(level=1)]

        if self.verbose: ts = Timestat("Loading {0} Media Data".format(baseIO.db), ind=4)
        baseIO.loadMedia()
        baseMediaData = baseIO.getAvailableMedia()
        if self.verbose: ts.stop()
            
        if self.verbose: ts = Timestat("Loading {0} Media Data".format(compareIO.db), ind=4)
        compareIO.loadMedia(ids=compids)
        compareMediaData = compareIO.getAvailableMedia()
        if self.verbose: ts.stop()
        
        mediaData = {}
        for key in nameMatchValues.keys():
            baseid,compid = key
            mediaData[key] = {"Base": Series(baseMediaData[baseid]), "Compare": Series(compareMediaData[compid])}
        mediaData = Series(mediaData)
        return mediaData
    
    
    
            
        
class SingleCrossMatchDB(MatchDBBase):
    def __init__(self, compareDB, mres: DataFrame, reqs: dict, **kwargs):
        super().__init__(reqs, **kwargs)
        
        self.compareIO = MatchDBDataIO(db=compareDB, mediaTypes=self.params.mediaTypes, mask=False, verbose=False, base=False)
        self.validDB(self.compareIO.db)
        
        self.mres = mres
        self.cmres = CrossMatchResults()
        self.save = self.cmres.save
        

    def match(self, **kwargs):
        verbose = kwargs.get('verbose', self.verbose)
        try:
            baseDBs = list(self.mres["DB"].unique())
        except:
            print("No dbs to cross match...")
            return
        
        tsMatch = Timestat(f"Cross Matching [{self.compareIO.db}] Against {baseDBs}", ind=0)
        
        compareIO = self.compareIO        
        compareIO.loadNames()
        compareIO.setAvailableNames(self.getDBReq(compareIO.db))
                
        index = self.mres.apply(lambda row: (row["BaseID"],row["DB"],row["CompareID"]), axis=1)
        baseNames = self.mres['Match'].apply(lambda x: x["Info"]["Name"])
        baseMediaData = self.mres["Match"].apply(lambda x: x["Media"])
        baseNames.index = index
        baseNames.name = "Name"
        baseMediaData.index = index
        baseMediaData.name = "Media"
        self.cmres.setBaseNames(baseNames)

        ########################################################################################################################################
        ## 1) Match Artist Names
        ########################################################################################################################################
        if verbose: ts = Timestat(f"String Matching {len(baseNames)} {baseDBs} x {compareIO.getNumNames()} [{compareIO.db}] Artist Names", ind=2)
        artistMatchResults = poolMatchNames(baseNames=baseNames, compNames=compareIO.getAvailableNames(), nCores=self.getPart(), progress=True)
        artistNameMatches  = self.selectArtistsForMediaMatch(artistMatchResults)
        mediaData          = self.prepareMediaData(artistNameMatches, baseMediaData, compareIO)
        del artistMatchResults
        del artistNameMatches
        if verbose: ts.stop()
            
            
        ########################################################################################################################################
        ## 2) Match Artist Albums Names
        ########################################################################################################################################
        if verbose: ts = Timestat(f"String Matching {mediaData.shape[0]} {baseDBs} Album Names", ind=2)
        albumMatchResults = self.matchMediaDataPool(mediaData)
        self.cmres.addResult(compareIO.db, compareIO, albumMatchResults)
        del albumMatchResults
        del mediaData
        if verbose: ts.stop()
                
        del compareIO

    
    ################################################################################################################################################
    ## Prepare Media Data For Match
    ################################################################################################################################################
    def prepareMediaData(self, artistNameMatches: Series, baseMediaData: Series, compareIO: MatchDBDataIO) -> 'DataFrame':
        nameMatchValues = {}
        for baseid,compareValues in artistNameMatches.iteritems():
            for compareid,value in compareValues.items():
                key   = (baseid,compareid)
                nameMatchValues[key] = value
        compids = [compid for compid,_ in Series(nameMatchValues).groupby(level=1)]        
        compareIO.loadMedia(ids=compids)
        compareMediaData = compareIO.getAvailableMedia()        

        mediaData = {}
        for key in nameMatchValues.keys():
            baseid,compid = key
            mediaData[key] = {"Base": Series(baseMediaData[baseid]), "Compare": Series(compareMediaData[compid])}
        mediaData = Series(mediaData)
        return mediaData