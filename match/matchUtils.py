from pandas import DataFrame, concat
from numpy import array_split
from multiprocessing import Pool
from functools import partial
from tqdm import tqdm
from time import sleep

from matchArtistToDB import matchArtistToDB
from masterIgnoreID import masterIgnoreID
from masterDBGate import masterDBGate
from listUtils import getFlatList
from timeUtils import timestat

from urllib.parse import urlparse
from artistIDBase import artistIDBase


def getUnmatchedArtistsFromDB(dbToUse, mdbData, mam, mmeDF, debug=False):
    print("="*10,dbToUse,"="*10)

    dbArtistNumAlbumData = mdbData.getDBBasicInfo(dbToUse)
    if debug:
        print("Found {0: >7} Artists IDs for [{1}]".format(dbArtistNumAlbumData.shape[0], dbToUse))

    matchedDBIDs = mmeDF[dbToUse][mmeDF[dbToUse].notna()]
    if debug:
        print("Found {0: >7} Previously Matched IDs for [{1}]".format(len(matchedDBIDs), dbToUse))

    mergedIDs = getFlatList([x["MergeData"].keys() for x in mam.getMergerDataByDB(dbToUse).values])
    if debug:
        print("Found {0: >7} Previously Merged IDs for [{1}]".format(len(mergedIDs), dbToUse))

    miid          = masterIgnoreID()
    mdbGate       = masterDBGate()

    dbArtistsToFind = dbArtistNumAlbumData.copy(deep=True)
    if debug:
        print("  {0: <8}: {1}".format(dbArtistsToFind.shape[0], "All DB Artists"))
    dbArtistsToFind = dbArtistsToFind[~dbArtistsToFind.index.isin(matchedDBIDs)]
    if debug:
        print("  {0: <8}: {1}".format(dbArtistsToFind.shape[0], "Unmatched DB Artists"))
    #dbArtistsToFind = dbArtistsToFind[~dbArtistsToFind.index.isin(mergedIDs)]
    #if debug:
    #    print("  {0: <8}: {1}".format(dbArtistsToFind.shape[0], "Unmatched/Unmerged DB Artists"))    
    
    ignoreIDs = miid.getIgnoreDBIDs(dbToUse)
    dbArtistsToFind = dbArtistsToFind[~dbArtistsToFind.index.isin(ignoreIDs)]
    if debug:
        print("  {0: <8}: {1}".format(dbArtistsToFind.shape[0], "Unmatched/Unmerged/Unignored DB Artists"))

    retval = {"dbArtistsToFind": dbArtistsToFind, "Stats": {nRows: dbArtistsToFind.iloc[nRows]["NumAlbums"] for nRows in [x for x in [0, 100, 500, 2000, 5000] if x < dbArtistsToFind.shape[0]]}}
    del dbArtistNumAlbumData
    return retval


def getUnmatchedStatistics(mdbGate):
    unmatchedDBArtistStats = {}
    ts = timestat("Collecting Unmatched DB Artists Stats")
    for dbToUse in mDBGate.getDBs():
        retval = getUnmatchedArtistsFromDB(dbToUse, mDBGate, mam, debug=False)
        unmatchedDBArtistStats[dbToUse] = retval["Stats"]
        del retval
    ts.stop()
    unmatchedDBArtistStats = DataFrame(unmatchedDBArtistStats).T.astype(int, errors="ignore").T
    return unmatchedDBArtistStats


def getArtistsToMatch(dbArtistsToFind, kwargs):
    minArtistAlbums = kwargs.get('minArtistAlbums', 2)
    maxArtistAlbums = kwargs.get('maxArtistAlbums', None)
    maxNumArtists   = kwargs.get('maxNumArtists', None)
    shuffleArtists  = kwargs.get('shuffleArtists', False)
    
    if minArtistAlbums is not None and maxArtistAlbums is not None:
        idxs = ((dbArtistsToFind["NumAlbums"] >= minArtistAlbums) & (dbArtistsToFind["NumAlbums"] < maxArtistAlbums))
    elif minArtistAlbums is not None:
        idxs = dbArtistsToFind["NumAlbums"] >= minArtistAlbums
    elif maxArtistAlbums is not None:
        idxs = dbArtistsToFind["NumAlbums"] < maxArtistAlbums
    else:
        idxs = None

    uniqueArtistsDF   = dbArtistsToFind[idxs] if idxs is not None else dbArtistsToFind
    uniqueArtistsDF   = uniqueArtistsDF if shuffleArtists is False else dbArtistsToFind[idxs].sample(frac=1)
    uniqueArtistNames = uniqueArtistsDF.head(maxNumArtists)["ArtistName"] if isinstance(maxNumArtists,int) else uniqueArtistsDF["ArtistName"]

    return uniqueArtistNames


def computeDBMatchStats():
    mdbData    = masterDBData(dbs=None)
    mdbData.load(loadAlbums=False, requireAlbums=0)

    infos = {db: mdbData.getDBBasicInfo(db) for db in mdbData.dbs}
    artists = {db: {} for db in mdbData.dbs}
    for db,info in infos.items():
        artists[db]['All'] = info[info["NumAlbums"] >=0].shape[0]
        artists[db]['>=1'] = info[info["NumAlbums"] >=1].shape[0]
        artists[db]['>=2'] = info[info["NumAlbums"] >=2].shape[0]
        artists[db]['>=3'] = info[info["NumAlbums"] >=3].shape[0]
    DataFrame(artists).T
    
    
    

#############################################################################################################################
# Main Run Code
#############################################################################################################################        
def serialRun(mmg, params):
    mfunc      = matchRunData
    runData    = mmg.getRunData()
    runResults = []
    if not isinstance(runData,DataFrame):
        return []
    
    #for result in tqdm([mfunc(item, **self.params) for item in self.runData.iterrows()]):
    #    self.runResults.append(result)
    N  = runData.shape[0]
    ts = timestat("Serial Run For {0} Artists".format(N))
    for n,item in enumerate(runData.iterrows()):
        runResults.append(mfunc(item, **params))
        ts.update(n=n+1,N=N)
    ts.stop()
    return runResults


def multiProcRunX(mmg, numProcs, params):
    ####################################################
    # Matching Function w/ Thresholds
    ####################################################
    mfunc   = matchRunData
    pfunc   = partial(mfunc, **params)
    runData = mmg.getRunData()
    if not isinstance(runData,DataFrame):
        return []

    
    
    ####################################################
    # Match Data (Albums With Index+Artist Data)
    ####################################################
    argument_list = list(runData.iterrows())
    #print(pfunc)
    #print(argument_list)
    #return []

    ####################################################
    # Multi Processes Matching
    ####################################################
    runResults = multiProc(func=pfunc, argument_list=argument_list, num_processes=numProcs)
    return runResults


#############################################################################################################################
# General TQDM Processing
#############################################################################################################################
def multiProc(func, argument_list, num_processes):
    pool = Pool(processes=num_processes)
    result_list_tqdm = []
    for result in tqdm(pool.imap(func=func, iterable=argument_list), total=len(argument_list)):
        result_list_tqdm.append(result)
        
    pool.terminate()
    sleep(1)
    pool.terminate()
    return result_list_tqdm


#############################################################################################################################
# Matching Code (Once Per Artist w/ Albums) 
#   Requires a few global variables (matchData.mdbData)
#############################################################################################################################
def matchRowData(row, **kwargs):
    matchData = kwargs['matchData']
    matdb = matchArtistToDB(matchData.mdbData, detail=kwargs.get("detail",0))
    matdb.setArtistInfo(row)
    matdb.setThresholds(**kwargs)
    if kwargs.get("by") == "Artist":
        matdb.findArtistMatches()
        retval = matdb.getArtistMatchResults()
    elif kwargs.get("by") == "Album":
        matdb.findArtistAlbumMatches()
        retval = matdb.getAlbumMatchResults()
    else:
        raise ValueError("Unknown 'by' Value [{0}]".format(kwargs.get('by')))
    del matdb
    return retval    
    
def matchDataFrameData(df, **kwargs):
    retval = df.apply(matchRowData, **kwargs, axis=1)
    return retval


#############################################################################################################################
# Parallelize Matching Code
#############################################################################################################################
def multiProcRun(mmg, numProcs, params):
    df     = mmg.getRunData()
    func   = partial(matchDataFrameData, **params)
    retval = parallelizeMatchDataFrameData(df, func, numProcs)
    return retval


def parallelizeMatchDataFrameData(df, func, n_cores=3):
    if not isinstance(df, DataFrame):
        return df
    if df.shape[0] == 0:
        return df
    n_cores  = min([n_cores, df.shape[0]])
    df_split = array_split(df, n_cores)
    pool     = Pool(n_cores)
    df       = concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df
    
    
def matchRunData(item, *args, **kwargs):
    #idx,artistData = item
    matchData = kwargs['matchData']
    matdb = matchArtistToDB(matchData.mdbData, detail=kwargs.get("detail",0))
    #matdb = matchArtistToDB(kwargs['mdb'])
    matdb.setArtistInfo(artistData)
    matdb.setThresholds(**kwargs)
    if kwargs.get("by") == "Artist":
        matdb.findArtistMatches()
        retval = matdb.getArtistMatchResults()
    elif kwargs.get("by") == "Album":
        matdb.findArtistAlbumMatches()
        retval = matdb.getAlbumMatchResults()
    else:
        raise ValueError("Unknown 'by' Value [{0}]".format(kwargs.get('by')))
    del matdb
    return retval

def func(x):
    print(x.shape)
    return [type(x),x.index]
