""" Pool Parsing Utilites """

__all__ = ["PoolIO", "poolParseIO", "poolMetaModIO", "poolMetaDBIO", "poolMergeIO", "poolSummaryIO", "poolDataFrame", "CompareData"]

from gate import MusicDBGate
from master import MasterParams
from timeutils import Timestat
from functools import partial
from multiprocessing import Pool
from pandas import concat, Series, DataFrame
from tqdm import tqdm
from time import sleep
from random import random
from numpy import array_split, vectorize
import Levenshtein


##############################################################################################################################
# DBs Utils
##############################################################################################################################
def getDBVals(dbVals):
    if isinstance(dbVals, list):
        pass
    elif isinstance(dbVals, str):
        dbVals=[dbVals]
    elif dbVals is None:
        dbVals=MasterParams().getDBs()
    else:
        raise ValueError("Did not undertstand dbVals [{0}]".format(dbVals))
    return dbVals


##############################################################################################################################
# ModVals Utils
##############################################################################################################################
def getModVals(modVals):
    if isinstance(modVals, list):
        pass
    elif isinstance(modVals, (int,str)):
        modVals=[modVals]
    elif isinstance(modVals, range):
        modVals=list(modVals)
    elif modVals is None:
        modVals = MasterParams().getModVals(listIt=True)
    else:
        raise ValueError("Did not undertstand modVals [{0}]".format(modVals))
    return modVals


##############################################################################################################################
# Function That Parses
##############################################################################################################################
def poolParse(item, *args, **kwargs):
    sleep(random())
    modVal = item
    parser = kwargs['parser']
    del kwargs["parser"]
    
    parser(modVal, **kwargs)

        
##############################################################################################################################
# TQDM Wrapper
##############################################################################################################################
def tqdmMap(func, argument_list, num_processes):
    pool = Pool(processes=num_processes)
    result_list_tqdm = []
    for result in tqdm(pool.imap(func=func, iterable=argument_list), total=len(argument_list)):
        result_list_tqdm.append(result)
    del pool
    return result_list_tqdm
    
    
##############################################################################################################################
# Eval Function
##############################################################################################################################
def poolParse(item, *args, **kwargs):
    sleep(random())
    modVal = item
    parser = kwargs['parser']
    del kwargs["parser"]
    
    parser(modVal, **kwargs)

    
##############################################################################################################################
# Master Pool I/O Function
##############################################################################################################################
def poolMasterIO(parser, modVals, expr="< 0 Days", force=False, numProcs=2):
    num_processes = numProcs
    func          = poolParse
    argument_list = modVals
    kwargs        = {"parser": parser, "expr": expr, "force": force}
    
    ## Create kwargs for pool
     # Giving some arguments for kwargs
    kwargs = {"parser": parser, "expr": expr, "force": force}
    pfunc  = partial(func, **kwargs)

    ts = Timestat("Running imap multiprocessing for {0} mod values ...".format(len(argument_list)))
    result_list = tqdmMap(func=pfunc, argument_list=argument_list, num_processes=num_processes)
    ts.stop()
    
    
##############################################################################################################################
# Pool Summary Function
##############################################################################################################################
def poolSummaryIO(dbVals=None, numProcs=3):
    sumFunction = MusicDBGate().makeSummaryData
    argument_list = getDBVals(dbVals)
    print("poolIO(numProcs={0})".format(numProcs))
    print("  ==> SummaryFunction: {0}".format(sumFunction.__name__))
    print("  ==> DBs:             {0}".format(argument_list))
    kwargs = {}
    pfunc  = partial(sumFunction, **kwargs)
    ts = Timestat("Running imap multiprocessing for {0} mod values ...".format(len(argument_list)))
    result_list = tqdmMap(func=pfunc, argument_list=argument_list, num_processes=numProcs)
    ts.stop()
    
    
##########################################################################################99####################################
# Pool Merge Function
################################################################################################################################
def poolMergeIO(mergeFunction, modVals=None, numProcs=3):
    argument_list = getModVals(modVals)
    print("poolMergeIO(numProcs={0})".format(numProcs))
    print("  ==> MergeFunction: {0}".format(mergeFunction.__name__))
    print("  ==> ModVals:       {0}".format(argument_list))
    kwargs = {}
    pfunc  = partial(mergeFunction, **kwargs)
    ts = Timestat("Running imap multiprocessing for {0} mod values ...".format(len(argument_list)))
    result_list = tqdmMap(func=pfunc, argument_list=argument_list, num_processes=numProcs)
    ts.stop()
    
    
##########################################################################################99####################################
# Pool Metadata Function
################################################################################################################################
def poolMetaModIO(makeFunction, modVals=None, numProcs=3):
    argument_list = getModVals(modVals)
    print("poolMetaIO(numProcs={0})".format(numProcs))
    print("  ==> MakeFunction: {0}".format(makeFunction.__name__))
    print("  ==> ModVals:      {0}".format(argument_list))
    kwargs = {}
    pfunc  = partial(makeFunction, **kwargs)
    ts = Timestat("Running imap multiprocessing for {0} mod values ...".format(len(argument_list)))
    result_list = tqdmMap(func=pfunc, argument_list=argument_list, num_processes=numProcs)
    ts.stop()
    
    
##########################################################################################99####################################
# Pool Metadata Function
################################################################################################################################
def poolMetaDBIO(dbVals=None, numProcs=3):
    makeFunction  = MusicDBGate().makeMetaData
    argument_list = getDBVals(dbVals)
    print("poolMetaIO(numProcs={0})".format(numProcs))
    print("  ==> MakeFunction: {0}".format(makeFunction.__name__))
    print("  ==> DBs:          {0}".format(argument_list))
    kwargs = {}
    pfunc  = partial(makeFunction, **kwargs)
    ts = Timestat("Running imap multiprocessing for {0} mod values ...".format(len(argument_list)))
    result_list = tqdmMap(func=pfunc, argument_list=argument_list, num_processes=numProcs)
    ts.stop()
    
    
    
    
##############################################################################################################################
# Pool Parse RawData Function
##############################################################################################################################
def poolParseIO(parseFunction, modVals=None, expr="< 0 Days", force=False, numProcs=3):
    argument_list = getModVals(modVals)
    print("poolIO(numProcs={0})".format(numProcs))
    print("  ==> ParseFunction: {0}".format(parseFunction.__name__))
    print("  ==> ModVals:       {0}".format(argument_list))
    kwargs = {"expr": expr, "force": force}
    pfunc  = partial(parseFunction, **kwargs)
    ts = Timestat("Running imap multiprocessing for {0} mod values ...".format(len(argument_list)))
    result_list = tqdmMap(func=pfunc, argument_list=argument_list, num_processes=numProcs)
    ts.stop()
    

class CompareData:
    def __init__(self, data, cutoff=0.8):
        self.data = data
        self.cutoff = cutoff
        self.vCompare = vectorize(self.compare)
        
    def getLevenshtein(self, x1, x2):
        try:
            return Levenshtein.ratio(x1, x2)
        except:
            print("ERROR With Levenshtein.ratio({0}, {1})".format(x1,x2))
            return 0.0        
        
    def compare(self, artistName):
        return self.data["Name"].apply(self.getLevenshtein, x2=artistName)
    
    def getNearestNames(self, artistName):
        print(artistName)
        retval = self.data["Name"].apply(self.getLevenshtein, x2=artistName)
        return retval
        #retval = Series(self.vCompare(artistName), index=self.data.index)
        print(artistName,'\t',retval)
        return self.data.index
        #return self.data.index[self.vCompare(artistName) > self.cutoff] #mdbSearchData["Name"])
        return ["Some Index"]
    
    
def poolDataFrame(df, func, n_cores=3):
    df_split = array_split(df, n_cores)
    pool     = Pool(n_cores)
    retval   = pool.map(func, df_split)
    #df       = concat(retval)
    pool.close()
    pool.join()
    return retval
    
    
class PoolIO:
    def __init__(self, db, **kwargs):
        gate = MusicDBGate(**kwargs)
        self.verbose = kwargs.get('verbose', False)
        self.mdbio   = gate.getIO(db=db)
        self.sum     = self.mdbio.sum.make
        self.search  = self.mdbio.search.make
        
        
    def parse(self, force=False):
        poolParseIO(parseFunction=self.mdbio.prd.parse, force=force)

    def merge(self):
        func = "mergeModValData"
        if hasattr(self.mdbio.prd, func) and callable(getattr(self.mdbio.prd, func)):
            poolMergeIO(mergeFunction=getattr(self.mdbio.prd, func))
        
    def meta(self):
        poolMetaModIO(makeFunction=self.mdbio.meta.make)