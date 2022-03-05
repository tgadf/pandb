""" Pool Parsing Utilites """

__all__ = ["poolIO"]

from timeutils import Timestat
from functools import partial
from multiprocessing import Pool
from tqdm import tqdm
from time import sleep
from random import random



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
# Master Pool I/O Function
##############################################################################################################################
def poolMasterIO(parser, modVals, expr="< 0 Days", force=False, numProcs=2):
    num_processes = numProcs
    func          = poolParse
    argument_list = modVals
    kwargs        = {"parser": parser, "expr": expr, "force": force}
    
    ## Create kwargs for pool
     # Giving some arguments for kwargs
    pfunc = partial(func, **kwargs)

    ts = Timestat("Running imap multiprocessing for {0} mod values ...".format(len(argument_list)))
    result_list = tqdmMap(func=pfunc, argument_list=argument_list, num_processes=num_processes)
    ts.stop()
    
    
##############################################################################################################################
# Master I/O Function
##############################################################################################################################
def poolIO(parseFunction, modVals=None, expr="< 0 Days", force=False, numProcs=3):
    if isinstance(modVals, list):
        pass
    elif isinstance(modVals, (int,str)):
        modVals=[modVals]
    elif isinstance(modVals, range):
        modVals=list(modVals)
    elif modVals is None:
        modVals = list(range(100))
    else:
        raise ValueError("Did not undertstand modVals [{0}]".format(modVals))
    print("poolIO(numProcs={0})".format(numProcs))
    print("  ==> ParseFunction: {0}".format(parseFunction.__name__))
    print("  ==> ModVals:       {0}".format(modVals))
    poolMasterIO(parser=parseFunction, modVals=modVals, expr=expr, force=force, numProcs=numProcs)