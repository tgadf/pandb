""" Pool/MultiProc Matching Code """

__all__ = ["poolMatchNames", "poolMatchAlbums"]

from timeutils import Timestat
from .matchlev import getLevenshtein
from tqdm import tqdm
from multiprocessing import Pool
from pandas import concat, Series, DataFrame
from numpy import array_split
from functools import partial
from typing import Union    
    
#########################################################################################################################
# Matching Code
#########################################################################################################################
def poolMatchNamesRunner(item, *args, **kwargs):
    return poolMatchStringSeries(base=item, compare=kwargs.get('comp'), showProgress=kwargs.get('progress', False))
    
def poolMatchAlbumsRunner(item, *args, **kwargs):
    if kwargs.get('progress') is True:
        retval = Series({key: poolMatchStringSeries(value["Base"], value["Compare"], showProgress=False) for key,value in tqdm(iterable=item.items(), total=len(item))})
    else:
        retval = Series({key: poolMatchStringSeries(value["Base"], value["Compare"], showProgress=False) for key,value in item.items()})
    return retval
    
def poolMatchStringSeries(base: Series, compare: Series, showProgress=True):
    if showProgress is True:
        retval = Series({baseid: compare.apply(getLevenshtein, x2=baseName) for baseid,baseName in tqdm(iterable=base.iteritems(), total=len(base))})
    else:
        retval = Series({baseid: compare.apply(getLevenshtein, x2=baseName) for baseid,baseName in base.iteritems()})
    return retval


#########################################################################################################################
# Artist Matching
#########################################################################################################################
def poolMatchNames(baseNames: Series, compNames: Series, **kwargs):
    nCores  = kwargs.get("nCores", 3)
    verbose = kwargs.get("verbose", False)
    baseNamesSplit = array_split(baseNames, nCores)
    pool   = Pool(nCores)
    kwargs = {"comp": compNames, 'progress': kwargs.get('progress')}
    pFunc  = partial(poolMatchNamesRunner, **kwargs)
    if verbose: ts = Timestat("Matching {0} x {1} Names".format(len(baseNames),len(compNames)))
    retval = concat(pool.map(func=pFunc, iterable=baseNamesSplit))
    pool.close()
    pool.join()
    if verbose: ts.stop()
    return retval
        
    
#########################################################################################################################
# Album Matching
#########################################################################################################################
def poolMatchAlbums(mediaData: Union[DataFrame,Series], **kwargs):
    nCores  = kwargs.get("nCores", 2)
    verbose = kwargs.get("verbose", False)

    if False:
        matchData = {}
        for baseid,row in mediaData.iterrows():
            for compareid,compareIDMedia in row["CompareMedia"].items():
                key   = (baseid,compareid)
                value = {"Base": Series(row["BaseMedia"]["Media"]), "Compare": Series(compareIDMedia["Media"])}
                matchData[key] = value
        splitMediaData = array_split(Series(matchData).sample(frac=1), nCores)
    else:
        splitMediaData = array_split(mediaData.sample(frac=1), nCores)
        
        
    pool   = Pool(nCores)
    kwargs = {'progress': kwargs.get('progress', False)}
    pFunc  = partial(poolMatchAlbumsRunner, **kwargs)
    if verbose: ts = Timestat("Matching {0} Artists' Albums".format(len(mediaData)), ind=6)
    retval = concat(pool.map(func=pFunc, iterable=splitMediaData))
    pool.close()
    pool.join()
    if verbose: ts.stop()
    return retval