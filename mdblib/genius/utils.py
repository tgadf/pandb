""" MusicDBIO Utilities """

__all__ = ["moveLocalFiles"]

from fileutils import FileInfo
from mdbmaster import MasterParams
from .musicdbio import MusicDBIO

def moveLocalFiles(**kwargs):    
    verbose = kwargs.get('verbose')
    if verbose: print("moveLocalFiles()")
    mp        = MasterParams()
    mioLocal  = MusicDBIO(local=True,mkDirs=True,debug=verbose)
    mioGlobal = MusicDBIO(local=False,mkDirs=True,debug=verbose)
    for modVal in mp.getModVals():
        files = mioLocal.dir.getRawAlbumModValDataDir(modVal).glob("*.p")
        _ = [FileInfo(ifile).mvFile(FileInfo(mioGlobal.data.getRawArtistAlbumFilename(modVal,ifile.stem))) for ifile in files]