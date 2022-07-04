""" MusicDBIO Utilities """

__all__ = ["moveLocalFiles", "moveLocalTarFiles"]

from fileutils import FileInfo
from master import MasterParams
#from lib.discogs import MusicDBIO
from .musicdbio import MusicDBIO


def moveLocalTarFiles(**kwargs):
    verbose = kwargs.get('verbose')
    if verbose: print("moveLocalTarFiles()")
    mp        = MasterParams()
    mioLocal  = MusicDBIO(local=True,mkDirs=True,debug=False)
    mioGlobal = MusicDBIO(local=False,mkDirs=True,debug=False)
    for modVal in mp.getModVals():
        dstDir = mioGlobal.dir.getRawMasterModValDataDir(modVal)
        dstDir.mkDir()
        srcDir = mioLocal.dir.getRawMasterModValDataDir(modVal)
        files  = srcDir.glob("*.tar")
        for ifile in files:
            src   = FileInfo(ifile)
            dst   = dstDir.join(src.name)
            src.mvFile(dst)

def moveLocalFiles(**kwargs):
    verbose = kwargs.get('verbose')
    if verbose: print("moveLocalFiles()")
    mp        = MasterParams()
    mioLocal  = MusicDBIO(local=True,mkDirs=True,debug=True)
    mioGlobal = MusicDBIO(local=False,mkDirs=True,debug=True)
    for modVal in mp.getModVals():
        files = mioLocal.dir.getRawAlbumModValDataDir(modVal).glob("*.p")
        _ = [FileInfo(ifile).mvFile(FileInfo(mioGlobal.data.getRawArtistAlbumFilename(modVal,ifile.stem))) for ifile in files]