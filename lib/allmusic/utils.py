""" MusicDBIO Utilities """

__all__ = ["moveLocalFiles"]

from fileutils import FileInfo
from master import MasterParams
from .musicdbio import MusicDBIO

def moveLocalFiles():
    mp        = MasterParams()
    mioLocal  = MusicDBIO(local=True,mkDirs=True,debug=True)
    mioGlobal = MusicDBIO(local=False,mkDirs=True,debug=True)
    for modVal in mp.getModVals():
        ## Raw Data
        files = mioLocal.dir.getRawModValDataDir(modVal).glob("*.p")
        _ = [FileInfo(ifile).mvFile(FileInfo(mioGlobal.data.getRawFilename(modVal,ifile.stem))) for ifile in files]