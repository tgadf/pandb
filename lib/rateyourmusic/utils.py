""" MusicDBIO Utilities """

__all__ = ["moveLocalFiles", "removeLocalFiles"]

from master import MasterParams
from .musicdbio import MusicDBIO
from fileutils import FileInfo,DirInfo
from ioutils import FileIO
from timeutils import Timestat

def removeLocalFiles(**kwargs):
    verbose   = kwargs.get('verbose', True)
    if verbose: print("removeLocalFiles()")
        
    mioLocal  = DirInfo("/Users/tgadfort/Desktop/RateYourMusic")
    io        = FileIO()    
    print("  ==> Finding Files in {0}: ".format(mioLocal.str), end="")
    files = list(mioLocal.glob("*.htm*"))
    print("  ==> Found {0} Files".format(len(files)))
    for ifile in files:
        FileInfo(ifile).rmFile()
    files = list(mioLocal.glob("*.htm*"))
    print("  ==> There are {0} remaining files".format(len(files)))
        

def moveLocalFiles(**kwargs):
    verbose   = kwargs.get('verbose', True)
    if verbose: print("moveLocalFiles()")
        
    mp        = MasterParams()
    mioGlobal = MusicDBIO(local=False,mkDirs=True,debug=False)
    mioLocal  = DirInfo("/Users/tgadfort/Desktop/RateYourMusic")
    io        = FileIO()    
    print("  ==> Finding Files in {0}: ".format(mioLocal.str), end="")
    files = list(mioLocal.glob("*.htm*"))
    print("  ==> Found {0} Files".format(len(files)))
    ts = Timestat("Moving {0} Local Files To Global Directories".format(len(files)))
    for n,ifile in enumerate(files):
        if (n+1) % 25 == 0:
            ts.update(n=n+1,N=len(files))
        data    = io.get(ifile)
        dbID    = mioGlobal.getdbid(data)
        modVal  = mioGlobal.getModVal(dbID)
        dstFile = FileInfo(mioGlobal.data.getRawFilename(modVal,dbID))
        print(ifile,'\t',dstFile.path)
        if dstFile.exists():
            print("  ==> File exists")
            continue
        FileIO().save(idata=open(ifile).read(), ifile=dstFile.path)
        #srcFile = FileInfo(ifile)
        #srcFile.mvFile(dstFile)
    ts.stop()