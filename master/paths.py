""" Master Param Data """

__all__ = ["MasterPaths"]

from fileutils import DirInfo

##################################################################################################################################
# Master List of Paths
##################################################################################################################################
class MasterPaths:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        mkDirs = kwargs.get('mkDirs', False)
        rawPathDrive  = DirInfo("/Volumes/Piggy")
        modPathDrive  = DirInfo("/Volumes/Seagate")
        sumPathDrive  = DirInfo("/Users/tgadfort/Music")
        
        if not sumPathDrive.exists():
            raise ValueError("Sum Drive [{0}] Does Not Exist.".format(sumPathDrive.str))
                        
        if mkDirs:
            if not rawPathDrive.exists():
                print("Raw Drive [{0}] Does Not Exist. Setting To [{1}]".format(rawPathDrive.str, sumPathDrive.str))
                rawPathDrive = sumPathDrive
        
            if not modPathDrive.exists():
                print("Mod Drive [{0}] Does Not Exist. Setting To [{1}]".format(rawPathDrive.str, sumPathDrive.str))
                modPathDrive = sumPathDrive
        
        self.rawPath  = rawPathDrive.join("Discog")
        self.modPath  = modPathDrive.join("Discog")
        self.sumPath  = sumPathDrive.join("Discog")
        if verbose is True:
            print("MasterPaths()")
            print("{0: <18}{1}".format("  ==> Raw:", self.rawPath.str))
            print("{0: <18}{1}".format("  ==> Mod:", self.modPath.str))
            print("{0: <18}{1}".format("  ==> Sum:", self.sumPath.str))
    
    def getRawPath(self):
        return self.rawPath

    def getModPath(self):
        return self.modPath

    def getSumPath(self):
        return self.sumPath