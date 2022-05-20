""" Master Param Data """

__all__ = ["MasterBasic"]

##################################################################################################################################
# Master Basic Info
##################################################################################################################################
class MasterBasic:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.maxModValue = 100
        self.projectName = "pandb"
        self.musicdbName = "musicdb"  
        if verbose is True:
            print("MasterBasic()")
            print("{0: <18}{1}".format("  ==> ModVals:", self.maxModValue))
            print("{0: <18}{1}".format("  ==> Project:", self.getProjectName()))
            print("{0: <18}{1}".format("  ==> MusicDB:", self.getMusicDBName())) 

    def getMaxModVal(self):
        return self.maxModValue
    
    def getModVals(self, listIt=False):
        retval = range(self.getMaxModVal())
        retval = list(retval) if listIt is True else retval
        return retval

    def getProjectName(self):
        return self.projectName

    def getMusicDBName(self):
        return self.musicdbName