""" Master DB Data """

__all__ = ["MasterDBs"]

##################################################################################################################################
# Master List of Databases
##################################################################################################################################
class MasterDBs:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.dbs = ["Discogs", "Spotify", "LastFM", "Genius", "RateYourMusic", "MetalArchives", "Deezer", "AllMusic", "MusicBrainz",
                    "AlbumOfTheYear", "SetListFM", "Beatport", "Traxsource", "MyMixTapez", "ClassicalArchives"]
        self.valid = {db: True for db in self.dbs}
        if verbose is True:
            print("MasterDBs()")
            print("{0: <18}{1}".format("  ==> DBs:", self.dbs))
        
    def isValid(self, db):
        return self.valid.get(db, False)
    
    def getDBs(self):
        return self.dbs