""" Master DB Data """

__all__ = ["MasterDBs"]

##################################################################################################################################
# Master List of Databases
##################################################################################################################################
class MasterDBs:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.dbs = ["Discogs", "Spotify", "LastFM", "Genius", "RateYourMusic", "MetalArchives", "Deezer", "AllMusic", "MusicBrainz",
                    "AlbumOfTheYear", "SetListFM", "Beatport", "Traxsource", "MyMixTapez", "ClassicalArchives", "JioSaavn"]
        self.valid = {db: True for db in self.dbs}
        if verbose is True:
            print("MasterDBs()")
            print("{0: <18}{1}".format("  ==> DBs:", self.dbs))
            
        self.dbTypes={'AllMusic': 'Trusted',
                'SetListFM': 'General',
                'Discogs': 'General',
                'Spotify': 'General',
                'LastFM': 'Dump',
                'Genius': 'General',
                'RateYourMusic': 'Trusted',
                'MetalArchives': 'Genre',
                'Deezer': 'Dump',
                'MusicBrainz': 'Trusted',
                'AlbumOfTheYear': 'General',
                'Beatport': 'Genre',
                'Traxsource': 'Genre',
                'MyMixTapez': 'Genre',
                'ClassicalArchives': 'Genre',
                'JioSaavn': 'Genre'}
        
    def isValid(self, db):
        return self.valid.get(db, False)
    
    def getDBType(self, db):
        retval = self.dbTypes.get(db, "Unknown DB!")
        return retval
    
    def getDBs(self):
        return self.dbs
    
    def getDBTypes(self):
        return self.dbTypes