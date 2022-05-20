""" Master DB Data """

__all__ = ["MasterMetas"]

##################################################################################################################################
# Master List of Metas
##################################################################################################################################
class MasterMetas:
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        self.medias = {"A": "Album", "B": "SingleEP", "C": "Appearance", "D": "Technical", "E": "Mix", "F": "Bootleg", "G": "AltMedia", "H": "Other"}
        self.mediaAlbums = [self.medias['A'],self.medias['B']]
        self.metas  = {"Basic": ["Name", "Ref", "NumAlbums"], "Media": ["{0}Media".format(media) for media in self.medias.values()],
                       "Genre": ["Genre"], "Bio": ["Bio"], "Link": ["Link"], "Metric": ["Metric"], "Counts": ["Counts"]}
        self.searches = ["Name"] + ["{0}Media".format(media) for media in ["Album", "SingleEP", "Appearance", "Technical", "Mix", "Bootleg", "AltMedia", "Other"]]
        if verbose is True:
            print("MasterMetas()")
            print("{0: <18}{1}".format("  ==> Media:", list(self.medias.values())))
            print("{0: <18}{1}".format("  ==> Metas:", list(self.metas.keys())))
            print("{0: <18}{1}".format("  ==> Searches:", list(self.searches)))
        
    def getMedias(self):
        return self.medias
    
    def getMetas(self):
        return self.metas
    
    def getSearches(self):
        return self.searches