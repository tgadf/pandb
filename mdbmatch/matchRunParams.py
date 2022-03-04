""" Match Run Parameters """

__all__ = ["MatchRunParams"]

class MatchRunParams:
    def __init__(self, debug=False):
        self.debug = debug

        runDefs = []
        runDefs.append({"minAlbums": 4000, "maxAlbums": 99999999, "maxArtists": 3})
        runDefs.append({"minAlbums": 3000, "maxAlbums": 4000, "maxArtists": 3})
        runDefs.append({"minAlbums": 2500, "maxAlbums": 3000, "maxArtists": 3})
        runDefs.append({"minAlbums": 2000, "maxAlbums": 2500, "maxArtists": 3})
        runDefs.append({"minAlbums": 1500, "maxAlbums": 2000, "maxArtists": 6})
        runDefs.append({"minAlbums": 1000, "maxAlbums": 1500, "maxArtists": 10})
        runDefs.append({"minAlbums": 500, "maxAlbums": 1000, "maxArtists": 20})
        runDefs.append({"minAlbums": 250, "maxAlbums": 500, "maxArtists": 40})
        runDefs.append({"minAlbums": 100, "maxAlbums": 250, "maxArtists": 100})
        runDefs.append({"minAlbums": 75, "maxAlbums": 100, "maxArtists": 150})
        runDefs.append({"minAlbums": 60, "maxAlbums": 75, "maxArtists": 150})
        runDefs.append({"minAlbums": 50, "maxAlbums": 60, "maxArtists": 200})
        runDefs.append({"minAlbums": 40, "maxAlbums": 50, "maxArtists": 350})
        runDefs.append({"minAlbums": 30, "maxAlbums": 40, "maxArtists": 500})
        runDefs.append({"minAlbums": 25, "maxAlbums": 30, "maxArtists": 500})
        runDefs.append({"minAlbums": 20, "maxAlbums": 25, "maxArtists": 500})
        runDefs.append({"minAlbums": 15, "maxAlbums": 20, "maxArtists": 500})
        runDefs.append({"minAlbums": 10, "maxAlbums": 15, "maxArtists": 1000})
        runDefs.append({"minAlbums": 8, "maxAlbums": 10, "maxArtists": 1500})
        runDefs.append({"minAlbums": 5, "maxAlbums": 8, "maxArtists": 2000})
        runDefs.append({"minAlbums": 2, "maxAlbums": 5, "maxArtists": 4000})
        runDefs.append({"minAlbums": 1, "maxAlbums": 2, "maxArtists": 5000})
        
        runDefs = []
        runDefs.append({"minAlbums": 1000, "maxAlbums": 99999999, "maxArtists": 20})
        runDefs.append({"minAlbums": 100, "maxAlbums": 1000, "maxArtists": 100})
        runDefs.append({"minAlbums": 20, "maxAlbums": 100, "maxArtists": 500})
        runDefs.append({"minAlbums": 5, "maxAlbums": 20, "maxArtists": 1000})
        runDefs.append({"minAlbums": 2, "maxAlbums": 5, "maxArtists": 2500})
        runDefs.append({"minAlbums": 1, "maxAlbums": 2, "maxArtists": 5000})


        matchThresholds = []
        matchThresholds.append({"minAlbums": 4000, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 3000, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 2500, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 2500, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 2000, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 1500, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 1000, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 500, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 250, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 3.0})
        matchThresholds.append({"minAlbums": 100, 'numArtists': 1, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 2.5})
        matchThresholds.append({"minAlbums": 75, 'numArtists': 2, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 2.25})
        matchThresholds.append({"minAlbums": 60, 'numArtists': 2, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 2.1})
        matchThresholds.append({"minAlbums": 50, 'numArtists': 2, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 2.0})
        matchThresholds.append({"minAlbums": 40, 'numArtists': 2, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 1.975})
        matchThresholds.append({"minAlbums": 30, 'numArtists': 2, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 1.975})
        matchThresholds.append({"minAlbums": 25, 'numArtists': 2, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 1.975})
        matchThresholds.append({"minAlbums": 20, 'numArtists': 2, 'artistCutoff': 0.95, 'albumCutoff': 0.9, 'numAlbums': 3, 'score': 1.95})
        matchThresholds.append({"minAlbums": 15, 'numArtists': 4, 'artistCutoff': 0.90, 'albumCutoff': 0.9, 'numAlbums': 2, 'score': 1.9})
        matchThresholds.append({"minAlbums": 10, 'numArtists': 5, 'artistCutoff': 0.90, 'albumCutoff': 0.9, 'numAlbums': 2, 'score': 1.9})
        matchThresholds.append({"minAlbums": 8, 'numArtists': 8, 'artistCutoff': 0.90, 'albumCutoff': 0.9, 'numAlbums': 2, 'score': 1.8})
        matchThresholds.append({"minAlbums": 5, 'numArtists': 8, 'artistCutoff': 0.90, 'albumCutoff': 0.9, 'numAlbums': 2, 'score': 1.8})
        matchThresholds.append({"minAlbums": 3, 'numArtists': 10, 'artistCutoff': 0.90, 'albumCutoff': 0.9, 'numAlbums': 2, 'score': 1.65})
        matchThresholds.append({"minAlbums": 2, 'numArtists': 20, 'artistCutoff': 0.90, 'albumCutoff': 0.925, 'numAlbums': 1, 'score': 1.6})
        matchThresholds.append({"minAlbums": 1, 'numArtists': 50, 'artistCutoff': 0.90, 'albumCutoff': 0.95, 'numAlbums': 1, 'score': 0.95})

        runParams = []
        for runDef in runDefs:
            for runThreshold in matchThresholds:
                if runThreshold["minAlbums"] == runDef["minAlbums"]:
                    runParams.append((runDef,{k: v for k,v in runThreshold.items() if k not in ["minAlbums"]}))
                    break
                    
        self.runParams = runParams

    def getRunParams(self):
        return self.runParams