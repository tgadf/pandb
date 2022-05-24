""" Primary Music Database IO """

__all__ = ["PanDBIO"]

from base import MusicDBDir, MusicDBData
from master import MusicDBPermDir, MasterDBs
from gate import IDStore
from .utils import PanDBUtils
from .metrics import PanDBMetrics

####################################################################################################################    
## Summary
####################################################################################################################    
class PanDBIO:
    def __init__(self, **kwargs):
        utils    = PanDBUtils(**kwargs)
        metrics  = PanDBMetrics(**kwargs)
        
        attributeList = {attribute: getattr(metrics, attribute) for attribute in dir(metrics)}
        for attribute,attribute_value in attributeList.items():
            if callable(attribute_value) and attribute.startswith('__') == False:
                exec(f"self.{attribute} = metrics.{attribute}")
        
        attributeList = {attribute: getattr(utils, attribute) for attribute in dir(utils)}
        for attribute,attribute_value in attributeList.items():
            if callable(attribute_value) and attribute.startswith('__') == False:
                exec(f"self.{attribute} = utils.{attribute}")
                
        print(dir(self))