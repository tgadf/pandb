""" Primary Music Database IO """

__all__ = ["PanDBIO"]

from .utils import PanDBUtils
from .metrics import PanDBMetrics


###############################################################################
# Sole access point to PanDB database (DataFrame) object
###############################################################################
class PanDBIO:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)

        utils = PanDBUtils(**kwargs)
        metrics = PanDBMetrics(**kwargs)
        
        attributeList = {attribute: getattr(metrics, attribute) for attribute in dir(metrics)}
        for attribute, attribute_value in attributeList.items():
            if callable(attribute_value) and attribute.startswith('__') is False:
                exec(f"self.{attribute} = metrics.{attribute}")
        
        attributeList = {attribute: getattr(utils, attribute) for attribute in dir(utils)}
        for attribute, attribute_value in attributeList.items():
            if callable(attribute_value) and attribute.startswith('__') is False:
                exec(f"self.{attribute} = utils.{attribute}")
                
    def tag(self):
        self.addMetrics()
        self.setData(force=True)
        self.setIndex()