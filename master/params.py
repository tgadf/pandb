""" Master Param Data """

__all__ = ["MasterParams"]

from .basic import MasterBasic
from .paths import MasterPaths
from .metas import MasterMetas
from .dbs import MasterDBs

##################################################################################################################################
# Master List of Params
##################################################################################################################################
class MasterParams():
    def __init__(self, **kwargs):
        verbose = kwargs.get('verbose', False)
        for mCls in [MasterBasic(**kwargs),MasterPaths(**kwargs), MasterMetas(**kwargs), MasterDBs(**kwargs)]:
            for method in [attribute for attribute in dir(mCls) if callable(getattr(mCls, attribute)) and attribute.startswith('__') is False]:
                exec("self.{0} = mCls.{0}".format(method))