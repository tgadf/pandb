""" Matching Utils """

__all__ = ["write", "printIntro"]

from pandas import Series
        

def printIntro(name, length=175, delimiter='*'):
    empty  = " "*(int(length/2 - 10))
    remain = " "*(length - len(name) - 2 - len(empty) - len(delimiter)*4)
    print("")
    print(delimiter*length)
    print(f"{delimiter*2}{empty}{name}(){remain}{delimiter*2}")
    print(delimiter*length)
    
    
def write(indent: int, value: str, objects = None):
    if isinstance(objects,(tuple,list)):
        objs   = {i: obj for i,obj in enumerate(objects)}
        result = value.format(*objs.values())
    elif objects is None:
        result = value
    else:
        try:
            result = value.format(objects)
        except:
            raise TypeError("Did not understand objects [{0}] with type [{1}]".format(objects,type(objects)))
    print(" "*indent,"==>",result)