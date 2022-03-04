""" A few project (root-package) helper functions """

__all__ = ["getRootPath", "getRootName"]

from pathlib import Path

def getRootPath() -> Path:
    return Path(__file__).parent.parent

def getRootName():
    return getRootPath().stem