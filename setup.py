from setuptools import setup, find_namespace_packages
from setuptools.command.install import install
from sys import prefix
from shutil import copyfile
from pathlib import Path

class PostInstallCommand(install):
    def run(self):
        dbDataPrefix = Path(prefix).joinpath("pandb")
        if not dbDataPrefix.exists():
            print("Install: Making Prefix Dir [{0}]".format(dbDataPrefix))
            dbDataPrefix.mkdir()
        dbIgnoreFilename = dbDataPrefix.joinpath("dbIgnoreData.yaml")
        if not dbIgnoreFilename.exists():
            print("Install: Creating Prefix Data From Local Data")
            copyfile("dbIgnoreData.yaml", dbIgnoreFilename)
    
setup(
  name = 'pandb',
  version = '0.0.1',
  #cmdclass={'install': PostInstallCommand},
  data_files = [],
  description = 'Universal Music Database',
  long_description = open('README.md').read(),
  author = 'Thomas Gadfort',
  author_email = 'tgadfort@gmail.com',
  license = "MIT",
  url = 'https://github.com/tgadf/pandb',
  keywords = ['database', 'music'],
  zip_safe = False,
  classifiers = [
    'Development Status :: 3',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules',
    'Topic :: Utilities'
  ],
  install_requires=['jupyter_contrib_nbextensions', 'python-Levenshtein', 'tqdm', 'spotipy'],
  packages=['mdbmaster', 'mdbbase', 'mdbid', 'mdbutils', 'mdbmeta', 'mdbsummary'] 
    + ['musicdb', 'mdbmatch']
    + ['mdblib.genius', 'mdblib.spotify', 'mdblib.discogs', 'mdblib.rateyourmusic', 'mdblib.allmusic', 'mdblib.lastfm', 'mdblib.deezer', 'mdblib.albumoftheyear']
)

## This takes forever so I'm not using it...
#find_namespace_packages(include=['mdblib.*']) 
