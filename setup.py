from setuptools import setup, find_namespace_packages
from setuptools.command.install import install
from sys import prefix
from shutil import copyfile
from pathlib import Path

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
  install_requires=['jupyter_contrib_nbextensions', 'python-Levenshtein', 'tqdm', 'spotipy', 'jupyterthemes', 'requests_cache'],
  packages=['master', 'base', 'dbid', 'utils', 'meta', 'musicdb', 'match' ,'gate']
    + ['lib.genius', 'lib.spotify', 'lib.discogs', 'lib.rateyourmusic', 'lib.allmusic', 'lib.lastfm', 'lib.deezer', 'lib.albumoftheyear', 'lib.metalarchives']
)

## This takes forever so I'm not using it...
#find_namespace_packages(include=['lib.*']) 
