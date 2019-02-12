from setuptools import setup
from setuptools import find_packages

setup(
    name = 'acsql',
    description = 'The Advanced Camera for Surveys Quicklook Project',
    url = 'https://github.com/spacetelescope/acsql.git',
    author = 'Matthew Bourque, Sara Ogaz, Meredith Durbin, Alex Viana',
    author_email = 'bourque@stsci.edu, ogaz@stsci.edu, mdurbin@uw.edu, alexcostaviana@gmail.com',
    keywords = ['astronomy'],
    classifiers = ['Programming Language :: Python'],
    packages = find_packages(),
    install_requires = ["pyaml", "pillow", "ccdproc"],
    version = 0.0,
    include_package_data=True
    )
