from setuptools import setup
from setuptools import find_packages

setup(
    name = 'acsql',
    description = 'The Advanced Camera for Surveys Quicklook Project',
    url = 'https://github.com/bourque/acsql.git',
    author = 'Matthew Bourque',
    author_email = 'bourque@stsci.edu',
    keywords = ['astronomy'],
    classifiers = ['Programming Language :: Python'],
    packages = find_packages(),
    install_requires = [],
    version = 0.0,
    include_package_data=True
    )