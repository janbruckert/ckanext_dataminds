import os
from setuptools import setup, find_packages

# Get the long description from the README file if available
here = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''

setup(
    name='dataminds',
    version='0.1.1',
    description='CKAN extension for automated data fetching and publishing using CKAN.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Jan Bruckert',
    author_email='janbru@uni-bremen.de',
    license='AGPL-3.0',
    url='http://localhost:5000/ckanext_dataminds',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'ckanapi==4.6',
        'pymongo==3.12.1',
        'requests==2.30.0',
        'Flask==2.2',
    ],
    entry_points={
        'ckan.plugins': [
            'dataminds=ckanext_dataminds.plugin:DatamindsPlugin'
        ]
    }
)

