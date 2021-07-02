from setuptools import setup, find_packages
import os

with open('requirements.txt') as f:
   required = f.read().splitlines()


setup(
   name='twapExecution',
   version='1.6.5',
   description='A twap module',
   author='Angus & Alex',
   author_email='xxxxx',
   install_requires=required,
   include_package_data=True,
   data_files=[('twapExecution', ['twapExecution/exchanges/config.json'])],
   packages=find_packages(),  #same as name
)