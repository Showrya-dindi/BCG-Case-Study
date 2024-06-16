from setuptools import setup 
  
setup( 
    name='src', 
    version='1.0.0', 
    description='A Python package to include analysis, data_loder and utils', 
    author='Venkata Showrya Dindi', 
    author_email='showrya.dindi@gmail.com', 
    packages=['src'], 
    install_requires=[ 
        'pyspark==3.3.0', 
        'pyyaml==6.0.1', 
        'datetime==5.5'
    ], 
) 