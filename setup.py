from setuptools import setup, find_packages

setup(
    name='axaz_dlt_sources',
    version='0.1.0',
    author='Christoffer Kleven Berg',
    author_email='christoffer.kleven.berg@axaz.com',
    packages=find_packages(),
    description='A collection of Data Load Tool sources as submodules.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        'dlt'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
