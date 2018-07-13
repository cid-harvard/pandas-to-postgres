import os
from setuptools import setup, find_packages


def read(fname, lines=False):
    f = open(os.path.join(os.path.dirname(__file__), fname))
    if lines:
        return [x.strip() for x in f.readlines()]
    else:
        return f.read()


setup(
    name="pandas_to_postgres",
    version="v0.0.1a",
    author="Brendan Leonard <Harvard CID>",
    description=(
        "Utility to copy Pandas DataFrames and DataFrames stored in HDF5 files "
        "to PostgreSQL "
    ),
    url="http://github.com/cid-harvard/pandas-to-postgres",
    packages=find_packages(),
    install_requires=["SQLAlchemy", "pandas", "psycopg2"],
    long_description=read("README.md"),
    classifiers=[
        "Topic :: Database",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: BSD License",
        "Development Status :: 3 - Alpha",
    ],
)
