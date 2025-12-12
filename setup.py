
from setuptools import setup, find_packages

setup(
    name="multitouch_attribution",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "pyspark",  # Provided by Databricks runtime usually, but good for local
    ],
)
