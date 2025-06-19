# /setup.py
from setuptools import setup, find_packages

# This file tells pip how to install our custom code library.
setup(
    name="snapshot_pro_etl",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "python-dateutil",
        "boto3==1.35.95",
        "s3fs",
        "pandas",
        "apache-airflow-providers-cncf-kubernetes",
        "apache-airflow-providers-oracle",
        "apache-airflow-providers-http",
        "apache-airflow-providers-amazon"
    ],
)