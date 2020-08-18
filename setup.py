from setuptools import find_packages, setup

import bigquery_views_manager


with open('requirements.txt', 'r') as f:
    REQUIRED_PACKAGES = f.readlines()


with open('README.md', 'r') as f:
    long_description = f.read()


packages = find_packages()


setup(
    name='bigquery-views-manager',
    version=bigquery_views_manager.__version__,
    author="eLife Sciences Publications, Ltd",
    url="https://github.com/elifesciences/bigquery-views-manager",
    install_requires=REQUIRED_PACKAGES,
    packages=packages,
    include_package_data=True,
    description='BigQuery Views Manager',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
