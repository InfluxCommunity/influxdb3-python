from setuptools import setup, find_packages
import os
import re


requires = [
    'reactivex >= 4.0.4',
    'certifi >= 14.05.14',
    'python_dateutil >= 2.5.3',
    'setuptools >= 21.0.0',
    'urllib3 >= 1.26.0',
    'pyarrow >= 8.0.0'
]

with open("./README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

def get_version_from_github_ref():
    github_ref = os.environ.get("GITHUB_REF")
    if not github_ref:
        return None

    match = re.match(r"refs/tags/v(\d+\.\d+\.\d+)", github_ref)
    if not match:
        return None

    return match.group(1)

def get_version():
    # If running in GitHub Actions, get version from GITHUB_REF
    version = get_version_from_github_ref()
    if version:
        return version

    # Fallback to a default version if not in GitHub Actions
    return "v0.0.0"

setup(
    name='influxdb3-python',
    version=get_version(),
    description='Community Python client for InfluxDB 3.0',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='InfluxData',
    author_email='contact@influxdata.com',
    url='https://github.com/InfluxCommunity/influxdb3-python',
    packages=find_packages(exclude=['tests', 'tests.*', 'examples', 'examples.*']),
    extras_require={'pandas': ['pandas'], 'polars': ['polars'], 'dataframe': ['pandas', 'polars']},
    install_requires=requires,
    python_requires='>=3.8',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ]
)
