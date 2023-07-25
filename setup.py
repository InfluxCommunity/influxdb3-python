from setuptools import setup
import os
import re


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
    packages=['influxdb_client_3'],
    extras_require={'pandas': ['pandas']},
    install_requires=['pyarrow','influxdb-client'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ]
)