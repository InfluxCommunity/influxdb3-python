from setuptools import setup
import os
import re

binary_name = "influx3"
binary_destination = os.path.join("bin", binary_name)

with open("README.md", "r", encoding="utf-8") as fh:
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
    return "v0.6.3"

setup(
    name='pyinflux3',
    version=get_version(),
    description='Community Python client for InfluxDB IOx',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='InfluxData',
    author_email='contact@influxdata.com',
    url='https://github.com/InfluxCommunity/pyinflux3',
    packages=['influxdb_client_3'],
    install_requires=['pyarrow', 'flightsql-dbapi', 'influxdb-client', 'pygments', 'prompt_toolkit', 'pandas', "tabulate"],
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
    ],
    data_files=[('influx3', ['influxdb_client_3/influx3.py'])],
    entry_points={"console_scripts": [f"{binary_name} = influxdb_client_3.influx3:main"]},

)