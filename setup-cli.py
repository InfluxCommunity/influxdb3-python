from setuptools import setup, find_packages
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
    name='pyinflux3-cli',
    version=get_version(),
    description='Community Python client for InfluxDB IOx (CLI)',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='InfluxData',
    author_email='contact@influxdata.com',
    url='https://github.com/InfluxCommunity/pyinflux3',
    packages=['influxdb_cli'],
    install_requires=['pyarrow', 'flightsql-dbapi', 'influxdb-client', 'pathlib', 'pygments', 'prompt_toolkit', 'pandas', "tabulate"],
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
    data_files=[('influx3', ['influxdb_cli/influx3.py'])],
    entry_points={'console_scripts': [f"influx3 = influxdb_cli.influx3:main"]},
)
