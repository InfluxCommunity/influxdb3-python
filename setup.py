from setuptools import setup
import os
import subprocess

binary_name = "influx3"
binary_destination = os.path.join("bin", binary_name)


def get_git_tag():
    try:
        return subprocess.check_output(["git", "describe", "--tags"]).strip().decode()
    except Exception as e:
        print(f"Error getting git tag: {e}")
        return "0.0.0"

setup(
    name='pyinflux3',
    version=get_git_tag(),
    description='Community Python client for InfluxDB IOx',
    author='InfluxData',
    author_email='contact@influxdata.com',
    url='https://github.com/InfluxCommunity/pyinflux3',
    packages=['influxdb_client_3'],
    install_requires=['pyarrow', 'flightsql-dbapi', 'influxdb-client', 'pygments', 'prompt_toolkit' ],
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
    data_files=[('influx3', ['influx3'])],
    entry_points={"console_scripts": [f"{binary_name} = {binary_name}:main"]},

)