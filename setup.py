from setuptools import setup

setup(
    name='pyinflux3',
    version='0.1',
    description='Community Python client for InfluxDB IOx',
    author='InfluxData',
    author_email='contact@influxdata.com',
    url='https://github.com/InfluxCommunity/pyinflux3',
    packages=['influxdb_client_3'],
    install_requires=['pyarrow', 'flightsql-dbapi', 'influxdb-client'],
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
    data_files=[('influx3', ['influx3'])]
)