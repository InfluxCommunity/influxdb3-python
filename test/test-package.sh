# bin/bash

# This script is used to create the CLI and Client libary as a test package
python ./setup-client.py sdist bdist_wheel
python ./setup-cli.py sdist bdist_wheel

pip install ./dist/pyinflux3-0.0.0-py3-none-any.whl --force-reinstall
pip install ./dist/pyinflux3_cli-0.0.0-py3-none-any.whl --force-reinstall
