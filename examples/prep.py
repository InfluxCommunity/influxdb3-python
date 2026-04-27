import os
import subprocess
import sys

"""
Installs extra packages needed by some examples.

Sets functional examples with shebang headers as executable.
"""

extra_packages = [
    'numpy',
    'pandas',
    'bson',
    'matplotlib',
]

functional_examples = [
    './core/basic_write.py',
    './core/basic_query.py',
    './core/basic_ssl.py',
    './core/timeouts.py',
    './write/batching.py',
    './write/fileimport.py',
    './write/source_data/updater.py',
    './write/handle_http_error.py',
    './write/pandas_write.py',
    './write/writeoptions.py',
    './query/handle_query_error.py',
    './query/query_async.py',
    './query/query_modes.py',
    './advanced/downsample.py'
]

def set_functional_examples_executable():
    global functional_examples
    dir_path = os.path.dirname(os.path.realpath(__file__))
    for example in functional_examples:
        if os.path.exists(f"{dir_path}/{example}"):
            print(f"Functional example found at {example}")
            os.chmod(f"{dir_path}/{example}", 0o775)


def install_extra_packages():
    global extra_packages
    for package in extra_packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", f"{package}"])

print(f"Installing extra packages {extra_packages}")
install_extra_packages()

print("Setting functional examples executable")
set_functional_examples_executable()
