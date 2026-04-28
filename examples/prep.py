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
    'pytz'
]


def set_functional_examples_executable():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    for root, dirs, files in os.walk(dir_path):
        root.split(os.sep)
        for file in files:
            with (open(os.path.join(root, file), "r")) as input_file:
                try:
                    head = [next(input_file) for _ in range(1)]
                    if head[0].startswith("#!/"):
                        os.chmod(input_file.name, 0o775)
                except UnicodeDecodeError:
                    continue


def install_extra_packages():
    for package in extra_packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", f"{package}"])


print(f"Installing extra packages {extra_packages}")
install_extra_packages()

print("Setting functional examples executable")
set_functional_examples_executable()
