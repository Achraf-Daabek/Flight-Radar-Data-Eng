from setuptools import setup, find_packages
import pathlib

from sefa.version import __version__

package_path = pathlib.Path(__file__).parent.resolve()

# setup requirements (distutils extensions, etc.)
setup_requirements = [
    'pytest-runner'
]

# package test requirements
test_requirements = [
    'pytest'
]

setup(
    name="airlines_analyzer",
    packages=find_packages(exclude=["tests", "tests.*"]),
    version=__version__,
    author="exalt squad",
    description="Databricks package for airlines analyzer MVP",
    install_requires=(package_path / "requirements.txt").read_text(encoding="utf-8").split("\n"),
    license="EXALT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
    python_requires=">=3.7",
    test_suite="tests",
    tests_require=test_requirements,
    setup_requires=setup_requirements
)