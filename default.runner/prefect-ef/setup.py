from setuptools import find_packages, setup

with open("requirements.txt") as install_requires_file:
    install_requires = install_requires_file.read().strip().split("\n")

setup(
    name="prefect-ef",
    description="Commonly used tasks, flows, and utility functions for Prefect @ EF",
    author="EF Tours Data Engineering",
    author_email="toursdataengineering@ef.com",
    keywords="prefect",
    version="0.1",
    packages=find_packages(exclude=("tests", "docs")),
    python_requires=">=3.7",
    install_requires=install_requires,
)
