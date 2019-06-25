from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pymongo-write-cache',
    version='0.0.1',
    description='Simple to use write cache for MongoDB.',
    author="Iwan Bolzern",
    url="https://github.com/iwanbolzern/mongodb-write-cache",
    packages=find_packages(),
    include_package_data=True,
    long_description=readme,
    license=license,
    keywords=[
        "pymongo-write-cache", "write-cache", "mongo", "mongodb", "pymongo"
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
    ],
)

