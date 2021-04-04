import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ligature",
    version="1.2.0",
    author="Andrew Geiger",
    author_email="andrew.geiger@corsosystems.com",
    description="A toy Python library for merging data, from low to high level abstractions.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CorsoSource/ligature",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.5',
)
