import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ooiod",
    version="0.0.1",
    author="Tim Crone",
    author_email="tjcrone@gmail.com",
    description="Helper library for moving OOI data into Azure Open Datasets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ooicloud/ooi-opendata",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
