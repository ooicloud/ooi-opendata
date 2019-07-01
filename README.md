# ooi-opendata
Scripts and notebooks for moving OOI data into Azure Open Datasets.

## Setup

### Conda

It is probably worth creating and activating the conda environment.yml file contained in the root directory of this repo.

### git-crypt

In order to obtain write access to containers in the ooiopendata storage account, it is necessary to install [`git-crypt`](https://www.agwa.name/projects/git-crypt/). Please read this [HOW GIT-CRYPT WORKS](https://www.agwa.name/projects/git-crypt/) if new to it.

### Repo configuration

* Request a git-crypt symmetric key from the maintainers of this repo for the container(s) you are using.
* Initialize git-crypt using the unlock command:
  * `git-crypt unlock /path/to/your.key`
* Install the local library using:
  * `pip install -e .`
