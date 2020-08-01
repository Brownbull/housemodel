# Hosing Market Model
- [Hosing Market Model](#hosing-market-model)
  - [Setup](#setup)
    - [Clone](#clone)
    - [env](#env)
    - [activate](#activate)
    - [install](#install)
    - [Update requierements](#update-requierements)
  - [Tag](#tag)
***
## Setup
### Clone
### env
```shell
python3 -m venv env
```
### activate
```shell
# Open folder containing project
cd path/where/this/file/is/readme.md
env\Scripts\activate.bat
```
### install
```shell
pip install -r requirements.txt 
```

### Update requierements
```shell
pip freeze > requirements.txt
```

## Tag
Register tag
```sh
git tag <tagname> -a
git push origin --tags
```