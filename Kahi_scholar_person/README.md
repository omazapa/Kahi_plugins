<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi google scholar works plugin 
Kahi will use this plugin to insert or update the authors information from Google Scholar database.

# Description
Plugin that read the information from a mongodb database with google scholar information to update or insert the information of the research products authors in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scholar_person
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
For the data dependencies the user must have the output of [Moai's](https://github.com/colav/Moai) scrapping of google scholar.
C++ library libhunspell-dev must be installed on your system. On ubuntu you can do it by typing
```shell
$ sudo apt install libhunspell-dev
```

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
workflow:
  scholar_person:
    num_jobs: 5
    verbose: 5
    database_url: localhost:27017
    database_name: scholar_colombia
    collection_name: stage
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

