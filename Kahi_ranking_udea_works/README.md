<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi ranking udea works plugin 
Kahi will use this plugin to insert or update the works information from ranking office file from University of Antioquia.

# Description
Plugin that read the information from a file with papers published and reported to the ranking office at University of Antioquia to update or insert the information of the research products in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_scholar_works
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
For the data dependencies the user must have a copy of the file from UdeA's ranking office..
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
  log_database: kahi
  log_collection: log
workflow:
  ranking_udea_works:
    file_path: /current/data/colombia/udea/produccion 2018-2022 al 27 oct 2022.xlsx
    verbose: 5
    num_jobs: 5
```

* WARNING *. This process could take several hours

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/

