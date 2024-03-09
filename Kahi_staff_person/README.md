<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi staff udea person plugin 
Kahi will use this plugin to insert or update the people information from UdeA's staff file

# Description
Plugin that reads the information from udea's staff file to update or insert the information of the people around university production in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_staff_udea_person
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one file from staff's office in the University of Antioquia.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  staff_udea_person:
    file_path: /current/data/colombia/udea/Base de Datos profesores 2019.xlsx
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



