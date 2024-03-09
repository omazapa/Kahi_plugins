<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi staff affiliations plugin 
Kahi will use this plugin to insert or update the affiliations information from institution's staff file

# Description
Plugin that reads the information from institution's staff file to update or insert the information of the faculties and departments in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_staff_affiliations
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one file from staff's office for example: University of Antioquia.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
UdeA's staff file
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  staff_affiliations:
    institution_name: Universidad de Antioquia
    file_path: /current/data/colombia/udea/Base de Datos profesores.xlsx
```
Unaula staff file

```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  staff_affiliations:
    institution_name: Universidad Autónoma Latinoamericana
    file_path: /current/data/colombia/unaula/Base de Datos profesores.xlsx
```

Univalle staff file
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  staff_affiliations:
    institution_name: University of Valle
    file_path: /current/data/colombia/univalle/Base de Datos profesores.xlsx
```

UEC staff file
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  staff_affiliations:
    institution_name: Universidad Externado de Colombia
    file_path: /current/data/colombia/uec/Base de Datos profesores.xlsx
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



