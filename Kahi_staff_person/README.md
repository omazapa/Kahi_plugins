<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi staff person plugin 
Kahi will use this plugin to insert or update the people information from institution's staff file

# Description
Plugin that reads the information from institutions's staff file to update or insert the information of the people around university production in CoLav's database format.

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
The user must have at least one file from staff's office from the institution.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  staff_person:
    databases:
      - institution_name: Universidad de Antioquia
        file_path: staff/Base de Datos profesores 2024_con_clasificación de Colciencias.xlsx
      - institution_name: Universidad del Valle
        file_path: staff/Maestro_empleado DOCENTES SEP 26_NORMALIZADO_univalle_2023.xlsx
      - institution_name: Universidad Autónoma Latinoamericana
        file_path: staff/UNAULA_Profesores regulares y ocasionales vige_NORMALIZADO.xlsx
      - institution_name: Universidad Externado de Colombia
        file_path: staff/Formato reporte de información docentes_2023_NORMALIZADO_uec.xlsx
    verbose: 5
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



