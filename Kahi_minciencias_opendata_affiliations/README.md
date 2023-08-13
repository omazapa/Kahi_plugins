<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias openadata affiliations plugin 
Kahi will use this plugin to insert or update the affiliations information from minciencias opendata file

# Description
Plugin that reads the information from minciencias opendata file to update or insert the information of the research groups in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_affiliations
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one file from minciencias opendata found [here](https://www.datos.gov.co/Ciencia-Tecnolog-a-e-Innovaci-n/Grupos-de-Investigaci-n-Reconocidos/hrhc-c4wu "minciencias groups data").

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  minciencias_opendata_affiliations:
    file_path: 
      - /current/data/colombia/scienti-abiertos/3-Grupos_de_Investigaci_n_Reconocidos.csv
```
Where file_path under minciencias_opendata_affiliations task is the full path where the minciencias opendata csv is located.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



