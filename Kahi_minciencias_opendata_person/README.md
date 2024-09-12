<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi minciencias openadata person plugin 
Kahi will use this plugin to insert or update the people's information from minciencias opendata file and a cvlac scraping information

# Description
Plugin that reads the information from minciencias opendata and cvlac files to update or insert the information of the people in CoLav's database format.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_minciencias_opendata_person
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have at least one file from minciencias opendata found [here](https://www.datos.gov.co/Ciencia-Tecnolog-a-e-Innovaci-n/Investigadores-Reconocidos-por-convocatoria/bqtm-4y2h "minciencias researchers data"). Also the user needs the file from the production of the research groups found [here](https://www.datos.gov.co/Ciencia-Tecnolog-a-e-Innovaci-n/Producci-n-Grupos-Investigaci-n/33dq-ab5a). Additionally user must have a cvlac [file](https://drive.google.com/file/d/1DwNqYzUg57YVjBSno-A6ZlEt51mPTzER/view?usp=drive_link) with a scraping of all available researchers.

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
   minciencias_opendata_person:
    database_url: localhost:27017
    database_name: yuku
    researchers: cvlac_data
    cvlac: cvlac_stage
    groups_production: gruplac_production_data
    private_profiles: cvlac_stage_private
    num_jobs: 12
    verbose: 5
```

# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



