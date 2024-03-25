<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi template plugin 
This is a template for xyz project
replace template for the name of the plugin everywhere.

# Description
Write something meaningful here ;)

# Installation

## Dependencies
What do I need fot this plugin?, it could be external services etc..

## Package
Write here how to install this plugin
usauly is 

`pip install kahi_impactu_postcalculations
`


# Usage
what should I know?
put it here.

Additional parameters for kahi_run in the workflow should be here as well.
example :

```
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  impactu_postcalculations:
    database_url: localhost:27017
    database_name: kahi_calculations
    n_jobs: 6
    verbose: 5
    author_count: 6 #use this with warning, maybe the network is too big and it can not be saved in MongoDB
```
Those parameters are not really needed in the workflow file, it is just for illustration.


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



