<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi template post cleanup entities
This plugin allows to clean up the entities that are not needed in the post processing.

# Description
Some author and affiliations are not needed in the post processing, this plugin allows to remove them.
The authors removed are the ones that are not in the list of authors of the works, and the affiliations removed are the ones that are not in the list of affiliations of the authors.

# Installation

## Package
Write here how to install this plugin
usauly is 

`pip install kahi_post_cleanup_entities`


# Usage
this plugin takes the kahi database, person and affiliations from initial config section.
```
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  ##Works plugins here
  post_cleanup_entities: # run this after all works plugins are done
    num_jobs: 20
    verbose: 4
```



# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



