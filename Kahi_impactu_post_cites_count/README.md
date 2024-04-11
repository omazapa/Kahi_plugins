<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi impactu post citations count
This plugin helps to calculate the citations count for the impactu output.
The citations count is calculated for each institution, faculty, deparment, research group and author.


# Installation

## Dependencies
This package requires MongoDB to be installed and running and kahi already executed.

## Package
Install the package with the following command:
`pip install kahi_impactu_post_cites_count`


# Usage
example for workflow:

```
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi
  log_collection: log
workflow:
  impactu_post_cites_count:
    database_url: localhost:27017
    database_name: kahi_calculations
    verbose: 5
```
# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



