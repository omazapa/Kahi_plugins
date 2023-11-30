<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi wikipedia affiliations plugin
Kahi will use this plugin to update the affiliations information from wikipedia

# Description
Plugin that reads the information from the mongodb collection with the affiliations in colav format to update the information of the names in various languages and the logos.

# Installation
You could download the repository from github. Go into the folder where the setup.py is located and run
```shell
pip3 install .
```
From the package you can install by running
```shell
pip3 install kahi_wikipedia affiliations
```

## Dependencies
Software dependencies will automatically be installed when installing the plugin.
The user must have already an instance of mongodb with the processed affiliations at least from ROR in colav's data format (users could use the Kahi_ror_affiliations plugin).

# Usage
To use this plugin you must have kahi installed in your system and construct a yaml file such as
```yaml
config:
  database_url: localhost:27017
  database_name: kahi
  log_database: kahi_log
  log_collection: log
workflow:
  wikipedia_affiliations:
    tasks:
      - names
      - logos
    num_jobs: 10
    verbose: 5
```
The user must have to take into account the limits of requests in wikipedia to estimate the value of the varibale num_jobs which determines the number of concurrent tasks. [Wikimedia limits](https://api.wikimedia.org/wiki/Rate_limits#:~:text=User%2Dauthenticated%20requests,requests%20per%20hour%20per%20user. "Wikimedia limits data") 


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



