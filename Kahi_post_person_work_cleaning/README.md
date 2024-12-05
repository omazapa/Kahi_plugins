<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# Kahi post processing work cleaning for person
This plugin have to be added in the post execution of the workflow after works are integrated.

# Description
This go through person and compares the person affiliation with the works affilation for that person,
and if the works doesn´t have any affiliation reported in person, then it is remove for that person. (Removing the id of the person in the work)

# Installation

## Dependencies

## Package
please run

`pip install Kahi_post_person_work_cleaning`


# Usage
Please added the entry in the workflow.
example :

```
  post_person_work_cleaning:
    verbose: 4
```


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



