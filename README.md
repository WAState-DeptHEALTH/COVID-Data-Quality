## Welcome to the COVID-19 Data Integration and Quality Assurance (DIQA) GitHub! ##
Our goal as a team at the WA DOH is to write code that processes, cleans, enriches, integrates, automates, and improves surveillance data for Washington State and ensures other epidemiologists at the DOH can easily utilize clean, reliable data. 

When the data comes to us, it is often messy; incomplete fields, information in the wrong fields, duplicate cases, no standardization of variable names, and more. With each script that we write, we aim to clean, enrich, and standardize data the best we can to create datasets that are not only easy to use for analysis/reporting by downstream teams, but are as reliable and accurate as possible.

In this GitHub Repo, you will find a variety of scripts that aim to standardize or improve the quality of the data in some way, shape, or form. See the descriptions below to get a better idea of how we clean up messy data, and then view the documented code to get a better idea of how we created these quality improvement processes through coding. 

### How to Create a Standardized Dataset ###
These R and R Markdown scripts create .RData and .csv files with cleaned and standardized COVID-19 data. This process creates more reliable, static datasets (referred to in this repo as the "R Objects") so that COVID-19 data can be seamlessly pulled into other processes. It helps teams avoid needing to access and use our constantly-changing state-wide database (WDRS) for routine reporting or tasks. 

### How to Use the Standardized Dataset ###
The following R scripts utilize the standarized COVID-19 datasets ("R Objects") to clean, enrich, and share data with internal and external partners. 

#### Street Address and Phone Number Cleanup ####
Contact information (namely street address and phone number) makes for often missing or messy fields-- this series of R scripts utilize the COVID-19 R Object standardized datasets to pull in missing or messy contact data to be cross matched with other internal DOH databases to fill in as much address and phone data as possible. 

#### Curated Dataset Creation for Partners ####
Internal and external partners (fellow WA DOH teams, CDC, etc) require standardized dataset creation on a smaller, more concise scale-- these stand-alone R scripts utilize the COVID-19 R Object standardized
datasets to pull in the data fields requested by our partners and creates a curated .csv file with the most relevant data to send to our partners for the purposes of further analysis or reporting.
