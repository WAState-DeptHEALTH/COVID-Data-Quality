## Curated Dataset Creation for Internal and External Partners ##
### How to Create Standardized, Concise Datasets with Only Relevant Data for Partner Organizations ###
These stand-alone R scripts create smaller, curated .csv files with only the most relevant information for internal partners (such as the Washington State Labor and
Industry Division) or the requested information from external partners (such as the CDC) by using filters for certain data fields, dates, types of cases, and more.


#### Dataset_Creation_for_CDC_DCIPHER_Reporting.R
This script queries WDRS to get COVID-19 case count data for the CDC's state-specific reporting portal DCIPHER (Data Collection and Integration for Public Health Event Responses).
In this script we retrieve the appropriate fields CDC has specified, clean the data according to how the CDC wants it filtered, and format the final data in a format that the CDC specifies. 
This script is helpful to external DOH partners and other health departments as it demonstrates how to create a standardized dataset according to the CDC's specifications and naming 
conventions for DCIPHER reporting. This script creates a .csv file with all of the information, formatting, and any other specifications the CDC has told us they want that is then uploaded 
to the DCIPHER portal within the SAMS system (Secure Access Management System) the CDC manages. This code also includes a Quality Assurance (QA) section where the script checks 
to make sure no issues have occurred with the file before we upload it to the CDC.
This script also sends some automatic emails to let ourselves and others know that the files have been created and sometimes even case counts for that day.


#### Occupational_Dataset_Creation_for_Labor_and_Industries 
This script queries WDRS to get occupational data relevant to the Labor and Industry Division of DOH (LNI) and is sent to them via the 
WA DOH Managed File Transfer (MFT) site. The LNI team works with the WDRS Developers team for any additional work on this file. This script is helpful to external 
DOH partners as it demonstrates how to create a standardized dataset for specific uses (in this case, occupational) by pulling variables relevant to other teams or 
departments. 
In this script decisions to keep or filter data is made based on the reliability of the data and also the request of the LNI team. 
This script cleans and organizes data according to what the LNI team asked for and what the data quality team can provide for further analysis. 
Then, the script sends an automatic email to our partners to let them know we have manually uploaded the dataset we created through this script.
