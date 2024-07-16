##  Missing and Incomplete Street Address and Phone Number Cleanup Process ##
### How to Identify Missing and Messy Contact Information and Enrich It
This process utilizes two internal databases (one for hospitalizations, one for immunizations) in order to enrich
contact information (phone numbers and address) of COVID-19 cases for improved case investigation, contact tracing, and case mapping. 
If a phone number or address is missing from case/contact information, it makes it more difficult for downstream case investigators to
successfully reach cases and contacts. This data quality-focused process finds missing phone/address information by cross-referencing other internal databases
that may have this information.

For example, a COVID-19 case (ID #123456) is missing a phone number. This script will find that case and put it into a line-level roster of other cases with 
missing information. The roster will be cross-matched in a separate process with an immunization database where the case #123456 does have a phone number 
listed after a flu vaccination the previous year. Now that we have found a phone number associated with case #123456, we will create a roster 
of the newly-discovered information and it will be put into the state-wide COVID-19 database (WDRS), without deleting any previous information. 

This series of R scripts creates a query to pull in cases with missing or incomplete phone and address data fields, cross matches those cases against cases with 
similar information in the immunization database (IIS), cross matches cases against the hospitalization database (RHINO), and then creates a .csv file of the 
newly found or updated phone numbers and addresses to be reviewed and uploaded into the COVID-19 database. 

#### Before Running This Code:
These R scripts use an extrnal .rds file (referred to in this series of scripts as an "R creds" file) to reference sensitive and/or confidential information such as database names, emails, usernames, passwords, etc. in order to avoid putting this sensitive information directly into the code and allow team members to collaborate on one R file without constantly changing user information. 
This file is created in a separate R script which each user runs separately on their computer, gets saved as a .rds file, and kept in a standard file location on each user's computer, so that the script and the file path is the same regardless of who is running the R code. 
This .rds file acts as an additional security measure as it prevents user's from hard-coding personal information into the shared code, and makes collaboration on the same code across team members easier.


#### 1) Missing_Information_Data_Pull.R
This R script addresses the first part of this process: pulling data that includes cases that have missing/incomplete
contact information and creating a roster of those cases for the subsequent matching processes. 

#### 2) Immunization_Database_Matching.R
This R script addresses the second part of this process: linking positive COVID-19 cases from WDRS (the WA State database) 
with missing address/phone information to the Immunization Information System (IIS) in order to obtain address and phone numbers 
for matching records. Matching is based primarily on First Name, Last Name, and Date of Birth (DOB).

#### 3) Hospitalization_Database_Matching.R
This R script addresses the third part of this process: linking positive COVID-19 cases from WDRS (the WA State database) 
with missing address/phone information to the RHINO (Rapid Health Information NetwOrk) hospitalization database in order to obtain address and phone numbers 
for matching records. Matching is based primarily on First Name, Last Name, and Date of Birth (DOB).
