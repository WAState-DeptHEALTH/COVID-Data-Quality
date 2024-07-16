## Standardized Dataset Creation ##
### How to Create a Standardized Dataset Using R Objects ###
These scripts create .RData and .csv files with cleaned and standardized COVID-19 data. This process creates more reliable, static datasets so that COVID-19 data can be seamlessly pulled into other processes. It helps teams avoid needing to access and use our constantly-changing state-wide database (WDRS) for routine reporting or tasks.

#### 0.5) Automatic_Trigger_for_R_Object_Process.R ####
This script is automatically run using Windows Task Scheduler. The Task Scheduler triggers a batch file which runs this script on a set schedule each week and generates a log file to monitor script performance. The script reads in all other .Rmd and .R scripts needed to create the standardized COVID-19 object in sequential order. The script was previously set up to run daily and conditionally produced different reports based on the day of the week.

#### 1) Everything_in_its_Place.Rmd

#### 2) Data_Pull.Rmd

#### 3) Modify_Specimen_Data.R

#### 4) Dataset_Creation.Rmd

#### 5) Geocoded_Dataset_Creation.R

#### 6) Quality_Check.Rmd

#### 7) Email_Notification.Rmd
