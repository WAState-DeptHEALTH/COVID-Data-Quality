#################################################################################################
#  START OF COVID CASES DATASET CREATION
#  COVID-19 R OBJECT DATASET CREATION AND CLEANING PART 0.5
################################################################################################
################################################################################################
#
#  This script is automatically run using Windows Task Scheduler. The Task Scheduler triggers a batch file which runs this script 
#  on a set schedule each week and generates a log file to monitor script performance. The script reads in all other 
#  .Rmd and .R scripts needed to create the standardized COVID-19 object in sequential order. 
#  The script was previously set up to run daily and conditionally produced different reports based on the day of the week.
#  This script starts the automatic, task scheduler run of subsequent R Object scripts in order to create clean, static COVID-19 datasets
#  in the form of .Rdata files, referred to as R Objects, have been created and are ready for use.
#  The R Objects are created as an alternative for querying the live (and therefore, more complex and messy) Washington COVID-19 database. 
#  
#
#  **IMPORTANT ACRONYMS/PHRASES USED:**
#     -WDRS: Washington Disease Reporting System (the primary database for COVID-19 disease reporting information)
#     -db: database
#     -df: dataframe; a way to structure data within the base functions of R
#     -R Object: a set of .RData files with cleaned COVID-19 data to use as a more reliable/static dataset to pull data from compared to the live databse WDRS 
#     -spectable: a table looking at specimen collection specifically
#
#
#  This script is helpful to external DOH partners as it demonstrates how to create a standardized dataset for all cases to use as an alternative to querying a live database.
#  
################################################################################################
################################################################################################


rm(list=ls()) #clear environment
Sys.setenv(RSTUDIO_PANDOC="filepath/") #set Pandoc environment 104
#Sys.setenv(RSTUDIO_PANDOC="filepath/") #set Pandoc environment 019

# USER INPUT ----------------------------------------------------------
# set run parameters
# use the objects below to indicate if you are running for production (run_production=TRUE, run_test=FALSE)
# or a test run: (run_production=FALSE, run_test=TRUE)
# These parameters toggle 3 things
# 1: They will determine if the geocoded_covid_cases script is run
# 2: They will toggle on the EVAL function in the script chunks in mise_en_place to set the working directories
# 3: They will toggle the EVAL function in the script chunks in QA_check and email_creation to create the correct outputs
run_production <- TRUE
run_test <- FALSE
#starting April 19th, 2024 - DIQA switched to a weekly cadence on Tuesdays, so the day_check variable is no longer relevant; will also run on Saturdays for geocoded
day_check <- substr(weekdays(Sys.Date()), 1, 3)

# FOR TEST RUNS ONLY
# The script is set to run rawNegatives and AllSpecimens ("long" version) and the corresponding outputs if day_check = "Mon" or "Wed"
# Otherwise it will not run these and instead run the "short" version of the script and corresponding outputs; if day_check = "Tue", "Thu", or "Fri"
# If you'd like to control whether the "short" or "long" version of the script is run, update day_check manually below

# if (run_test == TRUE) {
# day_check <- "Tue"
# }

# END OF USER INPUT ----------------------------------------------------------


# Run mise_en_place
# This section sets up the working drives for calling in and pushing out R objects. All saving, loading, etc. should reference the working drives below. 
# If a new folder is created for an output, please add it here and reference in your code vs. directly putting file pathways in sections of code. 

rmarkdown::render("filepath/mise_en_place.Rmd")

#Delete data from current folders (both user and DIQA-only)

#user files
WDRSdir <- Output_tables_wdrs
delfiles <- dir(path=WDRSdir)
file.remove(file.path(WDRSdir, delfiles))

#DIQA-only files
DIQAdir <- paste0(diqa_path, "CurrentData/")
delfiles <- dir(path=DIQAdir)
file.remove(file.path(DIQAdir, delfiles))


# Pull data from wdrs and save raw files

rmarkdown::render(input=paste0(GITDIR,"source scripts/wdrs_pull_and_raw_object_save.Rmd"))


# modify SpecTable

source(paste0(GITDIR,"source scripts/SpecTable_modifications.R"))


# create covid_cases

rmarkdown::render(input=paste0(GITDIR,"source scripts/covid_cases_creation.Rmd"))


#run geocoded_covid_cases script
if (run_production == TRUE){
  
  source(paste0(geocc_path,"geocoded_covid_cases.R"))
  
  rm(cases_censust_stateplane_r, cases_geocodedprimaryadd, cases_geocodedprimaryaddcloseaccuracy, cases_geocodedprimaryaddNOTcloseaccuracy, cases_geocodedreportadd,
     cases_geocodedreportaddcloseaccuracy, cases_geocodedreportaddNOTcloseaccuracy, cases_matched, cases_matchedcensust, cases_matchedclose, casestoattemptgeocoding,
     geocoded_covid_cases)
  
}


# Time check end of code (need for QA checks)

End_update_WDRS <-Sys.time()
End_update_WDRS<-data.frame(End_update_WDRS)
script_runtime<-End_update_WDRS-Start_update_WDRS


# Save user snapshot

datetimestamp<- gsub(":", ".", Sys.time(), fixed = TRUE) # Create a timestamp
out_directory_folder<-paste0(BASEDIR,"Data/DataSnapshots/WDRS/",datetimestamp) # Set/create the location of where you want the file to output(folder)
dir.create(out_directory_folder) # Generate the output folder
from <-Output_tables_wdrs # Location to copy from
to <- out_directory_folder # Location to copy to
list_of_files <- list.files(from, all.files = TRUE, full.name=TRUE, recursive = TRUE) 
file.copy(file.path(list_of_files), to)


# QA report

rmarkdown::render(input=paste0(GITDIR,"source scripts/QA_checks.Rmd"))


# e-mail

rmarkdown::render(input=paste0(GITDIR,"source scripts/email_creation.Rmd"))


#Save DIQA only snapshot

out_directory_folder_diqa<-paste0(diqa_path,"DataSnapshots/",datetimestamp) # Set/create the location of where you want the file to output(folder)
dir.create(out_directory_folder_diqa) # Generate the output folder
from <- paste0(diqa_path, "CurrentData/") # Location to copy from
to <- out_directory_folder_diqa # Location to copy to
list_of_files <- list.files(from, all.files = TRUE, full.name=TRUE, recursive = TRUE) 
file.copy(file.path(list_of_files), to)
