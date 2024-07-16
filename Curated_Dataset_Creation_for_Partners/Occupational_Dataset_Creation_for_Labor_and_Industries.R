#################################################################################################
#LABOR AND INDUSTRIES OCCUPATION COVID-19 DATASET CREATION
#Dataset Creation for Specific Partners That Want Only The Relevant Data
################################################################################################
################################################################################################
#
#  This script queries WDRS to get occupational data relevant to Labor and Industry Division of DOH (LNI) and is sent to them via the WA DOH Managed File Transfer (MFT) site. 
#  The LNI team works with the WDRS Developers team for any additional work on this file. 
#  
#
#  **IMPORTANT ACRONYMS/PHRASES USED:**
#     -WDRS: Washington Disease Reporting System (the primary database for COVID-19 disease reporting information)
#     -LNI: Labor and Industires 
#     -db: database
#     -df: dataframe; a way to structure data within the base functions of R
#     -R Object: a set of .RData files with cleaned COVID-19 data to use as a more reliable/static dataset to pull data from compared to the live databse WDRS 
#     -xmatch or x-match: cross matching
#     -spectable: a table looking at specimen collection specifically 
#
#
#  This script is helpful to external DOH partners as it demonstrates how to create a standardized dataset for specific 
#  uses (in this case, occupational) by pulling variables relevant to other teams or departments. 
#  In this script decisions to keep or filter data is made based on the reliability of the data and also the request of the LNI team.
#  This script cleans and organizes data according to what the LNI team asked for and what the data quality team can provide for further analysis. 
#  Then, the script sends an automatic email to our partners to let them know we have manually uploaded the dataset we created through this script. 
#
################################################################################################
################################################################################################

library(dplyr)
library(lubridate)
library(data.table)
library(sendmailR)

# File generation ---------------------------------------------------------
###########################################################################

#reading in data in the form of R objects (.rdata files created from the dynamic database WDRS)
#these objects are kept in the same filepath, so we can reference the name "wdrs_objects" and the name of the data file each time instead of writing out the filepath each time 
wdrs_objects <- "filepath/"

load(paste0(wdrs_objects,"data.RData"))

load("more_data.RData")

load("even_more_data.RData")

# Format SpecTable and build pre-requisite tables
#these filtering dates were requested by the LNI team
filter_date <- '2022-01-01'
cste_filter_date <- '2023-01-01'
pandemic_start_date <- '2020-01-01'

#filters for positive cases 
WDRS__TEST__PERFORMED_postive_filter <- c("PCR/Nucleic Acid Test (NAT, NAAT, DNA)", 
                                          "RT-PCR",
                                          "Positive by sequencing only",
                                          "Antigen by Immunoassay (respiratory specimen)", 
                                          "Fluorescent immunoassay (FIA)",
                                          "Antigen by Immunocytochemistry (autopsy specimen)")
#more filters for positive cases
WDRS__RESULT_positive_filter <- c(
  "SARS-CoV-2 (virus causing COVID-19) detected",
  "SARS-CoV-2 (virus causing COVID-19) antigen detected",
  "Autopsy specimen - SARS-CoV-2 (virus causing COVID-19) antigen detected"
)

######Replace missing/invalid SPECIMEN__COLLECTION__DTTM 
#in this section, we ensure the dates chosen are appropriate given the differences we see in our DOH database between investigation dates vs lab dates vs create dates, etc.
SpecTable$INVESTIGATION_CREATE_DATE<-as.Date(SpecTable$INVESTIGATION_CREATE_DATE)

setDT(SpecTable)
cols<- c("SPECIMEN__COLLECTION__DTTM", "RESULT__REPORT__DTTM", "ANALYSIS__DTTM", "OBSERVATION__DTTM")

#if any date columns listed above are before pandemic_start_date (set in mise_en_place and usually 01-01-2020), replace date with NA
#updated filter to replace Sys.Date with INVESTIGATION_CREATE_DATE on 12-22-2022, DH
SpecTable[, (cols) :=  lapply(.SD, function(x) if_else(x < pandemic_start_date | x > INVESTIGATION_CREATE_DATE, NA_Date_, x)), .SDcols = cols]

# Create replacement date for when SPECIMEN_COLLECTION_DTTM is missing
# Hierarchy is: replace missing SPECIMEN_COLLECTION_DTTM with first non-missing among:
# OBSERVATION__DTTM, ANALYSIS__DTTM, RESULT__REPORT__DTTM, WELRS__PROCESSED__DTTM, INVESTIGATION_CREATE_DATE
#we have determined this is the most reliable hierarchy for dates based on our experience with our database 

SpecTable[, SPECIMEN__COLLECTION__DTTM2 := fcase(
  !is.na(SPECIMEN__COLLECTION__DTTM), SPECIMEN__COLLECTION__DTTM,
  !is.na(OBSERVATION__DTTM),  OBSERVATION__DTTM,
  !is.na(ANALYSIS__DTTM),  ANALYSIS__DTTM,
  !is.na(RESULT__REPORT__DTTM), RESULT__REPORT__DTTM,
  !is.na(WELRS__PROCESSED__DTTM), WELRS__PROCESSED__DTTM,
  !is.na(INVESTIGATION_CREATE_DATE), INVESTIGATION_CREATE_DATE,
  default = NA_Date_ )] 

SpecTable[,`:=`(SPECIMEN__COLLECTION__DTTM = SPECIMEN__COLLECTION__DTTM2)][,c('SPECIMEN__COLLECTION__DTTM2'):=NULL]

#Keep only labs from 2021 or later in spectable
SpecTable <- SpecTable[SPECIMEN__COLLECTION__DTTM >= filter_date,]

setDF(SpecTable)

#Keep post-mortem antigen and sequencing only tests from 2023 or later
#note that there should be no sequenciny only tests with dummy labs prior to 2023, but this filter will remove them just in case
SpecTable <- SpecTable %>% filter(!(WDRS__TEST__PERFORMED == 'Antigen by Immunocytochemistry (autopsy specimen)' & CREATE_DATE < cste_filter_date))
SpecTable <- SpecTable %>% filter(!(WDRS__TEST__PERFORMED == 'Positive by sequencing only' & CREATE_DATE < cste_filter_date))

# create labtable, a copy of SpecTable with fewer variables
LabTable<-SpecTable %>% select(
  CASE_ID,
  WDRS__RESULT,
  WDRS__TEST__PERFORMED,
  WDRS__RESULT__SUMMARY,
  TEST__METHOD,
  WDRS__PERFORMING__ORG,
  INVESTIGATION_CREATE_DATE
)

# Filter Labtable for tests that are PCR or antigen
LabTable <-
  LabTable %>% filter(
    WDRS__TEST__PERFORMED %in% WDRS__TEST__PERFORMED_postive_filter)

# Filter for positive lab results and make case level dataset.
PosLabs <-
  LabTable %>% filter(
    WDRS__RESULT__SUMMARY == "Positive" &
      WDRS__RESULT %in% WDRS__RESULT_positive_filter
  ) %>% select(CASE_ID) %>% distinct()

# Create dataframe of everyone who has a negative lab result in WDRS and make case level dataset.
NegLabsfromTable <-
  LabTable %>% filter(WDRS__RESULT__SUMMARY != "Positive" &
                        WDRS__RESULT__SUMMARY != "Test not performed") %>% select(CASE_ID) %>% distinct()

# Create LTCF and LabTestResult calculated variables
covid_cases <- right_join(GCD_COVID_19raw %>% select(CASE_ID, TEN_DAYS_PATIENT_HEALTHCARE_SETTING), covid_cases)

covid_cases <- covid_cases %>% 
  mutate(
    LTCF = factor(TEN_DAYS_PATIENT_HEALTHCARE_SETTING , levels = c("No", "Yes")),
    LTCF_type = TEN_DAYS_PATIENT_HEALTHCARE_SETTING_START_DATE_TYPE_FACILITY,
    LTCF_type = ifelse(is.na(LTCF_type), "Unknown", LTCF_type),
    LTCF_type = factor(LTCF_type , levels = c("Hospital", "Long term care", "Clinic", "Other", "Unknown")))

covid_cases <- covid_cases %>% 
  mutate(LabTestResult = case_when(
    CASE_ID %in% unlist(PosLabs) ~ "Positive",
    CASE_ID %in% unlist(NegLabsfromTable) ~ "Negative"))


# Here, we remove some of the variables that are either no longer supported in the WDRS database or LNI no longer wants included in the their dataset
# Removed RACE_ASIAN, RACE_BLACK_OR_AFRICAN_AMERICAN, RACE_OTHER_RACE, RACE_OTHER_RACE_SPECIFY, RACE_UNKNOWN, RACE_WHITE, and ETHNICITY from raw
# removing the following living status variables due to request from L&I: CovidDeath, DIED_ILLNESS,DIED_ILLNESS_REPORT,DEATH_DATE,LIVING_STATUS 
a<- covid_cases %>% select(CASE_ID, mhosp, LTCF, LTCF_type, AgeGroup, 
                           admitdate, CREATE_DATE, INVESTIGATION_CREATE_DATE, DOB, Hospital,
                           ACH, LabTestResult, CaseType,OCCUPATION,LAST_NAME,
                           FIRST_NAME,
                           MIDDLE_NAME,
                           SEX_AT_BIRTH,
                           ADDRESS_LINE1,
                           CITY,
                           STATE,
                           POSTAL_CODE,
                           ACCOUNTABLE_COUNTY,
                           CDC_EVENT_DATE_SARS,
                           SYMPTOM_ONSET_DATE,
                           POSITIVE_PCR_LAB_DATE_COVID19,
                           CDC_N_COV_2019_HOSPITALIZED,
                           CDC_N_COV_2019_HOSPITALIZED_ICU,
                           REPORTING_ADDRESS,
                           REPORTING_CITY,
                           REPORTING_STATE,
                           REPORTING_ZIPCODE,
                           County,
                           CDC_EVENT_DATE
)%>%
  filter(CREATE_DATE > '2020-01-20 00:00:00')


Include<- right_join(GCD_COVID_19raw %>% select(CASE_ID,OCCUPATION_EMPLOYER,OCCUPATION_BUSINESS_TYPE,
                                                OCCUPATION_TYPE,WORK_NAME,AGE_YEARS,ANY_UNDERLYING_MEDICAL_CONDITION,
                                                CARDIAC_DISEASE,CHRONIC_KIDNEY_DISEASE,CHRONIC_LIVER_DISEASE,
                                                CHRONIC_LUNG_DISEASE_EG,DIABETES), a) 

col_order <- c("CASE_ID",
               "LAST_NAME",
               "FIRST_NAME",
               "MIDDLE_NAME",
               "ACCOUNTABLE_COUNTY",
               "SEX_AT_BIRTH",
               "CDC_EVENT_DATE_SARS",
               "ANY_UNDERLYING_MEDICAL_CONDITION",
               "CARDIAC_DISEASE",
               "CHRONIC_KIDNEY_DISEASE",
               "CHRONIC_LIVER_DISEASE",
               "CHRONIC_LUNG_DISEASE_EG",
               "DIABETES",
               "DOB",
               "OCCUPATION",
               "OCCUPATION_EMPLOYER",
               "OCCUPATION_BUSINESS_TYPE",
               "OCCUPATION_TYPE",
               "WORK_NAME",
               "SYMPTOM_ONSET_DATE",
               "ADDRESS_LINE1",
               "CITY",
               "STATE",
               "POSTAL_CODE",
               "POSITIVE_PCR_LAB_DATE_COVID19",
               "CDC_N_COV_2019_HOSPITALIZED",
               "CDC_N_COV_2019_HOSPITALIZED_ICU",
               "REPORTING_ADDRESS",
               "REPORTING_CITY",
               "REPORTING_STATE",
               "REPORTING_ZIPCODE",
               "Hospital",
               "County",
               "ACH",
               "LabTestResult",
               "mhosp",
               "CDC_EVENT_DATE",
               "admitdate",
               "CREATE_DATE",
               "INVESTIGATION_CREATE_DATE",
               "AGE_YEARS",
               "LTCF",
               "LTCF_type",
               "AgeGroup",
               "CaseType")
Include <- Include[, col_order]


#Split table in half for Excel
half <- round(nrow(Include)/2)

Include1 <- Include[1:half, ]
Include2 <- Include[(half + 1):nrow(Include), ]


# File output paths
file_path <- "filepath"
filenm1 <- paste0("Occupation data - created " , Sys.Date(), ' - 1.csv')
filenm2 <- paste0("Occupation data - created " , Sys.Date(), ' - 2.csv')

# Save files
data.table::fwrite(Include1, paste0(file_path, filenm1), row.names = FALSE)
data.table::fwrite(Include2, paste0(file_path, filenm2), row.names = FALSE)

# Print reminder to upload both files in SFT
if(filenm1 %in% list.files(file_path) | filenm2 %in% list.files(file_path)){paste("The files were saved! REMINDER: Upload the Occupation Data file! Files are saved here:", file_path)}



# Email generation (after file upload) ------------------------------------
###########################################################################

#Generating email:

#this is the "from" line in an email 
f <- sprintf("<email@email.com>") 

# reply-to
rl <- c("<email@email.com>","<email@email.com>") 

# cc
cl <- c("<email@email.com>")

# subject
subj <- "Occupational Data uploaded to file transfer site"

# body
bdy <- glue::glue(
  "Hello,
  
This is a notice that Occupational Data have been updated and the csv has been uploaded to file transfer folder.

Thank you,
DQ Epis
")

#Sending email:
sendmailR::sendmail(f, rl, subj, bdy, cl, headers= list("Reply-To"="<email@email.com>"), control= list(smtpServer= "server.info"))