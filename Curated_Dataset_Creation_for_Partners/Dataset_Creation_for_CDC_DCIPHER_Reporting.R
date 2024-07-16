
#################################################################################################
#CDC DCIPHER REPORTING DATASET CREATION AND FORMATTING
#Dataset Creation for the CDC to Report WA DOH's COVID-19 Case Counts
################################################################################################
################################################################################################
#
#  This script queries WDRS to get COVID-19 case count data for the CDC's state-specific reporting portal DCIPHER (Data Collection and Integration for Public Health Event Responses) 
#  In this script we retrieve the appropriate fields CDC has specified, clean the data according to how the CDC wants it filtered, 
#  and format the final data in a format that the CDC specifies. 
#  
#
#  **IMPORTANT ACRONYMS/PHRASES USED:**
#     -WDRS: Washington Disease Reporting System (the primary database for COVID-19 disease reporting information)
#     -db: database
#     -df: dataframe; a way to structure data within the base functions of R
#     -R Object: a set of .RData files with cleaned COVID-19 data to use as a more reliable/static dataset to pull data from compared to the live databse WDRS 
#     -xmatch or x-match: cross matching
#     -CDC DCIPHER: Data Collection and Integration for Public Health Event Responses 
#     -MMWR Year: MMWR (Morbidity and Mortality Weekly Report) year refers to the first week of the year being the one that has at least 4 days in the calendar year. 
#          For example, if January 1st is Anytime Sun - Wednesday, that is the first week of the year (as you would expect). 
#          However, if January 1st is a Thurs - Sat, that week is not the first week of the year. 
#          If Jan 1st is a Thursday, Sunday the 4th will actually be the start of the first week of the new MMWR year.
#          It's confusing, but it's for the purpose of keeping reporting consistent, as reporting becomes complicated when you consider there are many important dates 
#          for disease progression-- Date of Onset, Date of Diagnosis, Lab Test Date, Reporting Date, etc. 
#      
#
#
#  This script is helpful to external DOH partners and other health departments as it demonstrates how to create a standardized dataset
#  according to the CDC's specifications and naming conventions for DCIPHER reporting. 
#  This script creates a .csv file with all of the information, formatting, and any other specifications the CDC has told us they want that is then 
#  uploaded to the DCIPHER portal within the SAMS system (Secure Access Management System) the CDC manages. 
#  This code also includes a Quality Assurance (QA) section where the script checks to make sure no issues have occurred with the file before we upload it to the CDC. 
#  This script also sends some automatic emails to let ourselves and others know that the files have been created and sometimes even case counts for that day. 
#  
################################################################################################
################################################################################################
############################ OUTLINE OF THE SCRIPT ###############################################
###############################################################################################
#--------------------Step1: Lines  25 - 758 create the DCIPHER data----------------------------------------------------
#--------------------Step2: Lines 759 - 802 run QA on the DCIPHER data-------------------------------------------------
#--------------------Step3: Lines 808 - 822 export the final DCIPHER data and IDs as csv's----------------------------------------
#--------------------Step4: Lines 832+ create an email message to email our team who then submits our report to CDC------------

# # Update packages
# update.packages(ask=F)


rm(list=ls(all=TRUE))

pacman::p_load(data.table, dplyr, MMWRweek, tictoc,lubridate)

tic("time for full script")
tic("time for script minus email")
tic("processing cases time")

basepath <- "filepath/"
wdrs_objects <- "filepath/"
gcd_raw<-"filepath/"

# Bring in WDRS denormalized data using the R data objects created daily by our surveillance team
# The R object "covid_cases" lists the total cases (so all R users are reporting/working with the same number of cases each day)
# The R object "GCD_COVID_19raw" contains data from WDRS we need to fill out part of the DCIPHER report

# Load current covid_cases:
load(file.path(wdrs_objects,"covid_cases.RData"))

# Load current GCD_COVID_19raw:
load(file.path(gcd_raw, "GCD_COVID_19raw.RData"))

# These are the variables we use from GCD_COVID_19raw:
GCD_COVID_19raw <- dplyr::select(GCD_COVID_19raw, CASE_ID, CDC_N_COV_2019_DGMQ_ID, 
                                 IS_PATIENT_HEALTH_CARE_WORKER_HCW, ANY_FEVER_SUBJECTIVE_MEASURED, ANY_FEVER_SUBJECTIVE_MEASURED_TEMPERATURE_KNOWN, 
                                 CHILLS, HEADACHE, MYALGIA, PHARYNGITIS, CDC_N_COV_2019_CONGESTION, COUGH, COUGH_PRODUCTIVE, COUGH_DRY, 
                                 DIFFICULTY_BREATHING, DYSPNEA, PNEUMONIA, ACUTE_RESPIRATORY_DISTRESS_SYNDROME, NAUSEA, VOMITING, DIARRHEA, 
                                 ABDOMINAL_PAIN, CDC_N_COV_2019_ANOSMIA, CDC_N_COV_2019_DYSGEUSIA_AGEUSIA, OTHER_SYMPTOMS, COMPLAINANT_ILL, 
                                 SUSPECTED_EXPOSURE_SETTING, ACCOUNTABLE_COUNTY, INVESTIGATOR, ETHNICITY, SEX_AT_BIRTH, 
                                 RACE_AMERICAN_INDIAN_OR_ALASKA_NATIVE, RACE_ASIAN, RACE_BLACK_OR_AFRICAN_AMERICAN, 
                                 RACE_NATIVE_HAWIAIIAN_OR_OTHER_PACIFIC_ISLANDER, RACE_OTHER_RACE, RACE_OTHER_RACE_SPECIFY, RACE_UNKNOWN, RACE_WHITE, 
                                 PREGNANCY_STATUS, BIRTH_DATE, AGE_YEARS, AGE_MONTHS, ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY, CURRENT_SMOKER, CORYZA, 
                                 ANY_UNDERLYING_MEDICAL_CONDITION, CHRONIC_HEART_DISEASE, DIED_ILLNESS, CDC_EVENT_DATE_SARS, LIVING_STATUS, DEATH_DATE,
                                 DOB, CDC_NOTIFICATION_FILE_TIMESTAMP, CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE, CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE, 
                                 CDC_N_COV_2019_HOSPITALIZED_ICU, CDC_N_COV_2019_HOSPITALIZED_MECHANICAL_VENTILATION_INTUBATION_REQUIRED, 
                                 CDC_N_COV_2019_HOSPITALIZED, CDC_N_COV_2019_OTHER_TRAVEL, CDC_N_COV_2019_OTHER_TRAVEL_DESCRIBE, OTHER_SYMPTOMS_SPECIFY, 
                                 DIABETES, CARDIAC_DISEASE, CHRONIC_LUNG_DISEASE_EG, CHRONIC_KIDNEY_DISEASE, CHRONIC_LIVER_DISEASE, 
                                 IMMUNOSUPPRESSIVE_THERAPY_DISEASE, POSITIVE_PCR_LAB_DATE_COVID19, POSITIVE_DEFINING_LAB_DATE_SARS, 
                                 CDC_N_COV_2019_TESTING_CHEST_XRAY, CDC_N_COV_2019_NCOV_ID)

if(any(is.na(covid_cases$SPECIMEN__COLLECTION__DTTM))) stop('Cases are missing SPECIMEN_COLLECTION_DTTM\nCheck covid_cases for missing data')

case_counter <- covid_cases %>% 
  select(CovidDeath, CaseType, SPECIMEN__COLLECTION__DTTM) %>% 
  mutate(mmwr_year = MMWRweek::MMWRweek(SPECIMEN__COLLECTION__DTTM)[,'MMWRyear']) %>% 
  filter(mmwr_year >= 2022)

covid_cases_death <- length(which(case_counter$CovidDeath=="Yes"))
covid_cases_pcr <- length(which(case_counter$CaseType == "PCR Positive"))
covid_cases_antigen <- length(which(case_counter$CaseType == "Antigen Positive"))
rm(case_counter)


# Keep CASE_ID and all fields in covid_cases that are calculated in its R script
covid_cases <- dplyr::select(covid_cases,"CASE_ID", 
                             'AgeFirstPos',
                             'AgeGroup',
                             'Hospital', 
                             'admitdate',
                             'mhosp',
                             'CovidDeath',
                             'RaceCount', 'Race_Eth',
                             'County',
                             'CaseType',
                             'SPECIMEN__COLLECTION__DTTM_Antigen',
                             'INVESTIGATION_CREATE_DATE_Antigen',
                             'LAST_POSITIVE_SPECIMEN__COLLECTION__DTTM_Antigen',
                             'SPECIMEN__COLLECTION__DTTM_PCR',
                             'INVESTIGATION_CREATE_DATE_PCR',
                             'LAST_POSITIVE_SPECIMEN__COLLECTION__DTTM_PCR',
                             'LAST_POSITIVE_SPECIMEN__COLLECTION__DTTM',
                             'SPECIMEN__COLLECTION__DTTM',
                             'INVESTIGATION_CREATE_DATE',
                             'any_under',
                             'Symptomatic')
                            

# Joining the objects "covid_cases" and "GCD_COVID_19raw"
cases <- left_join(covid_cases, GCD_COVID_19raw, by = "CASE_ID") 
rm(covid_cases, GCD_COVID_19raw)


#-------------------------start of recode script (formating the way CDC wants us to)-----------------------------------------

coded_vars <- NULL

#!#!#!#! Recode local_id:
coded_vars <- c(coded_vars, "local_id")

colnames(cases)[colnames(cases)=="CASE_ID"] <-"local_id"
cases$local_id <- as.character(cases$local_id)
#!#!#!#! End local_id recode


#!#!#!#! Recode nndss_id:
coded_vars <- c(coded_vars, "nndss_id")
cases$nndss_id <- cases$local_id # Add nndss_id as case.id so it matches 'local record id' in GenV2 MMG. 08/10/2022 PC
#!#!#!#! End nndss_id recode


### Convert cases to data.table:
setDT(cases)


#!#!#!#! Recode mmwr_year and mmwr_week:
coded_vars <- c(coded_vars, "mmwr_year", "mmwr_week")

#Assign MMWR Year and filter cases to exclude any 2020 and 2021 cases
cases[ , c("mmwr_year", "mmwr_week") := MMWRweek::MMWRweek(SPECIMEN__COLLECTION__DTTM)[,c('MMWRyear' ,'MMWRweek')]]

cases <- cases[mmwr_year >= 2022, ] #Filter out rows where mmwr_year is in 2020 or 2021

cases[ , "SPECIMEN__COLLECTION__DTTM" := NULL] #drop unneeded var


# QA for MMWR Year:
if(any(is.na(cases$mmwr_year))) stop("MMWR Year was not completely filled. Check for cases where 'DOH_ANALYTIC_MMWR_YEAR' is NA")
if(any(cases$mmwr_year>2023)) warning("Future MMWR Year assigned to some cases. Range of 'DOH_ANALYTIC_MMWR_YEAR': ", paste(range(cases$mmwr_year), collapse = '-'))
#!#!#!#! End mmwr_year recode


#!#!#!#! Recode current_status:
coded_vars <- c(coded_vars, "current_status")

cases[ , CaseType := ifelse(CaseType=="PCR Positive", 5, ifelse(CaseType=="Antigen Positive", 6, NA))]
setnames(cases, "CaseType", "current_status")
#!#!#!#! End current_status recode


#!#!#!#! Recode res_state:
coded_vars <- c(coded_vars, "res_state")
cases[ , res_state := "WA"]
#!#!#!#! End res_state recode


#!#!#!#! Recode state:
coded_vars <- c(coded_vars, "state")
cases[ , state := "WA"]
#!#!#!#! End state recode


#!#!#!#! Recode process_epix:
coded_vars <- c(coded_vars, "process_epix")

cases[ , process_epix := case_when(
  is.na(CDC_N_COV_2019_DGMQ_ID) == FALSE ~ "1", TRUE ~ "")]
#!#!#!#! End process_epix recode


#!#!#!#! Recode process_dgmqid:
coded_vars <- c(coded_vars, "process_dgmqid")

cases[ , process_dgmqid := case_when(
  is.na(CDC_N_COV_2019_DGMQ_ID) ==FALSE ~ CDC_N_COV_2019_DGMQ_ID, TRUE ~ "")]

cases[ , CDC_N_COV_2019_DGMQ_ID := NULL] #drop unneeded var
#!#!#!#! End process_dgmqid recode


#!#!#!#! Recode hc_work_yn:
coded_vars <- c(coded_vars, "hc_work_yn")

cases[ , hc_work_yn := case_when(
  IS_PATIENT_HEALTH_CARE_WORKER_HCW=="Y" ~ "1",IS_PATIENT_HEALTH_CARE_WORKER_HCW=="N" ~ "0",TRUE ~ "")]

cases[ , IS_PATIENT_HEALTH_CARE_WORKER_HCW := NULL] #drop unneeded var
#!#!#!#! End hc_work_yn recode


#!#!#!#! Recode pna_yn:
coded_vars <- c(coded_vars, "pna_yn")

cases[ , pna_yn := case_when(PNEUMONIA == "Yes" ~ 1,
                             PNEUMONIA == "No" ~ 0,
                             T ~ 9)]
#!#!#!#! End pna_yn recode


#!#!#!#! Recode sympstatus:
coded_vars <- c(coded_vars, "sympstatus")

cases[ , sympstatus := case_when(
  ANY_FEVER_SUBJECTIVE_MEASURED == "Yes" | 
    ANY_FEVER_SUBJECTIVE_MEASURED_TEMPERATURE_KNOWN == "Yes" | 
    CHILLS == "Yes" | 
    HEADACHE == "Yes" | 
    MYALGIA == "Yes" | 
    PHARYNGITIS == "Yes" | 
    CDC_N_COV_2019_CONGESTION == "Yes" | 
    COUGH == "Yes" | 
    COUGH_PRODUCTIVE == "Yes" | 
    COUGH_DRY == "Yes" | 
    DIFFICULTY_BREATHING == "Yes" | 
    DYSPNEA == "Yes" | 
    PNEUMONIA == "Yes" | 
    ACUTE_RESPIRATORY_DISTRESS_SYNDROME == "Yes" | 
    NAUSEA == "Yes" | 
    VOMITING == "Yes" | 
    DIARRHEA == "Yes" | 
    ABDOMINAL_PAIN == "Yes" | 
    CDC_N_COV_2019_ANOSMIA == "Yes" | 
    CDC_N_COV_2019_DYSGEUSIA_AGEUSIA == "Yes" | 
    OTHER_SYMPTOMS == "Yes" ~ "1", 
  COMPLAINANT_ILL == "No" ~ "0",
  COMPLAINANT_ILL == "Unknown" ~ "9")]

cases[ , c("ANY_FEVER_SUBJECTIVE_MEASURED_TEMPERATURE_KNOWN", "CDC_N_COV_2019_CONGESTION", "COUGH_PRODUCTIVE", "COUGH_DRY", 
          "DIFFICULTY_BREATHING", "PNEUMONIA", "CDC_N_COV_2019_ANOSMIA", "CDC_N_COV_2019_DYSGEUSIA_AGEUSIA", "COMPLAINANT_ILL") := NULL] #drop unneeded vars
#!#!#!#! End sympstatus recode


#!#!#!#! Recode exp_house:
coded_vars <- c(coded_vars, "exp_house")

cases[ , exp_house := case_when(SUSPECTED_EXPOSURE_SETTING =="Home" ~ "1",TRUE ~ "")]
#!#!#!#! End exp_house recode


#### Reformat ACCOUNTABLE_COUNTY

#Convert county FIPS codes to county names

map <- c("WA-1" = "Adams County",
         "WA-3" = "Asotin County",
         "WA-5" = "Benton County",
         "WA-7" = "Chelan County",
         "WA-9" = "Clallam County",
         "WA-11" = "Clark County",
         "WA-13" = "Columbia County",
         "WA-15" = "Cowlitz County",
         "WA-17" = "Douglas County",
         "WA-19" = "Ferry County",
         "WA-21" = "Franklin County",
         "WA-23" = "Garfield County",
         "WA-25" = "Grant County",
         "WA-27" = "Grays Harbor County",
         "WA-29" = "Island County",
         "WA-31" = "Jefferson County",
         "WA-33" = "King County",
         "WA-35" = "Kitsap County",
         "WA-37" = "Kittitas County",
         "WA-39" = "Klickitat County",
         "WA-41" = "Lewis County",
         "WA-43" = "Lincoln County",
         "WA-45" = "Mason County",
         "WA-47" = "Okanogan County",
         "WA-49" = "Pacific County",
         "WA-51" = "Pend Oreille County",
         "WA-53" = "Pierce County",
         "WA-55" = "San Juan County",
         "WA-57" = "Skagit County",
         "WA-59" = "Skamania County",
         "WA-61" = "Snohomish County",
         "WA-63" = "Spokane County",
         "WA-65" = "Stevens County",
         "WA-67" = "Thurston County",
         "WA-69" = "Wahkiakum County",
         "WA-71" = "Walla Walla County",
         "WA-73" = "Whatcom County",
         "WA-75" = "Whitman County",
         "WA-77" = "Yakima County")

cases$ACCOUNTABLE_COUNTY <- map[as.character(cases$ACCOUNTABLE_COUNTY)]


#Create additional county variables including setting interviewer county to case county (as stated to do so in Kim M's spreadsheet):


#!#!#!#! Recode healthdept:
coded_vars <- c(coded_vars, "healthdept")

cases[ , healthdept := ACCOUNTABLE_COUNTY]
#!#!#!#! End healthdept recode


#!#!#!#! Recode res_county:
coded_vars <- c(coded_vars, "res_county")

cases[ , res_county := ACCOUNTABLE_COUNTY]
#!#!#!#! End res_county recode


#!#!#!#! Recode interviewer_org:
coded_vars <- c(coded_vars, "interviewer_org")

cases[ , interviewer_org := ACCOUNTABLE_COUNTY]
#!#!#!#! End interviewer_org recode


#!#!#!#! Recode interviewer_fn & interviewer_ln:
coded_vars <- c(coded_vars, "interviewer_fn", "interviewer_ln")

#Split interviewer's first/lastname
cases$"interviewer_fn" <- sapply(strsplit(as.character(cases$INVESTIGATOR), "\\s"), `[`, 1)
cases$"interviewer_ln" <- sapply(strsplit(as.character(cases$INVESTIGATOR), "\\s"), `[`, 2)
cases[ , INVESTIGATOR := NULL] #Remove unneeded var
#!#!#!#! End interviewer_fn & interviewer_ln recode


#!#!#!#! Recode ethnicity:
coded_vars <- c(coded_vars, "ethnicity")

cases[ , ETHNICITY := toupper(ETHNICITY)] #Remove unneeded var

setnames(cases, "ETHNICITY", "ethnicity")

cases$ethnicity[cases$ethnicity == "HISPANIC, LATINO/A, LATINX"] <- "1"
cases$ethnicity[cases$ethnicity == "HISPANIC OR LATINO"] <- "1"
cases$ethnicity[cases$ethnicity == "NON-HISPANIC, LATINO/A, LATINX"] <- "0"
cases$ethnicity[cases$ethnicity == "NOT HISPANIC OR LATINO"] <- "0"
cases$ethnicity[cases$ethnicity == "UNKNOWN"] <- "9"
cases$ethnicity[cases$ethnicity == "PATIENT DECLINED TO RESPOND"] <- "9"

cases$ethnicity[is.na(cases$ethnicity)] <- "9"

#ethnicity QA:
if(!all(cases$ethnicity %in% c('0', '1', '9'))) {
  print(table(cases$ethnicity))
  stop('Check ethnicity var coding')
}
#!#!#!#! End ethnicity recode



#!#!#!#! Recode sex:
coded_vars <- c(coded_vars, "sex")

setnames(cases, "SEX_AT_BIRTH", "sex")

cases$sex[cases$sex == "Male"] <- "1"
cases$sex[cases$sex == "Female"] <- "2"
cases$sex[cases$sex == "Other"] <- "9"
cases$sex[is.na(cases$sex)] <- "9"
#!#!#!#! End sex recode


#!#!#!#! Recode race_aian, race_asian, race_black, race_nhpi, race_other, race_spec, race_unk, race_white:
coded_vars <- c(coded_vars, "race_aian", "race_asian", "race_black", "race_nhpi", "race_other", "race_spec", "race_unk", "race_white")

cases[ , race_aian := ifelse(RACE_AMERICAN_INDIAN_OR_ALASKA_NATIVE=="Y", "1", "0")]
cases[ , race_asian := ifelse(RACE_ASIAN=="Y", "1", "0")]
cases[ , race_black :=ifelse(RACE_BLACK_OR_AFRICAN_AMERICAN=="Y", "1", "0")]
cases[ , race_nhpi := ifelse(RACE_NATIVE_HAWIAIIAN_OR_OTHER_PACIFIC_ISLANDER=="Y", "1", "0")]
cases[ , race_other := ifelse(RACE_OTHER_RACE=="Y", "1", "0")]
cases[ , race_spec := ifelse(RACE_OTHER_RACE_SPECIFY=="Y", "1", "0")]
cases[ , race_unk := ifelse(RACE_UNKNOWN=="Y", "1", "0")]
cases[ , race_white := ifelse(RACE_WHITE=="Y", "1", "0")]

cases[ , c("RACE_AMERICAN_INDIAN_OR_ALASKA_NATIVE", "RACE_BLACK_OR_AFRICAN_AMERICAN", "RACE_NATIVE_HAWIAIIAN_OR_OTHER_PACIFIC_ISLANDER", 
           "RACE_OTHER_RACE", "RACE_OTHER_RACE_SPECIFY", "RACE_UNKNOWN", "RACE_WHITE") := NULL]
#!#!#!#! End race_aian, race_asian, race_black, race_nhpi, race_other, race_spec, race_unk, race_white recode


#!#!#!#! Recode pregnant_yn:
coded_vars <- c(coded_vars, "pregnant_yn")

cases[ , pregnant_yn := case_when(
  sex=="2" &  PREGNANCY_STATUS=="Pregnant" ~ "1",
  sex=="2" &  PREGNANCY_STATUS=="Postpartum" ~ "0",
  sex=="2" &  PREGNANCY_STATUS=="Neither pregnant nor postpartum" ~ "0",
  sex=="2" & PREGNANCY_STATUS=="Unknown" ~"9", TRUE ~ "")]

cases[ , PREGNANCY_STATUS := NULL] #Remove unneeded var
#!#!#!#! End pregnant_yn recode


#!#!#!#! Recode dob:
coded_vars <- c(coded_vars, "dob")
setnames(cases, "BIRTH_DATE", "dob")
#!#!#!#! End dob recode


#!#!#!#! Recode ageunit:
coded_vars <- c(coded_vars, "ageunit")

cases[ , ageunit := case_when(
  is.na(AGE_YEARS)==FALSE~ "1", 
  is.na(AGE_YEARS)==TRUE & is.na(AGE_MONTHS)==FALSE~ "2", 
  TRUE ~ "9")]
#!#!#!#! End ageunit recode


#!#!#!#! Recode age:
coded_vars <- c(coded_vars, "age")

cases[ , age := case_when(
  is.na(AGE_YEARS)==FALSE~ AGE_YEARS,
  is.na(AGE_YEARS)==TRUE & is.na(AGE_MONTHS)==FALSE~ AGE_MONTHS, 
  is.na(DOB) & AGE_YEARS==0 ~ "",
  TRUE ~ "")]

cases[ , c("AGE_YEARS", "AGE_MONTHS") := NULL] #Remove unneeded var
#!#!#!#! End age recode


#!#!#!#! Recode smoke_former_yn:
coded_vars <- c(coded_vars, "smoke_former_yn")
cases[ , smoke_former_yn := case_when(ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY=="Former Smoker" ~"1", TRUE ~ "")]
#!#!#!#! End smoke_former_yn recode


#!#!#!#! Recode smoke_curr_yn:
coded_vars <- c(coded_vars, "smoke_curr_yn")

cases[ , smoke_curr_yn := tolower(CURRENT_SMOKER)]
cases[ , smoke_curr_yn := ifelse(smoke_curr_yn == "yes", "1", ifelse(smoke_curr_yn == "no", "0", "9"))]

cases[ , CURRENT_SMOKER := NULL] #Remove unneeded var
#!#!#!#! End smoke_curr_yn recode


#!#!#!#! Recode chills_yn, myalgia_yn, runnose_yn, sthroat_yn, cough_yn, sob_yn, headache_yn, abdom_yn, diarrhea_yn, fever_yn, medcond_yn, acuterespdistress_yn:
coded_vars <- c(coded_vars, "chills_yn", "myalgia_yn", "runnose_yn", "sthroat_yn", "cough_yn", "sob_yn", 
                "headache_yn", "abdom_yn", "diarrhea_yn", "fever_yn", "acuterespdistress_yn", "medcond_yn")

oldvar<- c("CHILLS",	"MYALGIA",	"CORYZA",	"PHARYNGITIS",	"COUGH",	"DYSPNEA",	"HEADACHE",	"ABDOMINAL_PAIN",	"DIARRHEA",
            "ANY_FEVER_SUBJECTIVE_MEASURED", "ANY_UNDERLYING_MEDICAL_CONDITION", "ACUTE_RESPIRATORY_DISTRESS_SYNDROME")

for (v in oldvar) set(cases, .SD, j=v, recode(cases[[v]], "Yes" = "1", "No" = "0", "Unknown" = "9", .default = ""))

setnames(cases,
         c("CHILLS", "MYALGIA", "CORYZA", "PHARYNGITIS", "COUGH", "DYSPNEA", "HEADACHE", "ABDOMINAL_PAIN", "DIARRHEA", "ANY_FEVER_SUBJECTIVE_MEASURED", "ACUTE_RESPIRATORY_DISTRESS_SYNDROME", "ANY_UNDERLYING_MEDICAL_CONDITION"),
         c("chills_yn", "myalgia_yn", "runnose_yn", "sthroat_yn", "cough_yn", "sob_yn", "headache_yn", "abdom_yn", "diarrhea_yn", "fever_yn", "acuterespdistress_yn", "medcond_yn"))
#!#!#!#! End chills_yn, myalgia_yn, runnose_yn, sthroat_yn, cough_yn, sob_yn, headache_yn, abdom_yn, diarrhea_yn, fever_yn, medcond_yn, acuterespdistress_yn recode


#Put chronic heart disease into "other diseases"

#!#!#!#! Recode otherdis_spec & otherdis_yn:
coded_vars <- c(coded_vars, "otherdis_spec", "otherdis_yn")

cases[ , otherdis_spec := case_when(
  CHRONIC_HEART_DISEASE=='Yes'~ 'Chronic Heart Disease',
  ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY=="Crohn's" ~ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY,
  ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY=="obstructive sleep apnea, hypothyroid" ~ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY,
  TRUE ~ "")]

cases[ , otherdis_yn := case_when(
  CHRONIC_HEART_DISEASE=='Yes'~ '1',
  ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY=="Crohn's"~"1",
  ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY=="obstructive sleep apnea, hypothyroid"~'1',
  CHRONIC_HEART_DISEASE=='No'~ '0',
  CHRONIC_HEART_DISEASE=='Unknown'~ '9', TRUE ~ "")]

cases[ , otherdis_yn := case_when(
  DIED_ILLNESS=="Unknown" ~"1",
  is.na(DIED_ILLNESS)==FALSE ~ "0", TRUE ~ "")]

cases[ , c("CHRONIC_HEART_DISEASE", "ANY_UNDERLYING_MEDICAL_CONDITION_SPECIFY") := NULL] #Remove unneeded var
#!#!#!#! End otherdis_spec & otherdis_yn recode


#!#!#!#! Recode onset_dt & onset_unk:
coded_vars <- c(coded_vars, "onset_dt", "onset_unk")

cases[ , CDC_EVENT_DATE_SARS := ifelse(is.na(CDC_EVENT_DATE_SARS)|CDC_EVENT_DATE_SARS > as.Date(DEATH_DATE) & !is.na(DEATH_DATE), 
                                       NA, CDC_EVENT_DATE_SARS)]
cases[ , CDC_EVENT_DATE_SARS := as.character(as.Date(CDC_EVENT_DATE_SARS, origin = "1970-01-01"))]

#DCL: In line with everything else we report out, SYMPTOM_ONSET_DATE has been replaced with the hierarchical CDC_EVENT_DATE_SARS on 6/14
#DCL: CDC does not like the proxy, as symptom onset can occur after DOD using this hierarchy, so it has been switched back on 8/10. Definition CDC referenced:
#"From CRF instruction: If the case was symptomatic: What was the onset date? Enter symptom onset date in MM/DD/YYYY format. If the case was symptomatic, but onset date of symptoms is unknown, select "unknown symptom onset date."
#DCL: 8/14: CDC does not like our switched and has told us to use the proxy but "remove the part that allows symptom onset date to occur after death" - coded above
cases[ , onset_dt := as.POSIXct(CDC_EVENT_DATE_SARS, format = "%Y-%m-%d", tz="UTC")] ## replaces parse_date_time code
cases[ , onset_dt := format(onset_dt, "%m/%d/%Y")]

cases[ , onset_unk := case_when(is.na(CDC_EVENT_DATE_SARS)==TRUE ~ "1", TRUE ~ "")]

cases[ , CDC_EVENT_DATE_SARS := NULL] #Remove unneeded var
#!#!#!#! End onset_dt & onset_unk recode


#!#!#!#! Recode death_unk:
coded_vars <- c(coded_vars, "death_unk")

cases[ , death_unk := case_when(
  DIED_ILLNESS=="Unknown" ~ "1", TRUE ~ "")]

cases[ , death_unk := ifelse(death_unk == "1" & LIVING_STATUS == 1,"", death_unk)]

cases[ , DIED_ILLNESS := NULL]
#!#!#!#! End death_unk recode


#!#!#!#! Recode case_cdcreport_dt:
coded_vars <- c(coded_vars, "case_cdcreport_dt")

#Create CDC notification date
cases[ , date := substr(CDC_NOTIFICATION_FILE_TIMESTAMP, 1, 8)]
cases[ , date := paste(substr(date, 1, 4),"-",substr(date, 5, 6),"-", substr(date, 7, 8), sep="")] ## replaces parse_date_time code

cases[ , case_cdcreport_dt := format(as.POSIXct(as.character(date),format = "%Y-%m-%d", tz = "UTC"), "%m/%d/%Y")]

cases <- cases[ , c("CDC_NOTIFICATION_FILE_TIMESTAMP", "date") := NULL]
#!#!#!#! End case_cdcreport_dt recode


#!#!#!#! Recode adm1_dt:
coded_vars <- c(coded_vars, "adm1_dt")

#Clean and search through the hospital variables values
cases[ , CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE := gsub("\\D", "", CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE)] 
cases[ , CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE := format(as.Date(substr(CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE,1,8), "%m%d%Y"), "%m/%d/%Y")] # replaces mdy code
cases[ , CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE := gsub("(.*),.*", "\\1", CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE)] #Fix hospitalization dates

setnames(cases, "CDC_N_COV_2019_HOSPITALIZED_ADMISSION_DATE", "adm1_dt")
#!#!#!#! End adm1_dt recode


#!#!#!#! Recode adm1_dt:
coded_vars <- c(coded_vars, "dis1_dt")

#Clean and search through the hospital variables values
cases[ , CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE := gsub("\\D", "", CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE)]
cases[ , CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE := format(as.Date(substr(CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE,1,8), "%m%d%Y"), "%m/%d/%Y")] # replaces mdy code
cases[ , CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE := gsub("(.*),.*", "\\1", CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE)] #Fix hospitalization dates

setnames(cases, "CDC_N_COV_2019_HOSPITALIZED_DISCHARGE_DATE", "dis1_dt")
#!#!#!#! End dis1_dt recode


#!#!#!#! Recode icu_yn, mechvent_yn, othsym1_yn:
coded_vars <- c(coded_vars, "icu_yn", "mechvent_yn", "othsym1_yn")

#Create hospitalized overnight/ICU/ventilator
cases$CDC_N_COV_2019_HOSPITALIZED_ICU <- ifelse(grepl("Yes",cases$CDC_N_COV_2019_HOSPITALIZED_ICU),'Yes',
                                                ifelse(grepl("No",cases$CDC_N_COV_2019_HOSPITALIZED_ICU),'No',
                                                       ifelse(grepl("Unknown",cases$CDC_N_COV_2019_HOSPITALIZED_ICU),'Unknown',NA)))
cases$CDC_N_COV_2019_HOSPITALIZED_MECHANICAL_VENTILATION_INTUBATION_REQUIRED <- ifelse(grepl("Yes",cases$CDC_N_COV_2019_HOSPITALIZED_MECHANICAL_VENTILATION_INTUBATION_REQUIRED),'Yes',
                                                                                       ifelse(grepl("No",cases$CDC_N_COV_2019_HOSPITALIZED_MECHANICAL_VENTILATION_INTUBATION_REQUIRED),'No',
                                                                                              ifelse(grepl("Unknown",cases$CDC_N_COV_2019_HOSPITALIZED_MECHANICAL_VENTILATION_INTUBATION_REQUIRED),'Unknown',NA)))

#Now, re-format/create
oldvar<-c("OTHER_SYMPTOMS", "CDC_N_COV_2019_HOSPITALIZED_ICU", "CDC_N_COV_2019_HOSPITALIZED_MECHANICAL_VENTILATION_INTUBATION_REQUIRED")

for (v in oldvar) set(cases, .SD, j=v, recode(cases[[v]], "Yes" = "1", "No" = "0", "Unknown" = "9", .default = ""))

#Now, rename 
#DCL 3/29: HOSPITALIZED_LEAST_OVERNIGHT* variables renamed CDC_N_COV_2019_HOSPITALIZED* as of 3/28
setnames(cases, 
         c("CDC_N_COV_2019_HOSPITALIZED_ICU", "CDC_N_COV_2019_HOSPITALIZED_MECHANICAL_VENTILATION_INTUBATION_REQUIRED", "OTHER_SYMPTOMS"), 
         c("icu_yn", "mechvent_yn", "othsym1_yn"))
#!#!#!#! End icu_yn, mechvent_yn, othsym1_yn recode


#!#!#!#! Recode hosp_yn:
coded_vars <- c(coded_vars, "hosp_yn")

setnames(cases, "mhosp", "hosp_yn")
cases$hosp_yn[cases$hosp_yn == "Yes"] <- "1" 
cases$hosp_yn[cases$hosp_yn == "No"] <- "0"
cases$hosp_yn[cases$hosp_yn == "Unknown"] <- "9"
table(cases$hosp_yn)
#!#!#!#! End hosp_yn recode


#!#!#!#! Recode exp_othcountry, exp_othcountry_spec
coded_vars <- c(coded_vars, "exp_othcountry", "exp_othcountry_spec")

#Recoding travel vars
oldvar<-c("CDC_N_COV_2019_OTHER_TRAVEL", "CDC_N_COV_2019_OTHER_TRAVEL_DESCRIBE")
for (v in oldvar) set(cases, .SD, j=v, recode(cases[[v]], "Yes" = "1", "No" = "0", "Unknown" = "9", .default = ""))

setnames(cases, 
         c("CDC_N_COV_2019_OTHER_TRAVEL", "CDC_N_COV_2019_OTHER_TRAVEL_DESCRIBE"), 
         c("exp_othcountry", "exp_othcountry_spec"))

#Recode exposure and housing, added 5/18
cases[ , exp_othcountry := ifelse(SUSPECTED_EXPOSURE_SETTING=="International travel" & (is.na(exp_othcountry) | exp_othcountry=="") ,"1",exp_othcountry)]
cases[ , SUSPECTED_EXPOSURE_SETTING := NULL] #Remove unneeded var
#!#!#!#! End exp_othcountry, exp_othcountry_spec recode


#!#!#!#! Recode nauseavomit_yn
coded_vars <- c(coded_vars, "nauseavomit_yn")

#Create nausea or vomiting combined 
cases[ , nauseavomit_yn := case_when(
  NAUSEA=="Yes" |  VOMITING=="Yes" ~ "1",
  NAUSEA=="No"  | VOMITING=="No" ~ "0",
  NAUSEA=="Unknown" ~ "9",
  VOMITING=="Unknown" ~ "9", TRUE ~ "")]

cases[ , c("NAUSEA", "VOMITING") := NULL] #drop unneeded vars
#!#!#!#! End nauseavomit_yn recode


#!#!#!#! Recode othsym1_spec1
coded_vars <- c(coded_vars, "othsym1_spec1")

cases[ , othsym1_spec1 := case_when(!is.na(OTHER_SYMPTOMS_SPECIFY) ~ OTHER_SYMPTOMS_SPECIFY, TRUE ~ "")]

cases[ , OTHER_SYMPTOMS_SPECIFY := NULL] #drop unneeded var
#!#!#!#! End othsym1_spec1 recode


#!#!#!#! Recode diabetes_yn, cvd_yn, cld_yn, renaldis_yn, liverdis_yn, immsupp_yn
coded_vars <- c(coded_vars, "diabetes_yn", "cvd_yn", "cld_yn", "renaldis_yn", "liverdis_yn", "immsupp_yn")

#Recode pre-existing conditions
oldvar<-c("DIABETES", "CARDIAC_DISEASE", "CHRONIC_LUNG_DISEASE_EG", "CHRONIC_KIDNEY_DISEASE", "CHRONIC_LIVER_DISEASE", "IMMUNOSUPPRESSIVE_THERAPY_DISEASE")

for (v in oldvar) set(cases, .SD, j=v, recode(cases[[v]], "Yes" = "1", "No" = "0", "Unknown" = "9", .default = ""))

setnames(cases,
         c("DIABETES", "CARDIAC_DISEASE", "CHRONIC_LUNG_DISEASE_EG", "CHRONIC_KIDNEY_DISEASE", "CHRONIC_LIVER_DISEASE", "IMMUNOSUPPRESSIVE_THERAPY_DISEASE"),
         c("diabetes_yn", "cvd_yn", "cld_yn", "renaldis_yn", "liverdis_yn", "immsupp_yn"))
#!#!#!#! End diabetes_yn, cvd_yn, cld_yn, renaldis_yn, liverdis_yn, immsupp_yn recode


#!#!#!#! Recode pos_spec_dt
coded_vars <- c(coded_vars, "pos_spec_dt")

#Recode first positive lab date (based on PCR lab testing data)
cases$POSITIVE_PCR_LAB_DATE_COVID19<- ifelse(as.Date(cases$POSITIVE_PCR_LAB_DATE_COVID19, origin = "1970-01-01") == "2030-01-01" | as.Date(cases$POSITIVE_PCR_LAB_DATE_COVID19, origin = "1970-01-01") >= as.Date(as.Date(Sys.time())) , NA, cases$POSITIVE_PCR_LAB_DATE_COVID19) # replaces today function.
class(cases$POSITIVE_PCR_LAB_DATE_COVID19)<-"Date"
cases$pos_spec_dt<-as.POSIXct(cases$POSITIVE_PCR_LAB_DATE_COVID19, format = "%Y-%m-%d")       ## replaces parse_date_time function
cases$pos_spec_dt <-  as.Date(as.POSIXct(cases$pos_spec_dt, format = "%Y-%m-%d", tz = "UTC"))
cases$pos_spec_dt<-format(cases$pos_spec_dt, "%m/%d/%Y")

cases[ , POSITIVE_PCR_LAB_DATE_COVID19 := NULL] #drop unneeded var
#!#!#!#! End pos_spec_dt recode


#!#!#!#! Recode pos_spec_unk
coded_vars <- c(coded_vars, "pos_spec_unk")

setnames(cases, "POSITIVE_DEFINING_LAB_DATE_SARS", "pos_spec_unk")
cases[ , pos_spec_unk := case_when(is.na(pos_spec_unk)==TRUE ~"1", TRUE ~"")]
#!#!#!#! End pos_spec_unk recode


#!#!#!#! Recode abxchest_yn
coded_vars <- c(coded_vars, "abxchest_yn")

setnames(cases, "CDC_N_COV_2019_TESTING_CHEST_XRAY", "abxchest_yn")

cases[ , abxchest_yn := case_when(
  abxchest_yn=="Abnormal" ~ "1",
  abxchest_yn=="Normal" ~ "0",TRUE ~"")]
#!#!#!#! End abxchest_yn recode


#!#!#!#! Recode death_yn & death_dt:
coded_vars <- c(coded_vars, "death_yn", "death_dt")

#Recode death using death file
#With removal of 2020 cases, it should not effect death QA since extra deaths should not be added to 2021+ cases as they are matched on case_id. Keep an eye on this though.
wa_cov_dth <- read.csv("filepath/.csv", stringsAsFactors = F)

wa_cov_dth <- wa_cov_dth[ , c("CASE_ID", "dod_wh")]

wa_cov_dth$dod_wh <- as.Date(wa_cov_dth$dod_wh)
#if DOD doesn't exisit:
#select(WDRS.ID, WDRS.DEATH.DATE) %>% 
#mutate(WDRS.DEATH.DATE = lubridate::mdy(WDRS.DEATH.DATE))
#if neither of above exist:
#select(WDRS.ID, DOD_WH) %>% 
#mutate(DOD_WH = lubridate::mdy(DOD_WH))
#wa_cov_dth$WDRS.ID <- as.character(wa_cov_dth$WDRS.ID)

setnames(wa_cov_dth, "CASE_ID", "local_id")
wa_cov_dth$local_id <- as.character(wa_cov_dth$local_id)

#Recode death_yn
#For death, make 9 as death is later confirmed using the death file; as of 8/28: new living status changes over the weekend will be unconfirmed until Mondays
cases$death_yn<-ifelse(cases$CovidDeath == "Yes" | cases$LIVING_STATUS == 1, "9", "")
cases[ , death_yn := ifelse(cases$local_id %in% wa_cov_dth$local_id, "1", death_yn)]

cases[ , c("CovidDeath", "LIVING_STATUS") := NULL] #drop unneeded vars
#End death_yn recode


#Recode death_dt
cases <- merge(cases, wa_cov_dth, by = c("local_id"), all.x = TRUE)
cases[ , death_dt := rep(NA, .N)] 
cases[ , death_dt := coalesce(dod_wh)] 
cases[ , death_dt := format(as.Date(death_dt), "%m/%d/%Y")]
cases[ , dod_wh := NULL]
#End death_dt recode
#!#!#!#! End death_yn & death_dt recode


#!#!#!#! Recode cdc_ncov2019_id
coded_vars <- c(coded_vars, "cdc_ncov2019_id")

#CDC has been combining state and local ID for those records missing cdc_ncov2019_id, we can do that using this code:
setnames(cases, "CDC_N_COV_2019_NCOV_ID", "cdc_ncov2019_id")
cases[ , cdc_ncov2019_id := ifelse(is.na(cdc_ncov2019_id),paste0("WA", local_id), cdc_ncov2019_id)]
#!#!#!#! End cdc_ncov2019_id recode


cases <- as.data.frame(cases)


#-------------------------end of recode script-----------------------------------------

#------------------------Skip lab data, get data in CDC order ------------------------------------

# Bring in a list of field names CDC wants in the DCIPHER report
cdc_var<- read.csv(paste0(basepath, "/CDC documentation/COVID19-CSV-Template_2022May18.csv")) %>% as.data.frame() # Pull CDC variable list
cdclistvars<-colnames(cdc_var) 

#add blank columns for the vars that were added in the old lab code and vars we're not sending yet
cdcvar_uncoded <- cdclistvars[!(cdclistvars %in% colnames(cases))]
cases[, cdcvar_uncoded] <- NA

export <- cases %>% 
  select(all_of(cdclistvars)) %>% # Search lab data for matching variables
  filter(!duplicated(local_id)) %>% # Drop any duplicates by local_id
  mutate(across(everything(), as.character)) # Convert all columns to character class

dcipher_id_export<-export%>%
  select(cdc_ncov2019_id,local_id,pos_spec_dt) %>%
  unique()%>%
  dplyr::rename(`local_id (WDRS case ID)`=local_id,
                `pos_spec_dt (first positive lab date based on positive PCR)`= pos_spec_dt)


#--------------------Created the final dataframe! QA is next!------------------------------------
toc()
tic("time for QA") #this line lets us know how long the script ran up until this line and how long it will run thr QA process
#--------------------Let the QA process begin----------------------------------------------------

#The code below creates QA output shown to you in the console, if you see all PASS's you can continue!
#QA1 checks to see if all of the variables coded throughout the script are included in the final export.
#QA2 checks to see if the number of deaths we have are less than or equal to those in the death file
#QA3 checks to see if the number of cases in 'export' is equal to those in 'covid_cases'
#QA4 
#  Other QA that could be added:
#  A QA6 that compares completeness per column in 'export' with yesterday's report (in sent to CDC), if 5%+ less copmlete QA fails
#  A QA7 that makes sures no NA-counties/Unknown/etc. view(freq(cases$res_county)); table(is.na(export$res_county))

QA1 <- ifelse(all(coded_vars %in% colnames(export)),
              paste0("(PASS) ",length(coded_vars)," out of ",length(coded_vars)," expected coded fields found"),
              paste0("(possible FAIL) 'export' is missing coded fields. Make sure these fields do not need to be submitted to CDC anymore: ", paste(coded_vars[!(coded_vars %in% colnames(export))], collapse=", ")))
QA2 <- ifelse(length(which(export$death_yn == 1))==covid_cases_death,
              
              paste0("(PASS) we have ",length(which(export$death_yn == 1)), " confirmed deaths and covid_cases has ",
                     covid_cases_death, " confimed deaths"),
              
              paste0("(FAIL) we have ",length(which(export$death_yn == 1 )), " confirmed deaths",", while covid_cases has ",
                     covid_cases_death, " confimed deaths" ))


QA3 <- ifelse(length(which(export$current_status ==5))==covid_cases_pcr & 
                length(which(export$current_status == 6))==covid_cases_antigen,
              paste0("(PASS) we have ",length(which(export$current_status == 5)), " confirmed cases and ", 
                     length(which(export$current_status == 6)), " probable cases, while covid_cases has ", covid_cases_pcr, " confimed cases and ",
                     covid_cases_antigen, " probable cases"),
              
              paste0("(possible FAIL) we have ",length(which(export$current_status == 5)), " confirmed and ", length(which(export$current_status == 6)),
                     " probable cases, covid_cases has ", covid_cases_pcr, " confirmed cases and ",  
                     covid_cases_antigen, " probable cases"
              ))

#added qa for newly filled nndss_id variable:
QA4 <- ifelse(length(export$nndss_id) == length(unique(export$nndss_id)),
              paste("(PASS) There are", length(export$nndss_id), "nndss_id's and", length(unique(export$nndss_id)), "unique nndss_id's"),
              paste("(FAIL) Possible duplicates in nndss_id. There are", length(export$nndss_id), "nndss_id's, but only", length(unique(export$nndss_id)), "unique nndss_id's"))

QA_checker <- data.frame(QA_Results = c(QA1, QA2, QA3, QA4))
print(QA_checker,right=F)

toc()

#--------------------End of QA! Next, create csv and email----------------------------------------------------

#--------------------Save 'export' as a csv in the Send to CDC folder-----------------------------------------

tic("saving CSVs time")

#Save the DCIPHER report in the 'Sent to CDC folder', add a time stamp to the file name (using the WDRS data object date)
timestamp <- file.info(file.path(wdrs_objects, "/covid_cases.RData"))$mtime
timestamp <- format(timestamp, "%Y%m%d%H%M")
Summary<-paste0(basepath, "/Archive/Sent to CDC", "/NCOV2019_CASELIST_WA_", timestamp, ".csv")
data.table::fwrite(export, Summary, na="", row.names = FALSE)

# Save IDs from the DCIPHER Report - to send to team doing TB surveillance so they can identify cases that had both TB and COVID
fwrite(dcipher_id_export,paste0("filepath/- ",Sys.Date(),".csv"))
#####

print("Don't forget to upload the DCIPHER report to CDC!")

toc()
#--------------------End of creating csv in Send to CDC folder-----------------------------------------

toc()

#--------------------Create email messages ------------------------------------------------------------

### EMAIL GREETING ###
### EMAIL GREETING ###
### EMAIL GREETING ###

currentHour<-format(Sys.time(),"%H")

if(currentHour<12){
  
  greeting<- "Good Morning,"
} else if (between(currentHour,12,16)){
  
  greeting<-"Good Afternoon,"
  
} else{
  greeting<-"Good Evening,"
  
}


library(sendmailR)

##### The code below sends an email to Surveillance lead to submit the Case Count Confirmation survey

# First, load in the 2020-2021 case counts:
load("filepath/.RData")
load("filepath/.RData")

# Next, add 2020 case/death totals to current data for cumulative total
confcases <- length(which(export$current_status == 5)) + confcases_2020 + confcases_2021
probcases <- length(which(export$current_status == 6)) + probcases_2020 + probcases_2021
confdeaths <- length(which(export$death_yn == "1")) + confdeaths_2020 + confdeaths_2021

#Generating email:

# from
f <- sprintf("<email@email.com>") 

# reply-to
rl <- c("<email@email.com>") 

# cc
cl <-sprintf(c("<email@email.com>", "<email@email.com>"))

# subject
subj <- "DCIPHER case confirmation numbers are ready"

# attachment
image <-mime_part(x='filepath.png')

# body
bdy <- glue::glue(
"
{greeting}
  
The DCIPHER case calculations are ready for the CDC Case Count Confirmation survey.

A - Contact Info: Enter your first and last name, and work email and phone number 

B - Jurisdiction: Select \"Washington\" from the drop down menu 

C - Transmission: Select \"Yes, widespread community transmission across several geographical areas\" from the drop down menu 

D - CDC numbers: This section should already be populated with the count from yesterday 

E - Confirm values: If the numbers need updating, select \"No, we have updated numbers\" from the drop down menu 

F - Updated numbers: If the numbers need updating, enter the values below into the corresponding field 

     Confirmed cases: {confcases}     Probable cases: {probcases}

     Confirmed deaths: {confdeaths}     Probable deaths: 0

Thank you,
DQ Epis
")

# save body as backup:
fwrite(cbind("Date" = paste(Sys.Date()), "Confirmed Cases" = confcases, "Probable Cases" = probcases, 
             "Confirmed Deaths" = confdeaths, "Probable Deaths" = 0) %>% as.data.table(), 
       file = "filepath.txt")

# body w/attachment
bdy <- list(bdy, image)

#Sending email:
sendmailR::sendmail(f, rl, subj, bdy, cl, headers= list("Reply-To"="<email@email.com>"), control= list(smtpServer= "relay.info"))
#



##### The code below sends an email to TB Surveillance team - IDs from the DCIPHER file
filepath <-  paste0("filepath/ - ",Sys.Date(),".csv")

#Generating email:

# from
f <- sprintf("<email@email.com>") 

# reply-to
rl <- c("<email@email.com>") 

# cc
cl <-sprintf("<email@email.com>")

# subject
subj <- "IDs from the DCIPHER file"

# body
bdy <- glue::glue(
  "
{greeting}
  
Below is the file path where you can find a CSV with the IDs from the DCIPHER file. 

File path: {filepath}


Thank you,
DQ Epis
")


#Sending email:
sendmailR::sendmail(f, rl, subj, bdy, cl, headers= list("Reply-To"="<email@email.com>"), control= list(smtpServer= "relay.info"))
#



toc()

if (file.exists(paste0("filepath/ - ",Sys.Date(),".csv")) & 
    file.exists(Summary)) {
  rm(list=ls())
  gc()
}
