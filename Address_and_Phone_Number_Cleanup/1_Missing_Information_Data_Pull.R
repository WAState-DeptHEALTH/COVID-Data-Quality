################################################################################################
#PART 1: PULLING IN MISSING/INCOMPLETE DATA
################################################################################################
################################################################################################
#
#  This process utilizes two internal databases (one for hospitalizations, one for immunizations) in order to enrich
#  contact information (phone numbers and address) of COVID-19 cases and contacts for improved case investigation and contact tracing. 
#  If a phone number or address is missing from case/contact information, it makes it more difficult for downstream case investigators to
#  successfully reach cases and contacts. This data quality-focused process finds missing phone/address information by cross-referencing other internal databases
#  that may have this information.
#
#  For example, a COVID-19 case (ID #123456) is missing a phone number. This script will find that case and put it into a line-level roster of other cases with 
#  missing information. The roster will be cross-matched in a separate process with an immunization database where the case #123456 does have a phone number 
#  listed after a flu vaccination the previous year. Now that we have found a phone number associated with case #123456's, we will create a roster 
#  of the newly-discovered information and it will be put into the COVID-19 database. 
#  
#
#  **This script addresses the first part of this process: pulling data with missing contact information and creating a roster.**
#  
#  This script runs two parts: 
#
#  First, all cases meeting phone number pull criteria:
#  -  SPECIMEN__COLLECTION__DTTM within 19 days
#  -  WDRS__RESULT__SUMMARY is positive & case is confirmed or probable (follows R objects filters)
#  -  Contact look up has not been completed already (CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE=='yes')
#  -  status == 0 (open) to exclude deleted and closed cases
#  These cases then go through a process to convert bad phone numbers to NA
#  Cases that do not have any phone numbers left (after the bad numbers are converted) are kept for
#  phone number matching and, possibly, look up.
#
#  Next, cases with bad addresses are selected from geocoded_covid_cases to include only cases where
#  geocoding could not be accurately completed (geomatchstatus == Not matched, or geoaccuracy does not equal "Close").
#  These cases are further filtered to include only those where SPECIMEN__COLLECTION__DTTM is within 19 days. 
#  If age.restriction is set to TRUE: later in the script, age is calculated based on DOB and 
#  SPECIMEN__COLLECTION__DTTM, then cases are filtered by CICT criteria: age is 22 and younger, or 50 and older.
#  
#  Cases are further filtered to exclude PO Box addresses, as these addresses are complete mailing addresses, but cannot be geocoded.
#  To ensure PO Box addresses that are missing numbers (e.g., 'P O BOX') still go through matching, street addresses that do not contain 
#  numbers are converted to NA. Afterward, street addresses containing variants of 'PO BOX' are excluded. Then, all address components 
#  for remaining cases (street, city, state, zip) are converted to NA.
#
#  Finally, addresses and phones cases are combined into one data frame. Badnames and oos_ord_fac (out of state ordering facilities) data frames are also combined and
#  written as csv's in their final location. The final mco data frame is saved as an .rds file to be used in IIS matching.
#
#  After Data Support is emailed case counts that are undergoing matching today, the file used by RHINO for xmatching is created and saved.
#  This section (block 5) needs to be run before RHINO starts their daily xmatching process at 9:30 AM in order for RHINO data to be received.
#
#
#  **IMPORTANT ACRONYMS/PHRASES USED:**
#     -WDRS: Washington Disease Reporting System (the primary database for COVID-19 disease reporting information)
#     -db: database
#     -df: dataframe; a way to structure data within the base functions of R
#     -R Object: a set of .RData files with cleaned COVID-19 data to use as a more reliable/static dataset to pull data from compared to the live databse WDRS 
#     -RHINO: Rapid Health Information NetwOrk. This internal syndromic surveillance database contains hospitalization information that we use to enrich phone/address data for contact investigation
#     -IIS: Immunization Information System. This registry holds records for Washington state residents who have received an immunization that we use to enrich phone/address data for contact investigation
#     -CICT: Case Investigation and Contact Tracing
#     -xmatch or x-match: cross matching
#     -geocoding: takes addresses and determines whether it is a location on a map (in this instance it helps determine whether an address is more "real" and not 123 Sesame Street)
#     -OOS: out of state
#     -mco: missing contact info
#
#
################################################################################################
################################################################################################

# Change this to TRUE if CICT age cut offs should be used to filter down cases being xmatched and sent to DS i.e. during a large surge.
age.restriction = F

# Load credentials:
# "R_Creds"
# Here, an external .rds file is referenced in order to call database names, emails, usernames, passwords, etc. in a more secure way by
# keeping sensitive information outside of the script. 
# The file is created in a separate R script, saved as a .rds file, (referred to in this script and subsequent scripts as "r_creds"
# and kept in a standard file location across team members so that the script stays the same regardless of 
# who is running it and on what computer.

pacman::p_load(tidyr, 
               dplyr, 
               stringr, 
               lubridate, 
               tidyselect, 
               readxl,
               glue)

# funs:
empty_as_na <- function(x) ifelse(trimws(x) == '', NA, x)
"%ni%" <- function(x, table) match(x, table, nomatch = -1) < 0

#Used for pulling badnames from phones and addresses:
badnames_fun <- function(df) {
  df <- df %>% filter(grepl("#|[0-9]", FIRST_NAME) |
                        grepl("#|[0-9]", LAST_NAME) |
                        LAST_NAME %in% c('Qqqaatest', 'Test') |
                        grepl('unknown', FIRST_NAME, ignore.case = T) |
                        grepl('unknown', LAST_NAME, ignore.case = T) |
                        grepl('Lnu', LAST_NAME, ignore.case = T))
  return(df)
}

#Used for pulling oos_ord_facs from phones and addresses:
oos_ord_fac_fun <- function(df) {
  df <- df %>% filter(tolower(ORDFAC__NAME) %in% c('ordering facility names'))
  return(df)
}


################################################################################################
################    Block 1     ################################################################
################ Missing Phones ################################################################
################################################################################################

# set date macro variables:
lookback.days <- ifelse(wday(Sys.Date()) == 2, 21, 19) #extend lookback by 2 days if run on Monday
# if holiday, extend lookback manually:
# lookback.days <- 19 + x #where x is days since last run

lookback <- as.POSIXct(paste(Sys.Date()-lookback.days, '00:00:00'), tz = 'UTC') #Look back 19 days to match WA notify criteria
rm(lookback.days)
# Note: if case counts get high during a large surge and script run time or time for manual review needs to be reduced,
#       change the phone lookback to be a more recent date. E.g., Sys.Date()-10 for 10 days ago instead of 19.

TODAY <- Sys.Date()
today <- format(TODAY, "%Y%m%d")


# set filters for positive cases:
WDRS__TEST__PERFORMED_postive_filter <- c("PCR/Nucleic Acid Test (NAT, NAAT, DNA)", 
                                          "RT-PCR",
                                          "Positive by sequencing only",
                                          "Antigen by Immunoassay (respiratory specimen)", 
                                          "Fluorescent immunoassay (FIA)",
                                          "Antigen by Immunocytochemistry (autopsy specimen)")

WDRS__RESULT_positive_filter <- c("SARS-CoV-2 (virus causing COVID-19) detected",
                                  "SARS-CoV-2 (virus causing COVID-19) antigen detected",
                                  "Autopsy specimen - SARS-CoV-2 (virus causing COVID-19) antigen detected")

# connect to Database (db)

db_connect <- function(){
  
  conn_string <- paste0("DRIVER=", r_creds$conn_list_db["DRIVER"], ";",
                        "SERVER=", r_creds$conn_list_db["SERVER"], ";",
                        "DATABASE=", r_creds$conn_list_db["DATABASE"], ";",
                        "UID=", Sys.getenv('USERNAME'), ";",
                        "trusted_connection=yes;",
                        "ApplicationIntent=ReadOnly")
  
  conn <- DBI::dbConnect(odbc::odbc(), 
                         .connection_string = conn_string, 
                         timezone = "America/Los_Angeles", 
                         timezone_out = "America/Los_Angeles")
  return(conn)
}

# Open conn:
db <- db_connect()

qry <- glue::glue_sql("
SELECT DISTINCT CASE_ID 
, CREATE_DATE
, LAST_NAME 
, FIRST_NAME
,  MIDDLE_NAME
,  SEX_AT_BIRTH  
,  BIRTH_DATE  
,  REPORTING_ADDRESS  
,  REPORTING_CITY  
,  REPORTING_STATE 
,  REPORTING_ZIPCODE 
,  REPORTING_COUNTY  
,  PHONE_NUMBER 
,   SUBMITTER  
,   WDRS__RESULT__SUMMARY  
,   ORDFAC__NAME  
,   ORDFAC__STATE  
,   ORDFAC__PHONE  
,   ORDPROV__FNAME  
,   ORDPROV__LNAME  
,   ORDPROV__STATE  
,   ORDPROV__PHONE  
,   WELRS__OF__ID  
,  CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE
,   SPECIMEN__COLLECTION__DTTM
,   OBSERVATION__DTTM
,   ANALYSIS__DTTM
,   RESULT__REPORT__DTTM
,   WELRS__PROCESSED__DTTM
,   INVESTIGATION_CREATE_DATE
FROM DD_GCD_COVID_19_FLATTENED cov 
INNER JOIN DD_ELR_DD_ENTIRE elr on  CASE_ID=  CASE_ID 
INNER JOIN IDS_CASE cs on  CASE_ID = cs.CASE_ID 
LEFT JOIN IDS_PARTY party on  EXTERNAL_ID = party.EXTERNAL_ID
WHERE (  SPECIMEN__COLLECTION__DTTM >= {lookback} OR   SPECIMEN__COLLECTION__DTTM is NULL) --Only include cases from past 19 days
AND   WDRS__RESULT__SUMMARY='Positive' --Aligns with covid_cases R object creation filters for positive cases
AND   WDRS__TEST__PERFORMED IN ({WDRS__TEST__PERFORMED_postive_filter*}) --Aligns with covid_cases R object creation filters for positive cases
AND   WDRS__RESULT IN ({WDRS__RESULT_positive_filter*}) --Aligns with covid_cases R object creation filters for positive cases
AND   INVESTIGATION_CREATE_DATE is not NULL --Aligns with covid_cases R object creation filters for positive cases
AND ( CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE = 'No' OR  CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE IS NULL) --Exclude cases where contact lookup has already been done 
AND cs.status=0 --Only include active cases (exclude deleted and closed cases)
                      ", .con=db)  

# Pull data:
out <- DBI::dbGetQuery(db, qry)

# Close conn:
DBI::dbDisconnect(db)

# Filter out old/frozen case data that were missing   SPECIMEN__COLLECTION__DTTM:
out2 <- out %>% filter(is.na(SPECIMEN__COLLECTION__DTTM))
out1 <- out %>% anti_join(out2, by = names(out))

out2 <- out2 %>% 
  mutate(across(SPECIMEN__COLLECTION__DTTM:RESULT__REPORT__DTTM, as.Date),
         WELRS__PROCESSED__DTTM = as.Date(WELRS__PROCESSED__DTTM, format = '%m/%d/%Y'),
         INVESTIGATION_CREATE_DATE = as.Date(INVESTIGATION_CREATE_DATE, tz = 'America/Los_Angeles')) %>% 
  mutate(SPECIMEN__COLLECTION__DTTM = 
           case_when(!is.na(OBSERVATION__DTTM) ~ OBSERVATION__DTTM,
                     !is.na(ANALYSIS__DTTM) ~ ANALYSIS__DTTM, 
                     !is.na(RESULT__REPORT__DTTM) ~ RESULT__REPORT__DTTM,
                     !is.na(WELRS__PROCESSED__DTTM) ~ WELRS__PROCESSED__DTTM,
                     !is.na(INVESTIGATION_CREATE_DATE) ~ INVESTIGATION_CREATE_DATE,
                     T ~ lubridate::NA_Date_)) %>% 
  filter(!is.na(SPECIMEN__COLLECTION__DTTM) & SPECIMEN__COLLECTION__DTTM >= lookback)


out <- rbind(out1, out2)
rm(out1, out2)

out <- out %>% 
  select(-OBSERVATION__DTTM,
         -ANALYSIS__DTTM,
         -RESULT__REPORT__DTTM,
         -WELRS__PROCESSED__DTTM,
         -INVESTIGATION_CREATE_DATE)

# This is a date-check step. You are expecting to see a create date of today
# If the create date is yesterday or earlier, there is likely an issue with WDRS
# Check in with DIQA team to let them know, and attempt to run the report again after some time
out %>% select(CASE_ID, CREATE_DATE) %>% dplyr::arrange(desc(CREATE_DATE)) %>% .[1,] %>% print()

#dedup by case id and create date:
out <- out %>% distinct(CASE_ID, CREATE_DATE, .keep_all = T)

# Next, we remove invalid phone numbers
#This function converts phone numbers with 10 repeating digits (7 for zeros and nines which are more commonly bad numbers) to NA.
source('filepath.R')


# Format phone numbers and separate into individual phone number columns:
phones <- out %>% 
  mutate(PHONE_NUMBER = as.character(PHONE_NUMBER)) %>% #Ensure PHONE_NUMBER is character class
  mutate(PHONE_NUMBER = iconv(PHONE_NUMBER, from='UTF-8', to='UTF-8', sub='')) %>% #Remove non-UTF-8 characters
  mutate(PHONE_NUMBER = gsub(';', ',', PHONE_NUMBER, fixed = T)) %>% #Convert ';' separators to ','
  #Separate phone numbers into their own columns:
  tidyr::separate(col = PHONE_NUMBER, into = paste0("PHONE_NUMBER_", 1:19), 
                  sep = ",", remove = FALSE, extra = "drop", fill = "right") %>% 
  mutate(across(.cols = PHONE_NUMBER:PHONE_NUMBER_19, .fns = empty_as_na))

# Set column indices for locating new columns
col1 <- match("PHONE_NUMBER_1", colnames(phones))
col19 <- match("PHONE_NUMBER_19", colnames(phones))
if(col19-col1 != 18) stop("PHONE_NUMBER_1 - PHONE_NUMBER_19 are not consecutive columns. Check 'phones' data structure.")

# Remove all non-digit characters and convert blank characters to NA
phones[, col1:col19] <- phones[, col1:col19] %>%  
  purrr::modify(function(x) gsub('\\D+', '', x)) %>% 
  mutate(across(.cols = where(is.character), .fns = empty_as_na))



### Fix phones that may have spaces instead of commas as separators:
spaces <- filter(phones, nchar(PHONE_NUMBER_1) > 19)

phones <- anti_join(phones, spaces, by = colnames(phones))

spaces$PHONE_NUMBER <- stringr::str_replace_all(spaces$PHONE_NUMBER, "\\) ", "\\)") #Remove spaces after end parentheses

spaces <- spaces %>% 
  tidyr::separate(col = PHONE_NUMBER, into = paste0("PHONE_NUMBER_", 1:19), 
                  sep = " ", remove = FALSE, extra = "drop", fill = "right")

spaces[, col1:col19] <- spaces[, col1:col19] %>%  
  purrr::modify(function(x) gsub('\\D+', '', x)) %>% 
  mutate(across(.cols = where(is.character), .fns = empty_as_na)) 

# Keep only phones that were separated that have 10 digits, or NA
# This means phones with no comma separator will be excluded if they have shortened, bad numbers e.g., "(123) 456-7890 206NA"
# but this way, phone numbers formatted like "(123) 456 7890" do not have one number separated into multiple number columns
spaces <- spaces %>% filter(if_all(.cols=PHONE_NUMBER_1:PHONE_NUMBER_19, .fns = function(x) {is.na(x) | nchar(x)==10}))

if(nrow(spaces) > 0) phones <- rbind(phones, spaces)

### End fixing space separated phones



#Remove duplicate numbers:
phones <- phones %>% 
  mutate(across(starts_with("PHONE_NUMBER_"), badPhone)) %>% #Convert bad numbers to NA:
  tidyr::unite("PHONE_NUMBER", paste0("PHONE_NUMBER_", 1:19), na.rm=TRUE, sep = ", ") %>% #Combine separated numbers back into one column:
  mutate(PHONE_NUMBER = empty_as_na(PHONE_NUMBER)) #Convert empty phones to na


# Keep only rows with ONLY bad phone numbers identified (PHONE_NUMBER_NEW == NA):
phones <- phones %>% filter(is.na(PHONE_NUMBER)) %>% distinct()


# This step diverts cases to different datasets
# Some of these datasets are routed to the Data Support Section for further investigation

cols <- colnames(phones)

# Here, we are diverting cases with invalid names:
badnames_phones <- badnames_fun(phones)
phones <- anti_join(phones, badnames_phones, by = cols)

# This list of ordering facilities are contracted with UW, and need to be routed to Data Support
oos_ord_fac_phones <- oos_ord_fac_fun(phones)
phones <- anti_join(phones, oos_ord_fac_phones, by = cols)

phones$WDRS__RESULT__SUMMARY <- NULL
phones$Tag_1 <- 'Missing phone'

# Some phones are pulled in that have blank addresses as well. Even if they don't meet the CICT requirements, we might as well try to do address matching on them anyway
tf <- is.na(phones$REPORTING_ADDRESS)

# Let's ensure any address missing street address has all other address components blank as well:
phones$REPORTING_CITY[tf] <- NA
phones$REPORTING_COUNTY[tf] <- NA
phones$REPORTING_STATE[tf] <- NA
phones$REPORTING_ZIPCODE[tf] <- NA
phones$Tag_1[tf] <- 'Missing demographics' # We need a new tag if we want to track how many phones need address look up



################################################################################################
################     Block 2     ###############################################################
################ Missing Address ###############################################################
################################################################################################


############################################################################
#Loading in R objects and variables
############################################################################

### load in R object and variables
#The ELR variables and others in this list are not yet necessarily in the R object, however Puffins have requested these be listed in the new R object

#loading COVID R Object geocoded_covid_cases
load("filepath.RData")

#Filter cases based on geocoding status and specimen collection date
# geoaccuracy only get's updated on Saturdays, so only include those cases on Monday so we don't keep pulling them in:
if (wday(TODAY) == 2) {
  # On Mondays, include geoaccuracy != "Close" cases
  geocoded_incomplete <- geocoded_covid_cases %>% 
    filter(geomatchstatus == "Not Matched" | geoaccuracy != "Close") %>% # Include only unsuccessfully or imprecisely geocoded addresses
    filter(SPECIMEN__COLLECTION__DTTM >= (lookback)) # Only included cases with spec collection date within past 19 days
} else {
  # On Tuesday - Friday, exclude geoaccuracy != "Close" cases
  geocoded_incomplete <- geocoded_covid_cases %>% 
    filter(geomatchstatus == "Not Matched") %>% # Include only unsuccessfully geocoded addresses
    filter(SPECIMEN__COLLECTION__DTTM >= (lookback)) # Only included cases with spec collection date within past 19 days
}

# Use this to get rid of PO Box addresses that are ONLY like "po box" not "po box 123".
geocoded_incomplete$REPORTING_ADDRESS[!grepl("[0-9]", geocoded_incomplete$REPORTING_ADDRESS)] <- NA

# PO Boxes don't get geocoded so they will still show up this data. Filter them out:
geocoded_incomplete <- geocoded_incomplete %>% filter(!grepl('P[\\. ]{0,2}O[\\. ]{0,2}BOX|P BOX|P. BOX', REPORTING_ADDRESS, ignore.case=T))


# Add testing sites to incompletely geocoded addresses:

# read in list of testing sites to search for:
site <-(read_xlsx((paste0('filepath.xlsx'))))

site<-site %>%
  select(Address)

a<-as.vector(site$Address)

site_string <- paste0(a, collapse = '|')

testing_site <- geocoded_covid_cases %>% 
  filter(SPECIMEN__COLLECTION__DTTM >= lookback) %>% 
  filter(grepl(site_string, gsub('[^[:alpha:][:digit:] ]+', '', REPORTING_ADDRESS), ignore.case = T)) %>%
  filter(F) #remove testing sites for now. 

# Combine testing site and incomplete geocoding addresses:
addresses <- rbind(testing_site, geocoded_incomplete) %>% distinct()

# Select columns we need for mco:                                     
addresses <- addresses %>% 
  select(CASE_ID, 
         CREATE_DATE, 
         LAST_NAME, 
         MIDDLE_NAME,
         FIRST_NAME,
         SEX_AT_BIRTH, 
         BIRTH_DATE=DOB,
         REPORTING_ADDRESS,
         REPORTING_CITY,
         REPORTING_STATE,
         REPORTING_ZIPCODE, 
         REPORTING_COUNTY = geocounty,
         SPECIMEN__COLLECTION__DTTM)

# Open conn:
wdrs <- wdrs_connect()

# Query additional WDRS vars for pulled cases:
qry <- glue::glue_sql(
  "SELECT DISTINCT  CASE_ID
    ,  CREATE_DATE
    ,  LAST_NAME 
    ,  FIRST_NAME
    ,  BIRTH_DATE
    ,   SUBMITTER
    ,   ORDFAC__NAME
    ,   ORDFAC__STATE
    ,   ORDFAC__PHONE
    ,   ORDPROV__FNAME
    ,   ORDPROV__LNAME
    ,   ORDPROV__STATE
    ,   ORDPROV__PHONE
    ,   WELRS__OF__ID
    ,  CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE  
  FROM DD_GCD_COVID_19_FLATTENED cov
    INNER JOIN DD_ELR_DD_ENTIRE elr on  CASE_ID=  CASE_ID
  WHERE   WDRS__RESULT__SUMMARY='Positive'
    AND  status = 0
    AND  CASE_ID in ({CASE_IDs*})", 
  CASE_IDs=addresses$CASE_ID,
  .con=wdrs)

out <- DBI::dbGetQuery(wdrs, qry)

# Close conn:
DBI::dbDisconnect(wdrs)

# Join the extra WDRS vars to our cases:
addresses <- left_join(addresses, out, by=c("CASE_ID", "CREATE_DATE", "LAST_NAME", "FIRST_NAME", "BIRTH_DATE"))

#dedup by case id and create date:
addresses <- addresses %>% distinct(CASE_ID, CREATE_DATE, .keep_all = T)

# Filter out cases where contact lookup was complete:
addresses <- addresses %>% filter(is.na(CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE) | CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE != 'Yes')


# create and save final testing site data, to be referenced in final mco script:
testing_site <- addresses %>%
  filter(CREATE_DATE >= (TODAY-1) & CREATE_DATE <= (TODAY+1)) %>% 
  filter(grepl(site_string, REPORTING_ADDRESS, ignore.case = T))

if (nrow(testing_site) > 0) saveRDS(testing_site, glue('filepath.RDS'))
# end testing site reference creation


# Convert addresses to NA
addresses[, c('REPORTING_ADDRESS', 'REPORTING_CITY', 'REPORTING_STATE', 'REPORTING_ZIPCODE', 'REPORTING_COUNTY')] <- NA



#Add in tag to distinguish address vs phone and placeholder for PHONE_NUMBER:
addresses$Tag_1 <- "Missing address"
addresses$PHONE_NUMBER <- "Address Missing via Geocoding"


# Here, we are diverting cases with invalid names:
cols <- colnames(addresses)

badnames_addresses <- badnames_fun(addresses)
addresses <- anti_join(addresses, badnames_addresses, by = cols)


# This list of ordering facilities are contracted with UW, and need to be routed to Data Support
oos_ord_fac_addresses <- oos_ord_fac_fun(addresses)
addresses <- anti_join(addresses, oos_ord_fac_addresses, by = cols)



################################################################################################
################                   Block 3                ######################################
################   Write XLSX's and create final dataset  ######################################
################################################################################################

to_export <- function(df1, df2, cols) {
  
  df1 <- df1 %>% select(all_of(cols))
  df2 <- df2 %>% select(all_of(cols))
  
  df3 <- rbind(df1, df2)
  
  #dedup by case id for combined phone and addresses
  df3 <- df3 %>% 
    group_by(CASE_ID, CREATE_DATE) %>% 
    filter(row_number()==1) %>%
    ungroup()
  
  return(df3)
}

cols_for_export <- c('CASE_ID', 
                     'CREATE_DATE', 
                     'LAST_NAME', 
                     'FIRST_NAME', 
                     'SEX_AT_BIRTH', 
                     'BIRTH_DATE', 
                     'REPORTING_ADDRESS', 
                     'REPORTING_CITY', 
                     'REPORTING_STATE', 
                     'REPORTING_ZIPCODE', 
                     'REPORTING_COUNTY', 
                     'PHONE_NUMBER', 
                     'SUBMITTER', 
                     'ORDFAC__NAME', 
                     'ORDFAC__STATE', 
                     'ORDFAC__PHONE', 
                     'ORDPROV__FNAME', 
                     'ORDPROV__LNAME', 
                     'ORDPROV__STATE', 
                     'ORDPROV__PHONE', 
                     'WELRS__OF__ID', 
                     'CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE')

badnames <- to_export(badnames_phones, badnames_addresses, cols_for_export)
oos_ord_fac <- to_export(oos_ord_fac_phones, oos_ord_fac_addresses, cols_for_export)

# Write xlsx's for DS:
pacman::p_load(openxlsx)

if (nrow(badnames) > 0) write.xlsx(badnames, glue('filepath.xlsx'), sheetName = 'No name')
if (nrow(oos_ord_fac) > 0) write.xlsx(oos_ord_fac, glue('filepath.xlsx'), sheetName = 'OOS')


###Save final data set:

# Final dups check before combining:
if(length(phones[,c('CASE_ID', 'CREATE_DATE')]) != length(unique(phones[,c('CASE_ID', 'CREATE_DATE')]))) stop('Duplicate CASE_IDs in phones. Dedup before rbind.')
#phones <- phones %>% group_by(CASE_ID, CREATE_DATE) %>% filter(row_number()==1) %>% ungroup()
if(length(addresses[,c('CASE_ID', 'CREATE_DATE')]) != length(unique(addresses[,c('CASE_ID', 'CREATE_DATE')]))) stop('Duplicate CASE_IDs in addresses. Dedup before rbind.')
#addresses <- addresses %>% group_by(CASE_ID, CREATE_DATE) %>% filter(row_number()==1) %>% ungroup()

mco <- rbind(phones, addresses)

# dedup final dataset:
dups <- mco %>% group_by(CASE_ID) %>% filter(row_number()==2) %>% ungroup()

if(nrow(dups != 0)) {
  mco <- anti_join(mco, dups, by=c('CASE_ID', 'CREATE_DATE')) #remove all rows in mco that contain the duped case id and create dates
  dups[, c('PHONE_NUMBER', 'REPORTING_ADDRESS', 'REPORTING_CITY', 'REPORTING_STATE', 'REPORTING_ZIPCODE', 'REPORTING_COUNTY')] <- NA #convert all phone and address vars to NA
  dups$Tag_1 <- "Missing demographics" #Change tag so that we know both phone & address are missing
  mco <- rbind(mco, dups)
}



################################################################################################
################               Block 4                    ######################################
################     Restrict cases by CICT Criteria      ######################################
################################################################################################

# This chunk never runs unless the age.restriction object (created at the beginning of the script) is set to TRUE.
# It is intended to be used if there are more cases than capacity to do manual look up on all the cases.
# If the number of cases sent for look up needs to be reduced, this block filters cases by CICT age criteria (as of 02/06/2023):


if (age.restriction) {
  
  mco <- mco %>% mutate(age = floor(lubridate::decimal_date(SPECIMEN__COLLECTION__DTTM) - lubridate::decimal_date(BIRTH_DATE)))
  mco <- mco %>% filter(age <= 22 | age >= 50)
  mco$age <- NULL #drop age var now as we don't need it going forward
  
}

mco$SPECIMEN__COLLECTION__DTTM <- NULL




################################################################################################
################                     Block 5                     ###############################
################     Save data and send email to Data Support    ###############################
################################################################################################


# save data for cross matching today:
saveRDS(mco, file = glue('filepath.RDS'))

# Make sure data were saved, and send DS an email if they were:
if(!file.exists(glue('filepath.RDS'))) {
  stop('Daily cross-matching data set failed to save.')
} else {
  # send data support an email:
  
  #Generating email:
  
  # from
  f <- '<email@email.com>'
  
  # reply-to
  rl <- c('<email@email.com>', '<email@email.com>')
  
  
  # cc
  cl <- '<email@email.com>'
  
  # subject
  subj <- glue('{format(TODAY, "%m/%d/%Y")} Missing Address')
  
  # body
  bdy <- glue::glue(
    '
Good morning!

Today we have:
    0 Bad Names
    0 Out of State Ordering Facilities
Spreadsheets are located here: filepath

{nrow(mco)} events will go through the IIS & RHINO matching process. Results will be sent to you around noon.


Thank you,
DQ Epis
')
  
  
  #Sending email:
  sendmailR::sendmail(f, rl, subj, bdy, cl, headers= list('Reply-To' = '<email@email.com>'), control= list(smtpServer = 'serverinfo'))
  
}  




################################################################################################
################             Block 6               #############################################
################   Write file for RHINO matching   #############################################
################################################################################################


rhino <- mco %>% select(CASE_ID, FIRST_NAME, LAST_NAME, SEX_AT_BIRTH, BIRTH_DATE, REPORTING_ADDRESS, REPORTING_CITY, 
                        REPORTING_STATE, REPORTING_ZIPCODE, REPORTING_COUNTY, PHONE_NUMBER, ORDFAC__NAME, WELRS__OF__ID, 
                        Tag_1)


# *Recoding variables;
rhino <- rhino %>% 
  mutate(
    Event_ID = CASE_ID,
    External_ID = CASE_ID,
    LastName = LAST_NAME,
    Middle_Name = NA_character_,
    FirstName = FIRST_NAME,
    Gender = SEX_AT_BIRTH,
    BirthDate = format(BIRTH_DATE, '%m-%d-%Y'),
    Street1 = NA_character_,
    City = NA_character_,
    State = NA_character_,
    PostalCode = NA_character_,
    County = NA_character_,
    Ordering_Facility = ORDFAC__NAME,
    Ordering_Facility_ID = WELRS__OF__ID)

# Convert blank to NA:
rhino <- rhino %>% mutate(across(.col = where(is.character), .fns = empty_as_na))


### Create final dataset for RHINO xmatching ###

# *Restrict variables and recorder;
rhino_final <- rhino %>% select(Event_ID, External_ID, LastName, Middle_Name, FirstName, Gender, BirthDate, Street1, City, 
                                State, PostalCode, County, PHONE_NUMBER, Ordering_Facility, Ordering_Facility_ID, Tag_1)

rhino_final <- rhino_final %>% rename('Event ID' = Event_ID,
                                      'External ID' = External_ID,
                                      'Last Name' = LastName,
                                      'Middle Name' = Middle_Name,
                                      'First Name' = FirstName,
                                      'Phone' = PHONE_NUMBER,
                                      'Ordering Facility' = Ordering_Facility,
                                      'Ordering Facility ID' = Ordering_Facility_ID,
                                      'Tag 1' = Tag_1)


# 6/10/2021 modified by chunyi, This is the file path for RHINO team to pull the file for matching
write.csv(rhino_final, 
          glue('filepath.csv'),
          row.names = F,
          na = '')


