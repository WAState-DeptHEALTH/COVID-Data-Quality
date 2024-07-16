#################################################################################################
#  GEOCODED COVID CASES DATASET CREATION
#  COVID-19 R OBJECT DATASET CREATION AND CLEANING PART 5
################################################################################################
################################################################################################
#
#  This R Markdown script loads in R packages, brings together previously run scripts in this process to join and filter data from various sources,
#  and submits addresses to be geocoded to validate them, in order to ultimately create clean, static COVID-19 datasets in the form of .Rdata files, referred to 
#  as R Objects. The R Objects are created as an alternative for querying the live (and therefore, more complex and messy) Washington COVID-19 database. 
#  
#
#  **IMPORTANT ACRONYMS/PHRASES USED:**
#     -WDRS: Washington Disease Reporting System (the primary database for COVID-19 disease reporting information)
#     -db: database
#     -df: dataframe; a way to structure data within the base functions of R
#     -R Object: a set of .RData files with cleaned COVID-19 data to use as a more reliable/static dataset to pull data from compared to the live databse WDRS 
#     -spectable: a table looking at specimen collection specifically
#     -geocoding: geographical coordinates that correspond to an address, further validating addresses for cases for mapping purposes
#
#
#  This script is helpful to external DOH partners as it demonstrates how to create a standardized dataset for all cases to use as an alternative to querying a live database.
#  
################################################################################################
################################################################################################



# GEOCODED CASES ----------------------------------------------------------

#Last updated: April 17, 2023

# PURPOSE -----------------------------------------------------------------

# The purpose of this script is to: 
# 1. identify covid-19 cases (confirmed and probable) address to geocode
# 2. clean and scrubbed reporting and primary addreses (fields end in "_c")
# 3. send address (as urls) to acgeo webservices for geocoding, address correction and geoprocessing based on hierarchy: 
# reporting addresses then if reporting address is not matched with 'close' accuracy attempt primary address for a close accuracy match for each CASE_ID
# 4. perform spatial join to get census tract variable
# 5. combine old and newly matched results (including those wi
# 3. output R object with geocoded reporting address and primary and combined covid_cases data set with good geocoded address.
# 4. email out when update is ready for the geocoded_covid R object. 



# CONTRIBUTION ------------------------------------------------------------

# Rasha Elnimeiry - Special Projects & Data Requests Team 
# Jon Downs - Center for Health Statistics
# Ian Painter - Office of Science, Health and Informatics
# Lilli Manahan - Special Projects & Data Requests Team
# Caitlin Oleson - Surveillance Process Team
# Tessa Fairfortune - Data Integration Quality Assurance Team
# Craig Erickson - HTS/ Office of Innovation


'-------------------------------------------------------------------------------------------------------------'
'-------------------------------------------------------------------------------------------------------------'
'-------------------------------------------------------------------------------------------------------------'

# LIBRARIES ---------------------------------------------------------------
Sys.time()
Sys.info()

suppressMessages(library(tidyverse))  
library(tictoc) %>% suppressMessages() %>% suppressWarnings()
library(stringi) %>% suppressMessages() %>% suppressWarnings()
library(httr) %>% suppressMessages() %>% suppressWarnings()
library(XML) %>% suppressMessages() %>% suppressWarnings()
library(readr) %>% suppressWarnings()%>% suppressWarnings()
library(rjson) %>% suppressMessages() %>% suppressWarnings()
library(raster) %>% suppressMessages() %>% suppressWarnings()
library(sp) %>% suppressMessages() %>% suppressWarnings()
library(lubridate) %>% suppressMessages() %>% suppressWarnings()
library(stringr)  %>% suppressMessages() %>% suppressWarnings()
library(rio)  %>% suppressMessages() %>% suppressWarnings()
library(formattable)  %>% suppressMessages() %>% suppressWarnings()
library(sjmisc) %>% suppressMessages() %>% suppressWarnings()
library(htmltools) %>% suppressMessages() %>% suppressWarnings()
tic("timeelapsedfull")
tic("total time to complete to email")



source("filepath/geocodingfunctions.R")

# DATA: CASES -------------------------------------------------------------
tic("load covid_cases + geocoded_covid_cases")

# if covid_cases is not available (being updated), will find the most recent
# snapshot copy of covid_cases and load it. message prints out to notify which
# covid_cases R object (current copy or snaptop and when that was last updated)
today <- lubridate::today()
today_f <- format(today,"%A, %B %d, %Y")
weekdays(Sys.Date())->today_w 

if (today_w == "Saturday"){
  
  if (file.exists(
    glue::glue(
      "filepath/covid_cases.Rdata"))){
    message = glue::glue(
      "{today_f} run - covid_cases R object used was last updated on: {file.info('filepath/covid_cases.Rdata') %>% dplyr::select(mtime) %>%  pull}")
    load("filepath/covid_cases.Rdata",  verbose = T)
    print(message)
  } else if (!file.exists(
    glue::glue("filepath/covid_cases.Rdata"))) {
    message = glue::glue(
      "{today_f} run - Potential issue with loading of covid_cases R object today. A previous snapshot of covid_cases was used instead:
   {list.files('filepath/',full.names = T) %>% enframe(name = NULL) %>% bind_cols(pmap_df(., file.info)) %>% filter(mtime==max(mtime)) %>% pull(value )} \n Review geocoded_covid_cases log and script.")
    load(glue::glue("{list.files('filepath/',full.names = T) %>% enframe(name = NULL) %>% bind_cols(pmap_df(., file.info)) %>% filter(mtime==max(mtime)) %>% pull(value )}/covid_cases.RData"),verbose = T)
    print(message)
  }
  
}
# load current copy of geocoded_covid_cases R object
load("filepath/geocoded_covid_cases.RData")


toc()



# 1.  IDENTIFY CASES TO ATTEMPT GEOCODING/ADDRESS CORRECTING/GEOPROCESSING -------------------------------------

tic("identify cases to attempt geocoding")

geocoded_covid_cases %>% 
  dplyr::select(CASE_ID, geomatchstatus, geoaccuracy) %>% 
  filter(geomatchstatus=="Not Matched" | geoaccuracy != "Close") ->geocodednotmatched


#antijoin gives us cases in covid_cases that have different addresses than in geocoded_covid_cases from yesterday and new cases in covid_cases
anti_join(
  covid_cases %>%
    dplyr::select(CASE_ID, REPORTING_ADDRESS, REPORTING_CITY, REPORTING_STATE, REPORTING_ZIPCODE, ADDRESS_LINE1, CITY, STATE, POSTAL_CODE, CREATE_DATE, MODIFICATION_DATE),
  geocoded_covid_cases,
  by = c('CASE_ID'='CASE_ID', 'REPORTING_ADDRESS'='REPORTING_ADDRESS_c', 'REPORTING_CITY'='REPORTING_CITY_c',
         'REPORTING_STATE'='REPORTING_STATE_c', 'REPORTING_ZIPCODE' = 'REPORTING_ZIPCODE_c', 'ADDRESS_LINE1' = 'ADDRESS_LINE1_c',
         'CITY' = 'CITY_c', 'STATE' = 'STATE_c', 'POSTAL_CODE' = 'POSTAL_CODE_c')) %>%
  
  #attach various geocoding variables
  left_join(geocoded_covid_cases %>%  dplyr::select(CASE_ID, REPORTING_ADDRESS_c, REPORTING_CITY_c, REPORTING_STATE_c, 
                                                    REPORTING_ZIPCODE_c, ADDRESS_LINE1_c, CITY_c, STATE_c, POSTAL_CODE_c, 
                                                    contains("geo"))) %>% 
  #filter to not-matched cases
  dplyr::filter(geomatchstatus == "Not Matched" | is.na(geomatchstatus)) %>% 
  
  #sort just for funsies
  arrange(desc(ymd(as.Date(CREATE_DATE))))->antijoined_cc


# based on day of the week, certain cases are attempted to go through acgeowebservices. 
# if today is M-F, pull CASE_IDs that are newly added in covid_cases (new CASE_IDs) &
# pull CASE_IDs that are different (or modified) between geocoded_covid_cases and covid_cases currently
# and these CASE_IDs will go through next process.
# if today is Sa, pull CASE_IDs that are newly added in covid_cases (new CASE_IDs) & 
# pull CASE_IDs that have been attempted to geocoded previously and have not been matched (weekend clean up).


if (today_w %in% c('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Sunday')) {
  covid_cases  %>%
    mutate(newcasestogeocode = case_when(
      (!(CASE_ID %in% geocoded_covid_cases$CASE_ID))           ~ "Need to geocode" ,
      (CASE_ID %in% antijoined_cc$CASE_ID)                     ~ "Need to geocode" , 
      TRUE ~ "No need to geocode")) %>%
    filter(newcasestogeocode=="Need to geocode") ->casestoattemptgeocoding
  glue::glue("Today is {today_w} - no attempt to do a clean up of all CASE_IDs with 'geomatchstatus'=='Not Matched' today (will attempt clean up on following Saturday).")
  
  
  
} else if (today_w == 'Saturday') {
  covid_cases  %>%
    mutate(newcasestogeocode = case_when(
      (!(CASE_ID %in% geocoded_covid_cases$CASE_ID))           ~ "Need to geocode" ,
      (CASE_ID %in% geocodednotmatched$CASE_ID)              ~ "Need to geocode" ,
      # (CASE_ID %in% recentmodification$CASE_ID_GCD)          ~ "Need to geocode"  ,  # tessa sql query logic here
      TRUE ~ "No need to geocode")) %>%
    filter(newcasestogeocode=="Need to geocode")    ->casestoattemptgeocoding
  glue::glue("Today is {today_w} - an attempt to do a clean up of all CASE_IDs with 'geomatchstatus'=='Not Matched' today is running - {Sys.time()}.")
  
}


glue::glue("cumulative number of cases: {covid_cases %>% nrow %>% formattable::comma(0)}")
glue::glue("number of cases to attempt geocoding in this run: {casestoattemptgeocoding %>% nrow %>% formattable::comma(0)}")
toc()


# > DELAY EMAIL TRIGGER: if casestoattemptgeocoding==0 ----------------------
if(casestoattemptgeocoding %>% nrow==0){
  today <- lubridate::today()
  time_n <- format(Sys.time(), format = "%H:%M")
  today_f <- format(today,"%A, %B %d, %Y")
  today_d <- format(lubridate::today() , "%Y-%m-%d")
  
  special_message <- "We ask for your patience, as we are looking in to the quality for today's geocoded_covid_cases R object. We expect to update geocoded_covid_cases R object before 3 pm."
  
  teamsignature <- glue::glue(
    "
 _

Data Integration and Quality Assurance (DIQA) Team
Public Health Outbreak Coordination, Informatics, Surveillance (PHOCIS) Office - Surveillance Section
Division of Disease Control and Health Statistics
Washington State Department of Health

 _
")
  
  autoemailmessage <- glue::glue("***This is an auto-generated email notifying you of delays to gecocoded_covid_cases R Object update for {today_f}. As soon as geocoded_covid_cases R object update becomes available, you will promptly receive an email. 
                             If there are any problems moving forward, please contact the DIQA team.***")
  # outpath <-  "filepath/geocoded_covid_cases.RData"
  # from
  f <-  sprintf("<your.email@email.com>")
  # reply-to
  readxl::read_excel("/filepath.xlsx",trim_ws = T)  %>%
    mutate(email_address = textclean::strip(email_address, char.keep = c( "@", ".", "-"), digit.remove = F, apostrophe.remove = T, lower.case = F),
           email_address = glue::glue("<{email_address}>"))  ->emaildistr
  rl <- sprintf(c(emaildistr$email_address))
  
  # subject
  subj <- glue::glue("***Delay Notice for {today_d} geocoded_covid_cases R Object***")
  # today_f <- format(today, "%Y_%m_%d")
  
  # body
  bdy <- glue::glue(
    "Dear Colleague(s),


Delay Notice for {today_f}:

{special_message}



{autoemailmessage}

Thank you and have a great day.


{teamsignature}
")
  
  sendmailR::sendmail(f, rl, subj, bdy,
                      # cl,
                      # bl,
                      headers= list("Reply-To"="<your.email@email.com>"), control= list(smtpServer= "relay"))
  glue::glue('delay trigger email sent')
  
  stop("no cases to attempt geocoding - end script")
  
  # >> ELSE: if casestoattemptgeocoding>0 keep going ---------------------------
} else{
  
  glue::glue("cases to attempt geocoding: {casestoattemptgeocoding %>% nrow %>% formattable::comma(0)}")
  
  
  # 2. ADDRESS CLEAN UP --------------------------------------------------------
  tic("address clean up")
  # Reporting address, city, zip and state fields and primary address, city,
  # zip, and state fields (fields can be messy and unpredictable -encoded
  # characters, text notes, random numbers, address in city field, etc.) using
  # string searches and replacements and custom developed functions. Cleaned
  # address fields have a suffix "_c".
  
  casestoattemptgeocoding %>%
    mutate(REPORTING_ADDRESS_c = as.character(REPORTING_ADDRESS)) %>% 
    mutate(REPORTING_ADDRESS_c = stringr::str_replace_all(REPORTING_ADDRESS_c, pattern = "[^\x20-\x7E]", " ")) %>%
    mutate(REPORTING_ADDRESS_c = textclean::replace_non_ascii(REPORTING_ADDRESS_c)) %>%  
    mutate(REPORTING_ADDRESS_c = textclean::strip(REPORTING_ADDRESS_c, char.keep = c("#", "-"), digit.remove = F, apostrophe.remove = T, lower.case = F)) %>% 
    # change this to case when reporting address after space and punct remove contains PO BOX or PBox to catch majority (!!)
    mutate(REPORTING_ADDRESS_c = case_when(REPORTING_ADDRESS_c %like% "^PO" | REPORTING_ADDRESS_c %like% "^Po" | REPORTING_ADDRESS_c %like% "^po"|  REPORTING_ADDRESS_c %like% "^P.O." | REPORTING_ADDRESS_c %like% "^P.o." | REPORTING_ADDRESS_c %like% "^p.o." | REPORTING_ADDRESS_c %like% "^P box" |REPORTING_ADDRESS_c %like% "^P. O"  ~ NA_character_,   TRUE ~REPORTING_ADDRESS_c)) %>%
    mutate(REPORTING_ADDRESS_c = case_when(nchar(REPORTING_ADDRESS_c)<=7  ~ NA_character_,   TRUE ~REPORTING_ADDRESS_c)) %>%  # when # of chars are less than 7
    # contains "homeless, unknown", ;update; when includes these terms and nchar is more than 8; then gsub out update term and keep the rest of field
    mutate(REPORTING_ADDRESS_c = case_when(
      letters_only(REPORTING_ADDRESS_c)==T ~ NA_character_,
      numbers_only(REPORTING_ADDRESS_c)==T ~ NA_character_,
      # punct_only(REPORTING_ADDRESS_c)==T   ~ NA_character_,
      containsstreetsuffix(REPORTING_ADDRESS_c)==F &
        containsstreetsuffixremove(REPORTING_ADDRESS_c)==T  ~ NA_character_,
      TRUE ~REPORTING_ADDRESS_c)) %>%
    mutate(ADDRESS_LINE1_c = stringr::str_replace_all(ADDRESS_LINE1, pattern = "[^\x20-\x7E]", " ")) %>%
    mutate(ADDRESS_LINE1_c = textclean::replace_non_ascii(ADDRESS_LINE1_c)) %>%
    mutate(ADDRESS_LINE1_c = case_when(ADDRESS_LINE1_c  %like% "^PO" | ADDRESS_LINE1_c  %like% "^Po" | ADDRESS_LINE1_c  %like% "^po"|  ADDRESS_LINE1_c  %like% "^P.O." | ADDRESS_LINE1_c  %like% "^P.o." | ADDRESS_LINE1_c  %like% "^p.o."~ NA_character_,   TRUE ~ADDRESS_LINE1_c )) %>%
    mutate(ADDRESS_LINE1_c = gsub(x = ADDRESS_LINE1_c, pattern= "^0",  replacement = "" )) %>%  #begin with "0"
    mutate(ADDRESS_LINE1_c = case_when(nchar(ADDRESS_LINE1_c)<=4  ~ NA_character_,   TRUE ~ADDRESS_LINE1_c)) %>%  # when # of chars are less than 4
    mutate(ADDRESS_LINE1_c = case_when(
      letters_only(ADDRESS_LINE1_c)==T ~ NA_character_,
      numbers_only(ADDRESS_LINE1_c)==T ~ NA_character_,
      # punct_only(ADDRESS_LINE1_c)==T  ~ NA_character_,
      containsstreetsuffix(ADDRESS_LINE1_c)==F & containsstreetsuffixremove(ADDRESS_LINE1_c)==T  ~ NA_character_, 
      TRUE ~ADDRESS_LINE1_c)) %>% 
    
    # > clean reporting primary city fields -------------------------------------
  # clean reporting city
  # reporting city - clean up all numbers, punction
  mutate(REPORTING_CITY_c = as.character(REPORTING_CITY)) %>%
    mutate(REPORTING_CITY_c = stringr::str_replace_all(REPORTING_CITY_c, pattern = "[^\x20-\x7E]", " ")) %>%
    mutate(REPORTING_CITY_c = textclean::replace_non_ascii(REPORTING_CITY_c)) %>%
    mutate(
      # remove if numbers only or punct only
      REPORTING_CITY_c = case_when(
        numbers_only(REPORTING_CITY_c)==T ~ NA_character_,
        # punct_only(REPORTING_CITY_c)==T  ~ NA_character_, 
        TRUE ~REPORTING_CITY_c)) %>%
    mutate(REPORTING_CITY_c = gsub("[[:punct:]]", "", REPORTING_CITY_c,perl = T))  %>%
    
    # primary city - clean up all numbers, punction
    mutate(CITY_c = as.character(CITY)) %>%
    mutate(CITY_c = stringr::str_replace_all(CITY_c, pattern = "[^\x20-\x7E]", " ")) %>%
    mutate(CITY_c = textclean::replace_non_ascii(CITY_c)) %>%
    mutate(
      # remove if numbers only or punct only
      CITY_c = case_when(
        numbers_only(CITY_c)==T ~ NA_character_,
        # punct_only(CITY_c)==T  ~ NA_character_, 
        TRUE ~CITY_c)) %>%
    mutate(CITY_c = gsub("[[:punct:]]", "", CITY_c,perl = T))  %>% 
    
    # > adjust reporting address field ------------------------------------------
  # if reporting address is empty and reporting city contains numbers and letters; fill in reporting address with reporting city
  mutate(REPORTING_ADDRESS_c = case_when(
    is.na(REPORTING_ADDRESS_c)==T & (numbers_only(REPORTING_CITY_c)==F & letters_only(REPORTING_CITY_c)==F) ~ REPORTING_CITY_c,
    nchar(REPORTING_ADDRESS_c) <=5 ~NA_character_,  
    numbers_only(REPORTING_ADDRESS_c)==T ~ NA_character_, 
    TRUE ~ REPORTING_ADDRESS_c)) %>%  
    # mutate(REPORTING_ADDRESS_c = case_when(nchar(REPORTING_ADDRESS_c)<=7  ~ NA_character_,   TRUE ~REPORTING_ADDRESS_c)) %>%  # when # of chars are less than 7
    mutate(REPORTING_ADDRESS_c = case_when(
      letters_only(REPORTING_ADDRESS_c)==T ~ NA_character_,
      numbers_only(REPORTING_ADDRESS_c)==T ~ NA_character_,
      # punct_only(REPORTING_ADDRESS_c)==T  ~ NA_character_,
      TRUE ~REPORTING_ADDRESS_c)) %>%
    # NA if reportingcity does not contain only letters or =/less than 2 character
    mutate(REPORTING_CITY_c = case_when(
      letters_only(REPORTING_CITY_c)==F ~NA_character_,
      nchar(REPORTING_CITY_c)<=2 ~NA_character_,
      TRUE ~ REPORTING_CITY_c)) %>% 
    # dplyr::select(REPORTING_ADDRESS, REPORTING_ADDRESS_c, ADDRESS_LINE1, ADDRESS_LINE1_c, REPORTING_CITY, REPORTING_CITY_c, CITY, CITY_c, REPORTING_ZIPCODE ) %>% view
    # > clean reporting and primary zip code fields ------------------------------
  
  # reporting zip - clean up
  # extract only numbers for zip
  mutate(REPORTING_ZIPCODE_c = stringr::str_replace_all(REPORTING_ZIPCODE, pattern = "[^\x20-\x7E]", " ")) %>%
    mutate(REPORTING_ZIPCODE_c = textclean::replace_non_ascii(REPORTING_ZIPCODE_c)) %>% 
    mutate(REPORTING_ZIPCODE_c = gsub("[[:punct:]]", "", REPORTING_ZIPCODE_c, perl = T))  %>% 
    mutate(REPORTING_ZIPCODE_c = case_when(
      letters_only(REPORTING_ZIPCODE_c)==T ~ NA_character_,
      # punct_only(REPORTING_ZIPCODE_c)==T  ~ NA_character_,
      TRUE ~REPORTING_ZIPCODE_c)) %>%
    mutate(REPORTING_ZIPCODE_c = readr::parse_number(REPORTING_ZIPCODE_c )) %>% 
    # get first 1-5 characters for zipcodes have #####- #### formatting
    mutate(REPORTING_ZIPCODE_c = str_sub(REPORTING_ZIPCODE_c, start = 1, end = 5)) %>% 
    # replace with NA if less than 5 chars
    mutate(REPORTING_ZIPCODE_c = ifelse(test = nchar(REPORTING_ZIPCODE_c) < 5, yes= NA_character_, no =REPORTING_ZIPCODE_c)) %>% 
    mutate(
      REPORTING_STATE_c= REPORTING_STATE,
      STATE_c = STATE) %>% 
    # primary zip - clean up
    # extract only numbers for zip
    mutate(POSTAL_CODE_c = stringr::str_replace_all(POSTAL_CODE, pattern = "[^\x20-\x7E]", " ")) %>%
    mutate(POSTAL_CODE_c = textclean::replace_non_ascii(POSTAL_CODE_c)) %>%
    # mutate(POSTAL_CODE = textclean::strip(POSTAL_CODE, digit.remove = F, apostrophe.remove = T, lower.case = F)) %>% 
    mutate(POSTAL_CODE_c = gsub("[[:punct:]]", "", POSTAL_CODE_c, perl = T))  %>%
    mutate(POSTAL_CODE_c = case_when(
      letters_only(POSTAL_CODE_c)==T ~ NA_character_,
      # punct_only(POSTAL_CODE_c)==T  ~ NA_character_,
      TRUE ~POSTAL_CODE_c)) %>%
    mutate(POSTAL_CODE_c = readr::parse_number(POSTAL_CODE_c)) %>% 
    # get first 1-5 characters for zipcodes have #####- #### formatting
    mutate(POSTAL_CODE_c = str_sub(POSTAL_CODE_c, start = 1, end = 5)) %>%  
    # replace with NA if less than 5 chars
    mutate(POSTAL_CODE_c = ifelse(test = nchar(POSTAL_CODE_c) < 5, yes= NA_character_, no =POSTAL_CODE_c))   -> cases
  
  toc()
  
  
  # 3. ADDRESS PREP: ADDRESSES AS URL, CASES AS LIST ------------------------
  
  
  tic("set up cases list rx")
  # NOTES: geocoding reporting address, later we use the primary address if a reporting address is not available. 
  # geocoding uses Tiger10 address locator, a WA DOH internal geocoder service
  # takes an address and returns long and lat and other measures of this conversion (i.e. accuracy of geocoding).
  
  # Case addresses (reporting and primary are prepped a URL string and put in
  # list format; then reporting addresses and primary addresses are prepped for
  # sending to ACGEO web service in the pathway outlined next.
  
  
  cases %>%
    as.data.frame %>%
    dplyr::select(CASE_ID, REPORTING_ADDRESS_c, ADDRESS_LINE1_c, REPORTING_CITY_c, CITY_c, REPORTING_STATE_c, STATE_c, REPORTING_ZIPCODE_c, POSTAL_CODE_c) %>%
    mutate(
      rCASE_ID = CASE_ID,
      reportingstr             = glue::glue("{URL_BASE}Getgeocodedaddress?{addscrubber(paste0('address=', REPORTING_ADDRESS_c))}{addscrubber(paste0('&city=', REPORTING_CITY_c))}{addscrubber(paste0('&state=', REPORTING_STATE_c))}{addscrubber(paste0('&zip=', REPORTING_ZIPCODE_c))}&zip4=&company=&Consumer=DOH1!&process=3"),
      primarystr               = glue::glue("{URL_BASE}Getgeocodedaddress?{addscrubber(paste0('address=', ADDRESS_LINE1_c))}{addscrubber(paste0('&city=', CITY_c))}{addscrubber(paste0('&state=', STATE_c))}{addscrubber(paste0('&zip=', POSTAL_CODE_c))}&zip4=&company=&Consumer=DOH1!&process=3"),
    ) -> cases_list_unnested
  
  
  cases %>% 
    # tail(1000) %>%
    as.data.frame %>%
    dplyr::select(CASE_ID, REPORTING_ADDRESS_c, ADDRESS_LINE1_c, REPORTING_CITY_c, CITY_c, REPORTING_STATE_c, STATE_c, REPORTING_ZIPCODE_c, POSTAL_CODE_c) %>%
    mutate(
      rCASE_ID = CASE_ID,
      reportingstr             = glue::glue("{URL_BASE}Getgeocodedaddress?{addscrubber(paste0('address=', REPORTING_ADDRESS_c))}{addscrubber(paste0('&city=', REPORTING_CITY_c))}{addscrubber(paste0('&state=', REPORTING_STATE_c))}{addscrubber(paste0('&zip=', REPORTING_ZIPCODE_c))}&zip4=&company=&Consumer=DOH1!&process=3"),
      primarystr               = glue::glue("{URL_BASE}Getgeocodedaddress?{addscrubber(paste0('address=', ADDRESS_LINE1_c))}{addscrubber(paste0('&city=', CITY_c))}{addscrubber(paste0('&state=', STATE_c))}{addscrubber(paste0('&zip=', POSTAL_CODE_c))}&zip4=&company=&Consumer=DOH1!&process=3"),
    ) %>% 
    group_by(CASE_ID,.drop=F) %>%
    tidyr::nest(.names_sep = "CASE_ID")-> rx
  
  
  toc()
  
  
  
  
  
  # 4. SENDING FOR ACGEO WEBSERVICES -------------------------------------------
  
  # (!!) running in parallel -----------------------------------------------------
  tic("parallel and memory set up")
  memincr <- memory.limit()*100000
  memory.limit(memincr)
  # up the limit to 1GB, this in bytes is 1014*1024^2 
  # options('future.globals.maxSize' = memory.limit(x))
  
  # create clusters; dedicating 50 workers; for 72 cores
  require(furrr) %>% suppressMessages() %>% suppressWarnings()
  options('future.globals.maxSize' = (1014*1024^2)*7)
  
  future::plan(list(future::tweak(future::multisession, workers = 5)))
  future::nbrOfWorkers() %>% as.numeric->nw
  glue::glue("number of workers: {nw}")
  toc()
  
  
  # > REPORTING ADDRESSES FIRST ---------------------------------------------
  # 1.	Reporting Addresses First:
  # (a)	prepped as a URL string and cases are put as a list (CASE_ID and URL address string)
  # (b)	URL address string sent webservice for geocoding, address correction, and geoprocessing (safe handling of errors and using parallel processing occurs here)
  # (c)	Returned results come back in XML
  # (d)	XML is parsed and converted to list (geoprocessed list)
  
  
  Sys.time()
  tic(glue::glue("parallel process time elapsed to geocode {length(rx[['data']])} reporting addresses using {nw} workers"))
  glue::glue("approx. number of chunks divided amongst {nw} workers: {plyr::round_any(length(rx[['data']])/nw, 100, f = ceiling)}")
  glue::glue("exact number of chunks divided amongst {nw} workers: {length(rx[['data']])/nw}")
  
  
  plyr::round_any(length(rx[['data']])/nw, 100, f = ceiling)->chnksize
  # safely handle iteration
  # saferhandling = safely( .f = ~ .x %>% httr::GET(.) %>% content(.,as =  "text")  %>% xmlParse() %>% xmlToList()
  #                  , otherwise = "errorrrrr")
  saferhandling = possibly( .f = ~ .x %>% httr::GET(.) %>% content(.,as =  "text")  %>% xmlParse() %>% xmlToList()
                            , otherwise = "errorrrrr")
  
  furrr::future_map(.x = rx$data , .f =~saferhandling( .x[['reportingstr']]),
                    .options = furrr_options(chunk_size =  chnksize )) ->ry
  
  toc() ->timeelapsedgeocodereportingadd
  lubridate::duration(round(timeelapsedgeocodereportingadd$toc-timeelapsedgeocodereportingadd$tic, 2), units ="seconds") ->timeelapsedgeocodereportingadd
  
  
  
  
  
  
  
  
  
  # >  map/parse reporting add to df -----------------------------------------
  tic("map/parse reporting address to df")
  # (e)	Cases list is joined/iterated over geoprocessed list -> output as
  # dataframe at case level with geocoded, address corrected, and geoprocessed
  # fields (error handling occurs here too).
  
  plyr::round_any(length(rx[['data']])/nw, 100, f = ceiling)->chnksize
  
  
  Sys.time()
  furrr::future_map2_dfr(.x = rx$data , 
                         .y = ry , 
                         .f = geoappending,
                         .options = furrr_options(chunk_size = chnksize )
                         # .options = furrr_options(chunk_size =  as.integer(cases %>% nrow/nw ))
  ) %>% 
    mutate(georeportingprimaryaddmatch = ifelse(geomatchstatus =="Matched", "Reporting", NA))->cases_geocodedreportadd
  
  
  glue::glue(
    "
--------------------
# of reporting addresses sent for geoprocessing: {cases_geocodedreportadd %>% nrow}
# of reporting addresses geocoded with close accuracy: {cases_geocodedreportadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow}
% of reporting addresses successfully geocoded/matched with close accuracy: {round(cases_geocodedreportadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ cases_geocodedreportadd %>% nrow,1)}%
% of reporting addresses of all cases successfully geocoded/matched with close accuracy: {round(cases_geocodedreportadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ length(rx[['data']]), 1)}%
-------------------      
"
  )
  toc()->timeelapsedparsingreportingadd
  lubridate::duration(round(timeelapsedparsingreportingadd$toc-timeelapsedparsingreportingadd$tic, 2), units ="seconds") ->timeelapsedparsingreportingadd
  
  # beepr::beep(2)
  
  # x %>% head
  rm(rx,ry)
  
  
  
  
  
  
  # > if successfull save R Object with reporting addresses here --------------
  #tic("save reporting addresses R object")
  # (f)	If parsing list to dataframe is successful, save an intermediate R object here.
  

  
  
  
  # 5. LOGIC: If reporting add not close accuracy, attempt primary add ---------
  
  
  # > select matched cases with long lat + close geocoding accuracy -----------------
  tic("select cases with long lat + close geocoding accuracy - reporting add")
  # (g)	pull out CASE_ID's with reporting addresses that were matched & matched
  # with close accuracy (keep CASE_IDs that were matched but with less than
  # close accuracy - we are keeping these for later because they have address
  # corrected fields)
  
  cases_geocodedreportadd %>% 
    dplyr::rename(CASE_ID = rCASE_ID ) %>% 
    group_by(CASE_ID, .drop=F) %>% 
    mutate(
      geolongitude = case_when(numbers_only(geolongitude)==T ~ geolongitude, 
                               letters_only(geolongitude)==T ~ NA_character_, 
                               TRUE ~ geolongitude), 
      geolatitude  = case_when(numbers_only(geolatitude)==T ~ geolatitude, 
                               letters_only(geolatitude)==T ~ NA_character_, 
                               TRUE ~ geolatitude)) %>% 
    mutate(
      geolongitude = ifelse(!is.na(geolongitude), geolongitude %>%  readr::parse_number() %>% as.numeric,  NA),
      geolatitude  = ifelse(!is.na(geolatitude), geolatitude %>%  readr::parse_number() %>% as.numeric, NA)) %>%
    filter(!is.na(as.numeric(geolongitude) & !as.numeric(geolatitude))) %>% 
    filter(geomatchstatus=="Matched" & geoaccuracy == c("Close"))   ->cases_geocodedreportaddcloseaccuracy
  
  
  cases_geocodedreportadd %>% 
    dplyr::rename(CASE_ID =rCASE_ID ) %>% 
    group_by(CASE_ID, .drop=F) %>% 
    mutate(
      geolongitude = case_when(numbers_only(geolongitude)==T ~ geolongitude, 
                               letters_only(geolongitude)==T ~ NA_character_, 
                               TRUE ~ geolongitude), 
      geolatitude  = case_when(numbers_only(geolatitude)==T ~ geolatitude, 
                               letters_only(geolatitude)==T ~ NA_character_, 
                               TRUE ~ geolatitude)) %>% 
    mutate(
      geolongitude = ifelse(!is.na(geolongitude), geolongitude %>%  readr::parse_number() %>% as.numeric,  NA),
      geolatitude  = ifelse(!is.na(geolatitude), geolatitude %>%  readr::parse_number() %>% as.numeric, NA)) %>%
    filter(!is.na(as.numeric(geolongitude) & !as.numeric(geolatitude))) %>% 
    filter(geomatchstatus=="Matched" & geoaccuracy != c("Close"))->cases_geocodedreportaddNOTcloseaccuracy
  
  
  toc()
  
  
  
  
  # > geocode primary address -----------------------------------------------
  tic("set cases list for primary address attempt")
  # NOTES: identify cases that did not match / were filtered out of reporting
  # address and geocode primary address
  # (a)	prepped as a URL string and put as a list (CASE_ID and URL address string)
  # (b)	URL address string sent webservice for geocoding, address correction, and geoprocessing (safe handling of errors here)
  # (c)	Returned results come back in XML
  # (d)	XML is parsed and converted to list (geoprocessed list)
  
  
  cases_list_unnested %>% 
    filter(!(CASE_ID %in% cases_geocodedreportaddcloseaccuracy$CASE_ID) &
             !(is.na(ADDRESS_LINE1_c))) %>% 
    group_by(CASE_ID,.drop=F) %>% 
    nest(.names_sep = "CASE_ID")->px
  toc()
  
  
  
  Sys.time()
  tic(glue::glue("parallel process time elapsed to geocode {length(px[['data']])} primary addresses using {nw} workers"))
  saferhandling = possibly( .f = ~ .x %>% httr::GET(.) %>% content(.,as =  "text")  %>% xmlParse() %>% xmlToList()   
                            , otherwise = "errorrrrr")
  
  furrr::future_map(.x = px$data  , .f =~saferhandling( .x[['primarystr']])) ->py
  
  toc()->timeelapsedgeocodeprimaryadd
  lubridate::duration(round(timeelapsedgeocodeprimaryadd$toc-timeelapsedgeocodeprimaryadd$tic, 2), units ="seconds") ->timeelapsedgeocodeprimaryadd
  
  
  
  
  
  # > map/parse primary add to df -------------------------------------------
  tic("map/parse primary add to df")
  # (e)	Cases list is joined/iterated over geoprocessed list -> output as
  # dataframe at case level with geocoded, address corrected, and geoprocessed
  # fields.
  
  plyr::round_any(length(px[['data']])/nw, 100, f = ceiling)->chnksize
  
  
  
  
  # > LOGIC: HANDLE ERROR 0 PRIMARY ADDRESSES ---------------------------------
  
  
  # > if 0 primary addresses to geocode, use reporting addresses --------------
  
  if(length(px[['data']])==0){
    
    toc()->timeelapsedparsingprimarygadd
    lubridate::duration(round(timeelapsedparsingprimarygadd$toc-timeelapsedparsingprimarygadd$tic, 2), units ="seconds") ->timeelapsedparsingprimarygadd
    cases_matched <- cases_geocodedreportaddcloseaccuracy
    cases_NOTmatched <- cases_matched %>% filter(CASE_ID=="0") 
    
    cases_geocodedprimaryadd=cases_geocodedreportaddcloseaccuracy %>% filter(CASE_ID=="0")
    
    glue::glue(
      "
--------------------
# of primary addresess sent for geoprocessing: {cases_geocodedprimaryadd %>% nrow}
-------------------      
"
    )
    
    # > ELSE: if primary address > 0 ------------------------------------------
    
    
  } else if (length(px[['data']])>0){
    
    
    
    
    # tic("map/parse primary add")
    furrr::future_map2_dfr(.x = px$data, 
                           .y = py, geoappending, 
                           .options = furrr_options(chunk_size = chnksize 
                                                    
                           )) %>%     mutate(georeportingprimaryaddmatch = ifelse(geomatchstatus =="Matched", "Primary", NA))->cases_geocodedprimaryadd
    
    glue::glue(
      "
--------------------
# of primary addresess sent for geoprocessing: {cases_geocodedprimaryadd %>% nrow}
# of primary addresses geocoded with close accuracy: {cases_geocodedprimaryadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow}
% of primary addresses successfully geocoded/matched with close accuracy: {round(cases_geocodedprimaryadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ cases_geocodedprimaryadd %>% nrow,1)}%
% of primary addresses of all cases successfully geocoded/matched with close accuracy: {round(cases_geocodedprimaryadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ cases %>% nrow,1)}% 
-------------------      
"
    )
    
    rm(px,py, cases_list_unnested)
    toc()->timeelapsedparsingprimarygadd
    lubridate::duration(round(timeelapsedparsingprimarygadd$toc-timeelapsedparsingprimarygadd$tic, 2), units ="seconds") ->timeelapsedparsingprimarygadd
    
    
    
    
    
    # toc()
    # > if successful save R Object with primary addresses here ----------------
    
    #tic("save primary addresses R object")
    
    # setwd("filepath/")
    # save(cases_geocodedprimaryadd, file=glue::glue("geocoded_primaryadd_covid_cases_{Sys.Date()}.RData"))
    # load( file=glue::glue("geocoded_primaryadd_covid_cases_{Sys.Date()-2}.RData"))
    # toc()
    # > select cases with long lat + close geocoding accuracy -----------------
    tic("clean long lat + close geocoding accuracy - primary add")
    
    # NOTES: select cases with a lat / long assigned and close accuracy
    
    
    
    cases_geocodedprimaryadd %>% 
      # dplyr::select(rCASE_ID, REPORTING_ADDRESS_c, dplyr::starts_with(vars = names(geocoded_covid_cases), match =  "geo"), dplyr::ends_with(vars = names(geocoded_covid_cases), match = "_c"), "geocorrectstreetnamepostdir", "geosubaddresstype", -rCASE_ID) %>% 
      dplyr::rename(CASE_ID =rCASE_ID ) %>% 
      group_by(CASE_ID, .drop=F) %>% 
      mutate(
        geolongitude = case_when(numbers_only(geolongitude)==T ~ geolongitude, 
                                 letters_only(geolongitude)==T ~ NA_character_, 
                                 TRUE ~ geolongitude), 
        geolatitude  = case_when(numbers_only(geolatitude)==T ~ geolatitude, 
                                 letters_only(geolatitude)==T ~ NA_character_, 
                                 TRUE ~ geolatitude)) %>% 
      mutate(
        geolongitude = ifelse(!is.na(geolongitude), geolongitude %>%  readr::parse_number() %>% as.numeric,  NA),
        geolatitude  = ifelse(!is.na(geolatitude), geolatitude %>%  readr::parse_number() %>% as.numeric, NA)) %>%
      filter(!is.na(as.numeric(geolongitude) & !as.numeric(geolatitude))) %>% 
      filter(geomatchstatus=="Matched" & geoaccuracy == c("Close"))  -> cases_geocodedprimaryaddcloseaccuracy
    
    
    # if not close accuracy - keep bind with primary (helpful for geocorrect fields) 
    cases_geocodedprimaryadd %>% 
      dplyr::rename(CASE_ID =rCASE_ID ) %>% 
      group_by(CASE_ID, .drop=F) %>% 
      mutate(
        geolongitude = case_when(numbers_only(geolongitude)==T ~ geolongitude, 
                                 letters_only(geolongitude)==T ~ NA_character_, 
                                 TRUE ~ geolongitude), 
        geolatitude  = case_when(numbers_only(geolatitude)==T ~ geolatitude, 
                                 letters_only(geolatitude)==T ~ NA_character_, 
                                 TRUE ~ geolatitude)) %>% 
      mutate(
        geolongitude = ifelse(!is.na(geolongitude), geolongitude %>%  readr::parse_number() %>% as.numeric,  NA),
        geolatitude  = ifelse(!is.na(geolatitude), geolatitude %>%  readr::parse_number() %>% as.numeric, NA)) %>%
      filter(!is.na(as.numeric(geolongitude) & !as.numeric(geolatitude))) %>% 
      filter(geomatchstatus=="Matched" & geoaccuracy != c("Close"))  -> cases_geocodedprimaryaddNOTcloseaccuracy
    
    
    toc()
    
    
    
    # 6. ROW BIND MATCHED CASES -----------------------------------------------
    
    
    tic("combine geocoded cases")
    # NOTES: combine all census tract information and join to cases dataset
    
    dplyr::bind_rows(
      cases_geocodedreportaddcloseaccuracy ,
      cases_geocodedprimaryaddcloseaccuracy
    )->cases_matchedclose
    
    dplyr::bind_rows(cases_geocodedreportaddNOTcloseaccuracy ,
                     cases_geocodedprimaryaddNOTcloseaccuracy ) %>% 
      dplyr::select(CASE_ID, dplyr::starts_with(vars = names(cases_matchedclose), match =  "geo"), dplyr::ends_with(vars = names(cases_matchedclose), match = "_c")) %>% 
      filter(!(CASE_ID %in% cases_matchedclose$CASE_ID)) %>% 
      mutate(
        duplicatedCASE_ID = case_when(
          duplicated(CASE_ID)==F ~ "not duplicated CASE_ID",
          duplicated(CASE_ID)==T ~ "duplicated CASE_ID"
        ), 
        keepCASE_ID = case_when(
          duplicatedCASE_ID =="not duplicated CASE_ID" & georeportingprimaryaddmatch =="Reporting" ~ "keep", 
          duplicatedCASE_ID =="not duplicated CASE_ID" ~ "keep", 
          TRUE ~ "not keep"
        )) %>% 
      filter(keepCASE_ID =="keep") %>% 
      dplyr::select(-keepCASE_ID, -duplicatedCASE_ID) %>% 
      dplyr::bind_rows(x= . , 
                       y= cases_matchedclose) ->cases_matched
    
    
    toc()
    
  }
  
  
  # 7. SPATIAL JOIN TO ADD CENSUS TRACT -------------------------------------
  
  # > cases as points + transform to state plane ------------------------------
  
  tic("spatial joining to get census tract")
  library(sf) %>% suppressMessages() %>% suppressWarnings()
  
  
  # EPSG code is ESRI:102749 | https://www.spatialreference.org/ref/esri/102749/ | "NAD_1983_StatePlane_Washington_South_FIPS_4602_Feet"
  CRSsp <- "+proj=longlat +datum=WGS84"
  # useCRS<- "+proj=lcc +lat_1=45.83333333333334 +lat_2=47.33333333333334 +lat_0=45.33333333333334 +lon_0=-120.5 +x_0=500000.0000000002 +y_0=0 +ellps=GRS80 +datum=NAD83 +to_meter=0.3048006096012192 +no_defs"
  useCRS <- "ESRI:102749"
  
  # get test results as spatial points
  points <-
    cases_matched %>% 
    mutate(long = geolongitude,                 
           lat = geolatitude ) %>% 
    st_as_sf(coords = c("long", "lat")) %>%
    st_set_crs(CRSsp) %>%
    st_transform(useCRS) 
  
  # DATA: CENSUS TRACTS -----------------------------------------------------
  # NOTES: loading census tract shapefile
  
  
  readRDS("filepath/censustract.RDS"
  ) %>%  spTransform(CRS("+proj=longlat +ellps=WGS84 +datum=WGS84"))  -> washpcensustract 
  
  mutate_if(washpcensustract@data, is.factor, as.character)   -> washpcensustract@data 
  
  
  # > census tracts as polygon + transform to state plane -------------------
  
  polygon <- washpcensustract %>% 
    st_as_sf( as(., "Spatial")) %>% 
    st_transform(st_crs(useCRS))
  
  # see the coordinate reference system of the points
  # st_crs(polygon)
  # class(polygon)
  
  
  
  # > spatial join points (cases) in polygon (censustract) -------------------
  
  cases_censust_stateplane_r <- st_join(x= points, y = polygon, join= st_within)
  
  
  cases_matchedcensust<- cases_censust_stateplane_r %>% as.data.frame %>%
    dplyr::rename(geocensustract = GEOID10) %>% 
    dplyr::select(-geometry,
                  -STATEFP10 ,
                  -COUNTYFP10,
                  -TRACTCE10 , 
                  -NAME10 ,
                  -NAMELSAD10,
                  -MTFCC10   , 
                  -FUNCSTAT10, 
                  -ALAND10   ,  
                  -AWATER10  ,  
                  -INTPTLAT10,
                  -INTPTLON10)
  
  
  toc()
  
  
  # 8. COMBINE ALL CASES MATCHED TO COVID_CASES and GEOCODED_COVID_C --------
  
  tic("combine geocoded cases with covid_cases")
  
  
  cases_matchedcensust %>% 
    dplyr::bind_rows(x=.,
                     y= geocoded_covid_cases %>% dplyr::select(CASE_ID, dplyr::starts_with(vars = names(cases_matchedcensust), match =  "geo"), dplyr::ends_with(vars = names(cases_matchedcensust), match = "_c")) %>% 
                       filter(!(CASE_ID %in% cases_matchedcensust$CASE_ID))) %>% 
    base::merge(x=.,
                y=covid_cases, by = "CASE_ID", all.x=F, all.y=T) %>%  
    dplyr::mutate(geomatchstatus = case_when(
      is.na(geomatchstatus)==T~ "Not Matched",
      geomatchstatus=="No Geocoding Attempted"~ "Not Matched",
      # CASE_ID %in% cases_geocodedreportadd$rCASE_ID & cases_geocodedreportadd$geomatchstatus=="Not Geocoded" ~ "Not Matched",
      # CASE_ID %in% cases_geocodedprimaryadd$rCASE_ID & cases_geocodedprimaryadd$geomatchstatus=="Not Geocoded" ~ "Not Matched",
      TRUE ~    geomatchstatus )) -> geocoded_covid_cases
  
  toc()
  
  # # check for duplicates
  glue::glue("# of CASE_ID's duplicated after binding cases matched and merge with covid_cases: {geocoded_covid_cases %>% filter(duplicated(CASE_ID)==T) %>% nrow}") %>% print
  
  
  print("geocoding performance:")
  glue::glue("geocoding performance: {cases_matched %>% nrow} of {cases %>% nrow} or {round(cases_matched %>% nrow*100/cases %>% nrow,1)}% case addresses matched and geocoded with close accuracy.") %>% print
  
  # stop("end")
  
  
  # 9. SAVE GEOCODED_COVID_CASES R OBJECT + BUC -----------------------------
  
  
  # save geocoded_covid_cases R Object --------------------------------------
  tic("save R object")
  
  
  # saved geocoded cases; 
  
  
  setwd("filepath/Geocoding")
  save(geocoded_covid_cases, file=glue::glue("geocoded_covid_cases.RData"))
  toc()
  
  
  # save buc of geocoded_covid_cases ----------------------------------------
  tic("save R object buc")
  setwd("filepath/Geocoding")
  save(geocoded_covid_cases, file=glue::glue("geocoded_covid_cases_{Sys.Date()}_BUC.RData"))
  # load(file=glue::glue("geocoded_covid_cases_{Sys.Date()}.RData"))
  # geocoded_covid_cases %>% names
  toc()
  
  
  
  
  # close clusters ----------------------------------------------------------
  future::plan(strategy = "sequential")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
  future::nbrOfWorkers()
  
  
  # 10. SEND EMAIL TO NOTIFY USERS (ONLY ON SATURDAYS; OTHER DAYS USED TO CREATE A LOG) -------------------------------------------
  
  if (file.exists(
    glue::glue(
      "filepath/covid_cases.Rdata"))){
    message = glue::glue(
      "{today_f} run - covid_cases R object used was last updated on: {file.info('filepath/covid_cases.Rdata') %>% dplyr::select(mtime) %>%  pull}")
    print(message)
  } else if (!file.exists(
    glue::glue("filepath/covid_cases.Rdata"))) {
    message = glue::glue(
      "{today_f} run - Potential issue with loading of covid_cases R object today. A previous snapshot of covid_cases was used instead:
   {list.files('filepath/',full.names = T) %>% enframe(name = NULL) %>% bind_cols(pmap_df(., file.info)) %>% filter(mtime==max(mtime)) %>% pull(value )} \n Review geocoded_covid_cases log and script.")
    print(message)
  }
  
  
  if (today_w == 'Saturday') {
    
    today <- lubridate::today()
    time_n <- format(Sys.time(), format = "%H:%M")
    effective_f <- format(as.Date("2023-04-21"),"%A, %B %d, %Y")
    today_f <- format(today,"%A, %B %d, %Y")
    
    teamsignature <- glue::glue(
      "
 _

    Data Integration & Quality Assurance (DIQA) Team
    Public Health Outbreak Coordination, Informatics, Surveillance (PHOCIS) - Surveillance Section
    Division of Disease Control and Health Statistics
    Washington State Department of Health

 _
")
    autoemailmessage <- glue::glue("***This is an auto-generated email notifying of update to gecocoded_covid_cases R Object***")
    
    contributionsmessage <- glue::glue("This work would not be possible with out the contributions of: Craig Erickson, Ian Painter, Lilli Manahan, Jon Downs, Rasha Elnimeiry, Tessa Fairfortune, Caitlin Oleson.")
    outpath <-  "filepath/geocoded_covid_cases.RData"
    # tic()
    toc() ->timeelapsed
    lubridate::duration(round(timeelapsed$toc-timeelapsed$tic, 2), units ="seconds") ->timeelapsed
    
    # from
    f <-  sprintf("<your.email@email.com>")
    
    
    readxl::read_excel(paste0("/filepath.xlsx"),trim_ws = T)  %>%
      mutate(email_address = textclean::strip(email_address, char.keep = c( "@", ".", "-"), digit.remove = F, apostrophe.remove = T, lower.case = F),
             email_address = glue::glue("<{email_address}>")) -> emaildistr
    
    
    rl <- sprintf(c(emaildistr$email_address))
    
    # subject
    subj <- glue::glue("{time_n} AUTOMATED EMAIL: geocoded_covid_cases R Object is Available")
    # body
    bdy <- glue::glue("
Dear Colleague(s),

* An update is available for the geocoded_covid_cases R object here:
{outpath} 

---- PURPOSE
The purpose of this R object is to provide users of covid_cases.RData with geospatial COVID-19 case data enchancement based on reporting and primary address fields.

geocoded_covid_cases R object contains:
    * address correction for validated addresses (fields with prefix 'geocorrect')
    * geocoding (fields: 'geolongitude' and 'geolatitude')
    * geoprocessing (which census tract, census block, school district, if falls in tribal land, fields: 'geocensustract', 'geocensusblock', 'geotribal', 'geoschooldistr', etc. )
    * and more! (is address mailable? is address a residential or business, a street address or highrise, is address an apt or trailer, is address found in USPS database, accuracy of match, quality of address i.e what address components were incorrect/missing and corrected, data source of the address locator used for match, which address was used to geocode i.e. reporting or primary address, etc.)

---- SUBSCRIPTION
* To make additions/removals to distribution list add/remove linelist information on this spreadsheet:
filepath.xlsx

* Subscription status is updated as soon as the next morning when this script runs.

---- DOCUMENTATION + DATA DICTIONARY
* Documentation + data dictionary for geocoded_covid_cases R object is found here: 
filepath.docx


---- METRICS
Below are metrics for geocoded_covid_cases R object. 

Current Performance (Current Run):

    Reporting address:
    --------------------
    # of reporting addresses sent for acgeo web services: {cases_geocodedreportadd %>% nrow %>% formattable::comma(0)}
    # of reporting addresses geocoded with close accuracy: {cases_geocodedreportadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow %>% formattable::comma(0)}
    % of reporting addresses of cases successfully matched and geocoded with close accuracy: {round(cases_geocodedreportadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ cases %>% nrow, 1)}% 
     -------------------   
    
    Primary address: 
    -------------------
    # of primary addresses sent for acgeo web services: {cases_geocodedprimaryadd %>% nrow %>% formattable::comma(0)}
    # of primary addresses matched and geocoded with close accuracy: {cases_geocodedprimaryadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow %>% formattable::comma(0)}
    % of primary addresses successfully matched and geocoded with close accuracy: {round(cases_geocodedprimaryadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ cases_geocodedprimaryadd %>% nrow,1)}%
     -------------------      
    
    Timing:
    -------------------
    Time elapsed geocoding reporting addresses: {timeelapsedgeocodereportingadd}
    Time elapsed geocoding primary addresses: {timeelapsedgeocodeprimaryadd}
    Time elapsed parsing reporting addresses: {timeelapsedparsingreportingadd}
    Time elapsed parsing primary addresses: {timeelapsedparsingprimarygadd}
     
    
    Quality Checks:
    -------------------   
    # of CASE_ID's duplicated in covid_cases R Object: {cases %>% filter(duplicated(CASE_ID)==T) %>% nrow}
    # of CASE_ID's duplicated after combining matched gecoding results (reporting and primary): {cases_matched %>% filter(duplicated(CASE_ID)==T) %>% nrow}
    # of CASE_ID's duplicated after combining matched results with covid_cases: {geocoded_covid_cases %>% filter(duplicated(CASE_ID)==T) %>% nrow}

    # of cases attempted to geocode: {cases %>% nrow %>% formattable::comma(0)}
    # of cases addresses successfully matched and geocoded with close accuracy: {cases_matched %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>%  nrow %>% formattable::comma(0)} 
    Total % of cases addresses successfully matched and geocoded with close accuracy: {round(cases_matched %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>%  nrow*100/cases %>% nrow,1)}% 

Overall Performance (All Runs):
 -------------------   
    Total # of cases in geocoded_covid_cases: {geocoded_covid_cases %>% nrow %>% formattable::comma(0)}
    Total # of cases in geocoded_covid_cases with addresses successfully matched and geocoded with close accuracy: {geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>% nrow %>% formattable::comma(0) }
    Total # of cases attempted to geocode and not matched: {geocoded_covid_cases %>% filter(geomatchstatus=='Not Matched' | (geoaccuracy!='Close' ) )%>% nrow %>% formattable::comma(0)}
    Total % of case addresses in geocoded_covid_cases currently matched and gecoded with close accuracy: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>% nrow*100/geocoded_covid_cases %>% nrow,1)}%
    Total % of cases successfully matched and geocoded with close accuracy using reporting address: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close') & georeportingprimaryaddmatch=='Reporting') %>% nrow*100/ geocoded_covid_cases %>% nrow,1)}%
    Total % of cases successfully matched and geocoded with close accuracy using primary address: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close') &  georeportingprimaryaddmatch=='Primary') %>% nrow*100/ geocoded_covid_cases %>% nrow,1)}% 
    # of CASE_ID's missing 'geocorrectcity' field: {geocoded_covid_cases %>% filter(is.na(geocorrectcity)) %>% nrow %>% formattable::comma(0)} ({round(geocoded_covid_cases %>% filter(is.na(geocorrectcity)) %>% nrow*100/geocoded_covid_cases %>% nrow, 1)}% of all cases) 
    * Total time elapsed to run geocoded_covid_cases R script: {timeelapsed} *
 -------------------   

{message}

{autoemailmessage}

{contributionsmessage}

Thank you and have a great day.

{teamsignature}")
    
    
    bdy
    
    # send email
    
    sendmailR::sendmail(f, rl, subj, bdy,
                        # cl,
                        # bl,
                        headers= list("Reply-To"="<your.email@email>"), control= list(smtpServer= "relay"))
    glue::glue('done - sent email')
    
  } else {
    today <- lubridate::today()
    time_n <- format(Sys.time(), format = "%H:%M")
    effective_f <- format(as.Date("2023-04-21"),"%A, %B %d, %Y")
    today_f <- format(today,"%A, %B %d, %Y")
    
    
    contributionsmessage <- glue::glue("This work would not be possible with out the contributions of: Craig Erickson, Ian Painter, Lilli Manahan, Jon Downs, Rasha Elnimeiry, Tessa Fairfortune, Caitlin Oleson.")
    outpath <-  "filepath/geocoded_covid_cases.RData"
    # tic()
    toc() ->timeelapsed
    lubridate::duration(round(timeelapsed$toc-timeelapsed$tic, 2), units ="seconds") ->timeelapsed
    
    # body
    bdy <- glue::glue("
AUTOMATED LOG {time_n}: geocoded_covid_cases R Object is Available
--------------------------------------      

* An update is available for the geocoded_covid_cases R object here:{outpath} 
* QA logs are available here: filepath/Geocoding/QA_Logs/ 

---- PURPOSE

The purpose of this R object is to provide users of covid_cases.RData with geospatial COVID-19 case data enchancement based on reporting and primary address fields.

geocoded_covid_cases R object contains:
* address correction for validated addresses (fields with prefix 'geocorrect')
* geocoding (fields: 'geolongitude' and 'geolatitude')
* geoprocessing (which census tract, census block, school district, if falls in tribal land, fields: 'geocensustract', 'geocensusblock', 'geotribal', 'geoschooldistr', etc. )
* and more! (is address mailable? is address a residential or business, a street address or highrise, is address an apt or trailer, is address found in USPS database, accuracy of match, quality of address i.e what address components were incorrect/missing and corrected, data source of the address locator used for match, which address was used to geocode i.e. reporting or primary address, etc.)

---- SUBSCRIPTION
* To make additions/removals to distribution list add/remove linelist information on this spreadsheet:
filepath.xlsx

* Subscription status is updated as soon as the next morning when this script runs.

---- DOCUMENTATION + DATA DICTIONARY
* Documentation + data dictionary for geocoded_covid_cases R object is found here: 
filepath.docx


---- METRICS

Below are metrics for geocoded_covid_cases R object. 

Current Performance (Current Run):

    Reporting address:
    --------------------
    # of reporting addresses sent for acgeo web services: {cases_geocodedreportadd %>% nrow %>% formattable::comma(0)}
    # of reporting addresses geocoded with close accuracy: {cases_geocodedreportadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow %>% formattable::comma(0)}
    % of reporting addresses of cases successfully matched and geocoded with close accuracy: {round(cases_geocodedreportadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ cases %>% nrow, 1)}% 
     -------------------   
    
    Primary address: 
    -------------------
    # of primary addresses sent for acgeo web services: {cases_geocodedprimaryadd %>% nrow %>% formattable::comma(0)}
    # of primary addresses matched and geocoded with close accuracy: {cases_geocodedprimaryadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow %>% formattable::comma(0)}
    % of primary addresses successfully matched and geocoded with close accuracy: {round(cases_geocodedprimaryadd %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close')) %>% nrow*100/ cases_geocodedprimaryadd %>% nrow,1)}%
     -------------------      
    
    Timing:
    -------------------
    Time elapsed geocoding reporting addresses: {timeelapsedgeocodereportingadd}
    Time elapsed geocoding primary addresses: {timeelapsedgeocodeprimaryadd}
    Time elapsed parsing reporting addresses: {timeelapsedparsingreportingadd}
    Time elapsed parsing primary addresses: {timeelapsedparsingprimarygadd}
     
    
    Quality Checks:
    -------------------   
    # of CASE_ID's duplicated in covid_cases R Object: {cases %>% filter(duplicated(CASE_ID)==T) %>% nrow}
    # of CASE_ID's duplicated after combining matched gecoding results (reporting and primary): {cases_matched %>% filter(duplicated(CASE_ID)==T) %>% nrow}
    # of CASE_ID's duplicated after combining matched results with covid_cases: {geocoded_covid_cases %>% filter(duplicated(CASE_ID)==T) %>% nrow}

    # of cases attempted to geocode: {cases %>% nrow %>% formattable::comma(0)}
    # of cases addresses successfully matched and geocoded with close accuracy: {cases_matched %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>%  nrow %>% formattable::comma(0)} 
    Total % of cases addresses successfully matched and geocoded with close accuracy: {round(cases_matched %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>%  nrow*100/cases %>% nrow,1)}% 

Overall Performance (All Runs):
 -------------------   
    Total # of cases in geocoded_covid_cases: {geocoded_covid_cases %>% nrow %>% formattable::comma(0)}
    Total # of cases in geocoded_covid_cases with addresses successfully matched and geocoded with close accuracy: {geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>% nrow %>% formattable::comma(0) }
    Total # of cases attempted to geocode and not matched: {geocoded_covid_cases %>% filter(geomatchstatus=='Not Matched' | (geoaccuracy!='Close' ) )%>% nrow %>% formattable::comma(0)}
    Total % of case addresses in geocoded_covid_cases currently matched and gecoded with close accuracy: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>% nrow*100/geocoded_covid_cases %>% nrow,1)}%
    Total % of cases successfully matched and geocoded with close accuracy using reporting address: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close') & georeportingprimaryaddmatch=='Reporting') %>% nrow*100/ geocoded_covid_cases %>% nrow,1)}%
    Total % of cases successfully matched and geocoded with close accuracy using primary address: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close') &  georeportingprimaryaddmatch=='Primary') %>% nrow*100/ geocoded_covid_cases %>% nrow,1)}% 
    # of CASE_ID's missing 'geocorrectcity' field: {geocoded_covid_cases %>% filter(is.na(geocorrectcity)) %>% nrow %>% formattable::comma(0)} ({round(geocoded_covid_cases %>% filter(is.na(geocorrectcity)) %>% nrow*100/geocoded_covid_cases %>% nrow, 1)}% of all cases) 
    * Total time elapsed to run geocoded_covid_cases R script: {timeelapsed} *
 -------------------   

{message}

{contributionsmessage}")
    
    gcc_log <- blastula::compose_email(body = blastula::md(bdy))
    
    htmltools::save_html(gcc_log$html_html, file = paste0("filepath/geocoded_covid_cases_QA_Log.html"))
    
    
    gcc_QA_message <- glue::glue("
* An update is available for the geocoded_covid_cases R object here: {outpath} 
* QA logs are available here: filepath/Geocoding/QA_Logs/ 

---- **Quality Checks:**
* N of cases attempted to geocode: {cases %>% nrow %>% formattable::comma(0)}
* N of cases addresses successfully matched and geocoded with close accuracy: {cases_matched %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>%  nrow %>% formattable::comma(0)} 
* Total % of cases addresses successfully matched and geocoded with close accuracy: {round(cases_matched %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>%  nrow*100/cases %>% nrow,1)}% 
      
---- **Overall Performance (All Runs):**
* Total N of cases in geocoded_covid_cases: {geocoded_covid_cases %>% nrow %>% formattable::comma(0)}
* Total N of cases in geocoded_covid_cases with addresses successfully matched and geocoded with close accuracy: {geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>% nrow %>% formattable::comma(0) }
* Total N of cases attempted to geocode and not matched: {geocoded_covid_cases %>% filter(geomatchstatus=='Not Matched' | (geoaccuracy!='Close' ) )%>% nrow %>% formattable::comma(0)}
* Total % of case addresses in geocoded_covid_cases currently matched and gecoded with close accuracy: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy=='Close') %>% nrow*100/geocoded_covid_cases %>% nrow,1)}%
* Total % of cases successfully matched and geocoded with close accuracy using reporting address: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close') & georeportingprimaryaddmatch=='Reporting') %>% nrow*100/ geocoded_covid_cases %>% nrow,1)}%
* Total % of cases successfully matched and geocoded with close accuracy using primary address: {round(geocoded_covid_cases %>% filter(geomatchstatus=='Matched' & geoaccuracy == c('Close') &  georeportingprimaryaddmatch=='Primary') %>% nrow*100/ geocoded_covid_cases %>% nrow,1)}% 
* N of CASE_ID's missing 'geocorrectcity' field: {geocoded_covid_cases %>% filter(is.na(geocorrectcity)) %>% nrow %>% formattable::comma(0)} ({round(geocoded_covid_cases %>% filter(is.na(geocorrectcity)) %>% nrow*100/geocoded_covid_cases %>% nrow, 1)}% of all cases) 
* Total time elapsed to run geocoded_covid_cases R script: {timeelapsed}")   
    
    save(gcc_QA_message, file = paste0(GITDIR, "filepath/gcc_QA_message.RData"))
    
  }
  
  setwd("filepath")
  
  toc() ->timeelapsedfull
  lubridate::duration(round(timeelapsedfull$toc-timeelapsedfull$tic, 2), units ="seconds") ->timeelapsedfull
  
  glue::glue(" total time elapsed: {timeelapsedfull}")
  print(glue::glue("END -------------- {Sys.time()}--------------"))
  '-------------------------------------------------------------------------------------------------------------'
  '-------------------------------------------------------------------------------------------------------------'
  '-------------------------------------------------------------------------------------------------------------'
  
}