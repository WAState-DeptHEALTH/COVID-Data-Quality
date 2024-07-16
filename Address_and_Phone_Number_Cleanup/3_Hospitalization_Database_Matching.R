################################################################################################
#PART 3: HOSPITALIZATION DATABASE MATCHING AND FINAL ROSTER CREATION
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
#  **This script addresses the third part of this process: linking positive COVID-19 cases from WDRS (the WA State database) 
#   with missing address/phone information to the RHINO (Rapid Health Information NetwOrk) hospitalization database in order to obtain address and phone numbers 
#   for matching records. Matching is based on First Name, Last Name, and Date of Birth (DOB)**
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
#     -fuzzy matching: technique for finding an approximate match between two different lines of data (in this instance, we use it to approximately match DOB by Year, Month, and Day)
#     -exact matching: technique for finding an exact or precise match between two different lines of data (in this instance, each character in a person's first and last name must match)
#     -API: Application programming interface; in this context, the USPS API allows R to interact with the USPS software to help us geocode addresses and determine the reliability of the addresses we found in the two database searches
#
#
################################################################################################
################################################################################################


pacman::p_load(data.table, 
               dplyr,
               httr,
               xml2,
               glue)

today1 <- Sys.Date()
today2 <- format(today1, "%Y%m%d")


# Load credentials:
# "R_Creds"
# Here, an external .rds file is referenced in order to call database names, emails, usernames, passwords, etc. in a more secure way by
# keeping sensitive information outside of the script. 
# The file is created in a separate R script, saved as a .rds file, (referred to in this script and subsequent scripts as "r_creds"
# and kept in a standard file location across team members so that the script stays the same regardless of 
# who is running it and on what computer.

################################################################################################
###########                  Block 1                     #######################################
###########     Create standardized xmatched datasets    #######################################
################################################################################################


# Read in missing contact info data set (mco) and xmatched contact info data sets: ----------------------------------------------------------------------

# Read in our original missing contact data set:
mco <- readRDS(file = glue('filepath.RDS'))
mco <- mco %>% dplyr::mutate(across(.fns = as.character))

# Read in our IIS matched data set:
iis <- readRDS(glue('filepath.rds'))
iis <- iis %>% mutate(across(.fns = as.character))

# Read in our RHINO matched data set:
rhino <- data.table::fread(glue('filepath.csv'))
rhino <- rhino %>% mutate(across(.fns = as.character))


# Clean phone, county, and state (for RHINO) data in xmatched data sets: ---------------------------------------------------------------------------------------------------------------------

### clean phone:
source('filepath/badPhone.R')

# Clean up our phone data:
iis <- iis %>% 
  mutate(across(contains('phone'), function(x) gsub('\\D', '', x))) %>% #convert all phones to only digits
  mutate(across(contains('phone'), badPhone)) #convert bad numbers to NA
rhino$PtPhone <- badPhone(gsub('\\D', '', rhino$PtPhone)) #convert phones to only digits and bad numbers to NA


### clean RHINO:
### Find US FIPS Codes here: https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt
# Recode RHINO state and counties:
fips <- data.table::fread('filepath/US_FIPS_Codes.csv',
                          colClasses = 'character')
rhino <- left_join(rhino, 
                   select(fips, CountyName, StateAbbr, FIPS5), 
                   by = c('PtCounty' = 'FIPS5'))

### clean county:

# Use this function below to clean counties.
# Input is a character element or vector. Output is an element or vector of the same length with bad county names
# (using criteria below: anything containing a number, uknown, homeless, unassigned,) changed to NA,
# and the substring ' County' removed from each element.
cleanCounty <- function(x) {
  x[grepl('[0-9]', x)] <- NA
  x[grepl('unknown|uknown|homeless|unassign', x, ignore.case = T)] <- NA
  x <- gsub('[ ]{0,}county', '', x, ignore.case = T) #remove ' county' (or 'county') from county name
  x[nchar(x) > 25 | nchar(gsub('[^[:alpha:]]', '',  x)) < 3] <- NA
  
  return(x)
}

iis$Patient_County <- cleanCounty(iis$Patient_County)
rhino$CountyName <- cleanCounty(rhino$CountyName)


# Create standardized xmatched dat sets: ---------------------------------------------------------------------------------------------------------------------
matched_info_iis <- data.frame(CASE_ID = iis$Event_ID,
                               Address = iis$Patient_Address,
                               City = iis$Patient_City,
                               County = iis$Patient_County,
                               State = iis$Patient_State,
                               Zip = iis$Patient_Zipcode,
                               Phone = coalesce(iis$Patient_Phone_Number, iis$Patient_Phone2, iis$Patient_Phone3, iis$Patient_Phone4, iis$Patient_Phone5))

matched_info_rhino <- data.frame(CASE_ID = rhino$`Event ID`,
                                 Address = rhino$PtStreet,
                                 City = rhino$PtCity,
                                 County = rhino$CountyName,
                                 State = rhino$StateAbbr,
                                 Zip = rhino$PtZipcode,
                                 Phone = rhino$PtPhone)






################################################################################################
###################            Block 2          ################################################
###################         Fix Addresses       ################################################
################################################################################################



# Clean addresses in standardized xmatched data sets using USPS API and zip table to fill in County: ---------------------------------------------------------------------------------------------------------------------

### USPS matching:

#' address_validion
#' @description This function takes one of the standardized xmatched data sets and sends each address through the
#' USPS Address Verification API. This API returns corrected addresses or errors if the address could not be 
#' verified. The functions will replace the data found in df$Address, City, State, and Zip with the verified
#' address parts for all rows (if unvalidated.na = T) or for only addresses where at least one address column in that
#' row are not present.
#' @details USPS Address API documentation: 
#' https://www.usps.com/business/web-tools-apis/address-information-api.pdf
#' https://www.usps.com/business/web-tools-apis/
#' @param df data frame with CASE_ID, Address, City, State, and Zip columns.
#' @param userid USPS Address Verification API username (required).
#' 
address_validation <- function(df, userid) {
  
  require(httr)
  require(xml2)
  require(glue)
  
  # Not the prettiest way to prepare our api request, but the easiest given the predictable structure of data. Could make this more flexible (especially if someone were to turn this into a more widely used function/package)
  string_pre <- glue('https://secure.shippingapis.com/ShippingAPI.dll?API=Verify&XML=<AddressValidateRequest%20USERID="{userid}">')
  string_post <- '</AddressValidateRequest>'
  
  df <- df %>% mutate(across(.fns = as.character)) #convert df to all character vectors
  df$usps.State <- df$State
  df$usps.State[is.na(df$Zip) & (is.na(df$State) | df$State == '')] <- 'WA' #convert empty and NA states w/o zip codes to 'WA'
  df[is.na(df)] <- '' #convert NA to blank
  df$Validated_Address <- F
  
  i <- 0
  
  while (i < nrow(df)) {
    
    i <- i+1
    
    address = df$Address[i] #create copy so we can reference if needed for street_flag later
    
    street_flag = regexpr('[0-9]', address) > 8 #flag address if there is a large substring before the first number
    
    string = glue('<Revision>1</Revision><Address><Address1>{address}</Address1><Address2></Address2><City>{df$City[i]}</City><State>{df$usps.State[i]}</State><Zip5>{df$Zip[i]}</Zip5><Zip4></Zip4></Address>')
    string = gsub('#{1}', 'Unit ', string) #replace a single hash with 'unit' so usps recognizes it as a unit/apt/ste/etc #
    string = gsub('[#$@^&*!?\\]+', '', string)
    
    url = modify_url(paste0(string_pre, string, string_post))
    
    a = GET(url)
    a = xml2::read_xml(a, as = 'parsed', encoding = 'UTF-8')
    a = xml2::as_list(a)
    a = a[[1]][[1]]
    # Error checking:
    if ('Error' %in% names(a) & street_flag) {
      df$Address[i] <- sub('[^0-9]+', '', address)
      i <- i-1 #redo i in the while loop
      next
    }
    
    if ('Error' %in% names(a)) next
    
    a = unlist(a)
    
    df$Address[i] <- ifelse(is.na(a['Address1']), a['Address2'], paste(a['Address2'], a['Address1'], sep = ', '))
    df$City[i] <- a['City']
    df$State[i] <- a['State']
    df$Zip[i] <- a['Zip5']
    df$Validated_Address[i] <- T
    
    
  } #end while statement
  
  
  df <- df %>% mutate(usps.State = NULL,
                      across(where(is.character), trimws))
  
  df[df==''] <- NA #convert blanks back to NA
  
  
  return(df)
}


matched_info_iis <- address_validation(matched_info_iis, r_creds$usps_api_username)
print(glue::glue('IIS: {nrow(filter(matched_info_iis, Validated_Address))} addresses were validated out of {nrow(filter(matched_info_iis, !is.na(Address) | !is.na(City)))} present addresses'))

matched_info_rhino <- address_validation(matched_info_rhino, r_creds$usps_api_username)
print(glue::glue('RHINO: {nrow(filter(matched_info_rhino, Validated_Address))} addresses were validated out of {nrow(filter(matched_info_rhino, !is.na(Address) | !is.na(City)))} present addresses'))


### End USPS matching


# Minimal address cleaning:
matched_info_iis$Address[!grepl('[0-9]', matched_info_iis$Address)] <- NA
matched_info_rhino$Address[!grepl('[0-9]', matched_info_rhino$Address)] <- NA


### County fill in using zip to id correct counties:
zip_counties <- read.table('filepath/tab20_zcta520_county20_natl.txt',
                           header = T, sep='|', na.strings = '', colClasses = 'character', quote = "", encoding = 'UTF-8')


zip_counties <- zip_counties %>%
  inner_join(fips, by = c('GEOID_COUNTY_20' = 'FIPS5')) %>%
  select(GEOID_ZCTA5_20, CountyName, StateAbbr) %>%
  filter(!is.na(GEOID_ZCTA5_20))

zip_counties <- zip_counties %>%
  group_by(GEOID_ZCTA5_20) %>%
  mutate(CountyName = case_when(max(row_number()) == 1 ~ CountyName)) %>% #get rid of county for zips that span multiple counties
  distinct() %>%
  filter(max(row_number()) == 1) %>% #get rid of rows for zips that span multiple states
  ungroup()




zip_fill_in <- function(df, zip_df) {
  df <- df %>%
    left_join(zip_df, by = c('Zip' = 'GEOID_ZCTA5_20')) %>%
    mutate(State = case_when(!is.na(State) ~ State,
                             T ~ StateAbbr),
           County = case_when(!is.na(County) ~ County,
                              T ~ CountyName),
           StateAbbr = NULL, CountyName = NULL)
  
  return(df)
}



### Use zip to fill in state and/or county if those are missing and zip is present:

matched_info_rhino <- zip_fill_in(matched_info_rhino, zip_counties)
matched_info_iis <- zip_fill_in(matched_info_iis, zip_counties)


############End zip fill in





################################################################################################
############                 Block 3                    ########################################
############        Fill in missing contact info        ########################################
################################################################################################




############ Cross match missing info from matched data:

# Ensure these addresses are blank (they should be already, but matching results will be whacky if not):
mco[!(mco$Tag_1 %in% 'Missing phone'), c('REPORTING_ADDRESS', 'REPORTING_CITY', 'REPORTING_STATE', 'REPORTING_COUNTY', 'REPORTING_ZIPCODE')] <- NA




# Join validated address information using hierarchy IIS then RHINO: (because we have determined IIS macthes are slightly better than RHINO)

matching_full <- function(df, df2) {
  cols <- names(df)
  
  df <- df %>% 
    left_join(df2, by = 'CASE_ID') %>% 
    mutate(complete_address_og = case_when(!is.na(REPORTING_ADDRESS) & !is.na(REPORTING_CITY) & !is.na(REPORTING_STATE) & !is.na(REPORTING_ZIPCODE) ~ T, #removed "& !is.na(REPORTING_COUNTY). DS will fill in during their manual process.
                                           T ~ F),
           only_phone = (Tag_1 %in% 'Missing phone'), #T indicates the case was pulled only for missing phone, F indicates case needs address matching
           
           REPORTING_ADDRESS = case_when(only_phone | complete_address_og ~ REPORTING_ADDRESS, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                         Validated_Address ~ Address,
                                         T ~ REPORTING_ADDRESS),
           REPORTING_CITY = case_when(only_phone | complete_address_og ~ REPORTING_CITY, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                      Validated_Address ~ City,
                                      T ~ REPORTING_CITY),
           REPORTING_STATE = case_when(only_phone | complete_address_og ~ REPORTING_STATE, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                       Validated_Address ~ State,
                                       T ~ REPORTING_STATE),
           REPORTING_ZIPCODE = case_when(only_phone | complete_address_og ~ REPORTING_ZIPCODE, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                         Validated_Address ~ Zip,
                                         T ~ REPORTING_ZIPCODE),
           REPORTING_COUNTY = case_when(only_phone | complete_address_og ~ REPORTING_COUNTY, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                        Validated_Address ~ County,
                                        T ~ REPORTING_COUNTY),
           
           PHONE_NUMBER = case_when(Tag_1 %in% 'Missing address' ~ PHONE_NUMBER,
                                    !is.na(PHONE_NUMBER) ~ PHONE_NUMBER,
                                    T ~ Phone)) %>% 
    select(all_of(cols))
  
  return(df)
}  


mco_xmatched <- mco

# join verified addresses and print how many addresses were filled from which source:
counter.addresses.to.add <- nrow(filter(mco_xmatched, Tag_1 %in% c('Missing address', 'Missing demographics')))


mco_xmatched <- mco_xmatched %>% matching_full(matched_info_iis) 
# Count how many addresses were added by IIS data:
counter.verified.iis <- nrow(filter(mco_xmatched, !is.na(REPORTING_ADDRESS), Tag_1 %in% c('Missing address', 'Missing demographics')))
# Print how many addresses were added by IIS data:
print(glue::glue('IIS: {counter.verified.iis} out of {counter.addresses.to.add} missing addresses filled in by verified IIS addresses'))


mco_xmatched <- mco_xmatched %>% matching_full(matched_info_rhino)
# Count how many addresses were added by RHINO data:
counter.verified.rhino <- nrow(filter(mco_xmatched, !is.na(REPORTING_ADDRESS), Tag_1 %in% c('Missing address', 'Missing demographics'))) - counter.verified.iis
# Print how many addresses were added by RHINO data:
print(glue::glue('RHINO: {counter.verified.rhino} out of {counter.addresses.to.add} missing addresses filled in by verified RHINO addresses'))


# Print that total number of verified addresses added:
print(glue::glue('Total: {counter.verified.iis + counter.verified.rhino} out of {counter.addresses.to.add} missing addresses filled in by verified IIS & RHINO addresses'))

# Remove 'counter.' objects that were used for printing verified address matching results:
rm(counter.addresses.to.add, counter.verified.iis, counter.verified.rhino)



# Keep unvalidated/partial address information using hierarchy IIS then RHINO:

matching_partial <- function(df, df2) {
  cols <- names(df)
  
  df <- df %>% 
    left_join(df2, by = 'CASE_ID') %>% 
    mutate(complete_address_og = case_when(!is.na(REPORTING_ADDRESS) & !is.na(REPORTING_CITY) & !is.na(REPORTING_STATE) & !is.na(REPORTING_ZIPCODE) ~ T, #removed "& !is.na(REPORTING_COUNTY). DS will fill in during their manual process.
                                           T ~ F),
           present_address = !is.na(Address),
           only_phone = (Tag_1 %in% 'Missing phone'), #T indicates the case was pulled only for missing phone, F indicates case needs address matching
           
           REPORTING_ADDRESS = case_when(only_phone | complete_address_og ~ REPORTING_ADDRESS, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                         present_address  ~ Address,
                                         T ~ REPORTING_ADDRESS),
           REPORTING_CITY = case_when(only_phone | complete_address_og ~ REPORTING_CITY, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                      present_address ~ City,
                                      T ~ REPORTING_CITY),
           REPORTING_STATE = case_when(only_phone | complete_address_og ~ REPORTING_STATE, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                       present_address  ~ State,
                                       T ~ REPORTING_STATE),
           REPORTING_ZIPCODE = case_when(only_phone | complete_address_og ~ REPORTING_ZIPCODE, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                         present_address ~ Zip,
                                         T ~ REPORTING_ZIPCODE),
           REPORTING_COUNTY = case_when(only_phone | complete_address_og ~ REPORTING_COUNTY, #don't match addresses if the case was only pulled for a missing phone, or if data were already matched
                                        present_address ~ County,
                                        T ~ REPORTING_COUNTY)) %>%
    select(all_of(cols))
  
  return(df)
}


# Combine partial/unvalidated addresses:
mco_xmatched <- mco_xmatched %>% matching_partial(matched_info_iis)
mco_xmatched <- mco_xmatched %>% matching_partial(matched_info_rhino)




### Create final dataset -------------------------------------------------------------------------------------

mco_final <- mco_xmatched %>% 
  mutate(PHONE_NUMBER = case_when(Tag_1 == 'Missing address' ~ NA_character_, T ~ PHONE_NUMBER), #convert PHONE_NUMBER to NA for cases that were only pulled for address fill-in
         across(REPORTING_ADDRESS:REPORTING_COUNTY, function(x) ifelse(Tag_1 == 'Missing phone',NA_character_, x))) #convert address vars to NA for cases that were only pulled for phone fill-in

# Convert CASE_ID to int for DS, otherwise their conditional formatting fails:
mco_final$CASE_ID <- as.integer(mco_final$CASE_ID)



################################################################################################
##############               Block 4              ##############################################
##############        Create final datasets       ##############################################
################################################################################################



# create df containing only rows where new data were added:
info_added <- mco_final %>% 
  filter(
    (Tag_1 %in% c('Missing phone', 'Missing demographics') & !is.na(PHONE_NUMBER))
    | (Tag_1 %in% c('Missing address', 'Missing demographics') 
       & (!is.na(REPORTING_ADDRESS) 
          | !is.na(REPORTING_CITY)
          | !is.na(REPORTING_STATE)
          | !is.na(REPORTING_ZIPCODE)
          | !is.na(REPORTING_COUNTY)))
  ) %>% 
  mutate(REPORTING_COUNTY = case_when(nchar(REPORTING_COUNTY) >= 2 ~ gsub('county', '', REPORTING_COUNTY, ignore.case = T),
                                      T ~ NA_character_),
         Answer.REPORTING_ADDRESS = REPORTING_ADDRESS,
         Answer.REPORTING_CITY = REPORTING_CITY,
         Answer.REPORTING_COUNTY = REPORTING_COUNTY,
         Answer.REPORTING_STATE = REPORTING_STATE,
         Answer.REPORTING_ZIPCODE = REPORTING_ZIPCODE,
         Answer.CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE = '',
         Model.INVESTIGATION_STATUS = '',
         Model.INVESTIGATION_STATUS_UNABLE_TO_COMPLETE_REASON = '',
         Case.Note = 'Contact details data added via COVID-19 Updating contact details roster') %>%
  select(Case.CaseID = CASE_ID
         , PartyAttribute.Phone = PHONE_NUMBER
         , Party.PrimaryContactPoint.Street1 = REPORTING_ADDRESS
         , Party.PrimaryContactPoint.City = REPORTING_CITY
         , Party.PrimaryContactPoint.County = REPORTING_COUNTY
         , Party.PrimaryContactPoint.State = REPORTING_STATE
         , Party.PrimaryContactPoint.PostalCode = REPORTING_ZIPCODE
         , Answer.REPORTING_ADDRESS
         , Answer.REPORTING_CITY
         , Answer.REPORTING_COUNTY
         , Answer.REPORTING_STATE
         , Answer.REPORTING_ZIPCODE
         , Answer.CDC_N_COV_2019_DOH_CONTACT_LOOKUP_COMPLETE
         , Model.INVESTIGATION_STATUS
         , Model.INVESTIGATION_STATUS_UNABLE_TO_COMPLETE_REASON
         , Case.Note)


info_missing <- mco_final %>%
  filter(
    (Tag_1 %in% c('Missing phone', 'Missing demographics') & is.na(PHONE_NUMBER))
    | (Tag_1 %in% c('Missing address', 'Missing demographics') 
       & (is.na(REPORTING_ADDRESS)
          | is.na(REPORTING_CITY)
          | is.na(REPORTING_STATE)
          | is.na(REPORTING_ZIPCODE)))
  ) %>%
  mutate(Accurint = '',
         CDR = '',
         WAIIS = '',
         Experian = '',
         Notes = '') %>%
  select(Accurint,
         CDR,
         WAIIS,
         Experian,
         Notes,
         CASE_ID,
         Last.Name = LAST_NAME,
         First.Name = FIRST_NAME,
         DOB = BIRTH_DATE
  )




site_fp <- glue('filepath.RDS')

# create testing site dataset:

if(file.exists(site_fp)) {
  site <- readRDS(site_fp)
  site <- site$CASE_ID
  info_missing_site <- info_missing %>% filter(CASE_ID %in% site)
  info_missing <- info_missing %>% anti_join(info_missing_site)
  
  
} else {info_missing_site <- filter(info_missing, F)} # Create empty df for testing site address




################################################################################################
##############                  Block 5              ###########################################
##############        Write files and send email     ###########################################
################################################################################################

pacman::p_load(writexl)

# export final sheets:
write_xlsx(x = list('COVID19labsForUpload' = info_added, #add export for all cases with some new info added
                    'COVID19labsPartialMissing' = info_missing, #adding export for 'partial match' data
                    'TestingSitePartialMissing' = info_missing_site), #added third tab for testing site address
           path = glue('filepath.xlsx'),
           col_names = T)

# export final complete matched dataset for your records:
data.table::fwrite(mco_final,
                   glue('filepath.csv'))



# Send data support an email with file location:

# from
f <- '<email@email.com>'

# reply-to
rl <- c('<email@email.com>', '<email@email.com>')

# cc
cl <- c('<email@email.com>')

# subject
subj <- glue('{format(today1, "%m/%d/%Y")} Matching Results')

# body
bdy <- glue::glue(
  "
Hello!

Today's matching results are saved here: filepath.xlsx

There are:
    {nrow(info_added)} records in COVID19labsForUpload
    {nrow(info_missing)} records in COVID19labsPartialMissing
    {nrow(info_missing_site)} records in TestingSitePartialMissing


Thank you,
DQ Epis
")


# Sending email:
sendmailR::sendmail(f, rl, subj, bdy, cl, headers= list('Reply-To' = '<email@email.com>'), control= list(smtpServer = 'server.info'))

