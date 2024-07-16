################################################################################################
#PART 2: IMMUNIZATION DATABASE MATCHING
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
#  **This script addresses the second part of this process: linking positive COVID-19 cases from WDRS (the WA State database) 
#   with missing address/phone information to the Immunization Information System (IIS) in order to obtain address and phone numbers 
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
#
#
################################################################################################
################################################################################################

pacman::p_load(dplyr, 
               stringr, 
               stringdist, 
               data.table, 
               qs)

today <- format(Sys.Date(), '%Y%m%d')

#funs:
'%ni%' <- function(x, table) match(x, table, nomatch = -1) < 0
empty_as_na <- function(x) ifelse(trimws(x) == '', NA, x)

#these filepaths go to the functions that we just created
source('filepath/badPhone.R')
source('filepath/soundexR.R')

# *********************************************************
# Standardize variables/names in mco dataset for matching
# *********************************************************
#

# Read in missing contact data pulled earlier:
mco <- readRDS(paste0('filepath/mco_', today, '.RDS'))

#Converts DOBs to character class:
mco$BIRTH_DATE <- as.character(mco$BIRTH_DATE) 

# Remove empty records;
mco <- filter(mco, !is.na(CASE_ID))

# REMOVE PUNCTUATION AND STANDARDIZE CAPITALIZATION;
WDRSF <- mco %>%
  mutate(FNAME = toupper(FIRST_NAME),
         LNAME = toupper(LAST_NAME),
         MNAME = toupper(MIDDLE_NAME)) %>% 
  mutate(FNAME = trimws(gsub('[^[:alpha:]]+', ' ', FNAME)),
         LNAME = trimws(gsub('[^[:alpha:]]+', ' ', LNAME)),
         MNAME = trimws(gsub('[^[:alpha:]]+', ' ', MNAME)))

# Assign soundex values for first and last name
WDRSF <- WDRSF %>%
  mutate(FNAME_SDX = soundexR(FNAME),
         LNAME_SDX = soundexR(LNAME))

# Select only needed vars
WDRSF <- WDRSF %>% 
  select(CASE_ID, FNAME, LNAME, MNAME, FNAME_SDX, LNAME_SDX, LAST_NAME, FIRST_NAME, MIDDLE_NAME, BIRTH_DATE, Tag_1) %>% 
  distinct()


# **********************************************************
# *xMatch WDRS to IIS
# **********************************************************;
### Block 1: Identify exact matches on name and DOB;

# Read in IIS data:
iisf <- qs::qread('filepath/updated_iis.qs')

setDT(iisf)
iisf[ , DOBD := as.character(DOBD)] #convert dob to character for matching w/WDRS cases


# Create obj with exact matches:
setDT(WDRSF)
EXACT <- WDRSF[iisf, on=c(FNAME='FNAMED', LNAME='LNAMED', BIRTH_DATE='DOBD'), nomatch=NULL]
EXACT[, c('FNAME_SDX', 'LNAME_SDX') := NULL]
#this part matches only on First Name, Last Name, and DOB. First and Last Names are exact matches whereas DOB is a fuzzy match

# 01/11/23 PC issue found; need to dedup multiple exact matches
# Dedup based on most recent update:
setorder(EXACT, CASE_ID, -UPDATE_DATE) # sort by CASE_ID and descending UPDATE_DATE
EXACT <- EXACT[!duplicated(EXACT, by = 'CASE_ID')] # remove later UPDATE_DATE's within same CASE_ID

# Create new WDRS dataset that excludes cases with matched records for Block 2: Fuzzy matches
WDRSF <- WDRSF[CASE_ID %ni% EXACT$CASE_ID, ]

### Block 2: Identify fuzzy matches on name and DOB;
#
### Exact DOB & inexact name matching:
INEXACT_NAME1 <- WDRSF[iisf, on=c(FNAME_SDX='FNAMED_SDX', LNAME_SDX='LNAMED_SDX', BIRTH_DATE='DOBD'), nomatch=NULL]

INEXACT_NAME2 <- WDRSF[iisf, on=c(BIRTH_DATE='DOBD'), nomatch=NULL] #We will get a lot of matches with this

# Calculate Lev. Distance scores for first and last names:
INEXACT_NAME2[ , LEV_F := stringdist(FNAME, FNAMED)]
INEXACT_NAME2[ , LEV_L := stringdist(LNAME, LNAMED)]

if(nrow(INEXACT_NAME2)>0) {
  INEXACT_NAME2 <- INEXACT_NAME2 %>% 
    rowwise() %>% 
    mutate(NORM_LEV_F=LEV_F/max(nchar(FNAME), nchar(FNAMED)),
           NORM_LEV_L=LEV_L/max(nchar(LNAME), nchar(LNAMED))) %>% 
    ungroup()
  
  INEXACT_NAME2 <- INEXACT_NAME2 %>% filter((NORM_LEV_F < 0.4 & NORM_LEV_L < 0.2) | (NORM_LEV_F < 0.2 & NORM_LEV_L < 0.4)) 
}

### Exact name & inexact DOB matching:
INEXACT_DOB <- WDRSF[iisf, on=c(FNAME='FNAMED', LNAME='LNAMED'), nomatch=NULL]

# Calculate Lev. Distance scores for DOB:
INEXACT_DOB[ , LEV_DOB := stringdist(BIRTH_DATE, DOBD)]
INEXACT_DOB <- INEXACT_DOB[LEV_DOB == 1, ] #Pretty strict cutoff here. Only 1 number difference allowed. Allowing 2 differences brings in decent amounts of non-matches 

#### Combine inexact matching results
INEXACT_NAME1 <- INEXACT_NAME1 %>% 
  mutate(DOBD = BIRTH_DATE) %>% 
  select(CASE_ID, FNAME, FNAMED, LNAME, LNAMED, MNAME, MNAMED, BIRTH_DATE, DOBD, ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, Tag_1)

INEXACT_NAME2 <- INEXACT_NAME2 %>% 
  mutate(DOBD = BIRTH_DATE) %>% 
  select(CASE_ID, FNAME, FNAMED, LNAME, LNAMED, MNAME, MNAMED, BIRTH_DATE, DOBD, ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, Tag_1)

INEXACT_DOB <- INEXACT_DOB %>% 
  mutate(FNAMED = FNAME, LNAMED = LNAME) %>% 
  select(CASE_ID, FNAME, FNAMED, LNAME, LNAMED, MNAME, MNAMED, BIRTH_DATE, DOBD, ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, Tag_1)

INEXACT_NAME <- rbind(INEXACT_NAME1, INEXACT_NAME2) %>% distinct()
INEXACT_ALL <- rbind(INEXACT_NAME1, INEXACT_NAME2, INEXACT_DOB) %>% distinct()





#########################################################
### Manual review and match exclusion ###################
#########################################################

#### Prepare results to review:
REVIEW_INEXACT_NAME <- INEXACT_NAME %>% 
  as_tibble() %>% 
  filter(!(nchar(LNAMED) == 11 
           & LNAMED == substr(LNAME, 1, 11) 
           & FNAME == FNAMED)
  ) %>% # Skip review for cases where names are an exact match except last name has been truncated
  select(ID, CASE_ID:DOBD)

REVIEW_INEXACT_DOB <- INEXACT_DOB %>% as_tibble() %>% select(ID, CASE_ID:DOBD)

#### Print results for review:
print("Pairs to review (inexact names):")
print(REVIEW_INEXACT_NAME)

print("Pairs to review (inexact DOBs):")
print(REVIEW_INEXACT_DOB)



######### STOP HERE FOR MANUAL REVIEW OF MATCHED CASES #########


#IMPORTANT!!! exclude any fuzzy matches determined not to be true matches. Add new CASE_ID's below as needed.
excluded_case_id <- c(123456789) #e.g.: c(105626541, 105630374, etc.) exclude CASE_ID's if a case has no correct matches

# If there are multiple matches but only one ID should be kept, exlcude ID's: (shouldn't need to do this often) 
excluded_iis_id <- c(1234567) #e.g.: c(2508591, 7917991, etc.)



INEXACT_FINAL <- INEXACT_ALL %>% filter(ID %ni% excluded_iis_id & CASE_ID %ni% excluded_case_id)
INEXACT_FINAL <- INEXACT_FINAL %>% group_by(CASE_ID) %>% mutate(row=row_number()) %>% ungroup()
if(any(INEXACT_FINAL$row > 1)) warning('Multiple IIS matches for CASE_ID ',
                                       paste(unique(INEXACT_FINAL$CASE_ID[INEXACT_FINAL$row == 2]), collapse = ", "),
                                       '\nOnly the first match will be kept')

INEXACT_FINAL <- INEXACT_FINAL %>% filter(row == 1) #only keep the first row for each case_id if there are duplicates
INEXACT_FINAL$row <- NULL #remove row var which is no longer needed

#########################################################
### End manual review and match exclusion ###############
#########################################################




#clean up objects:
rm(INEXACT_NAME1, INEXACT_NAME2, INEXACT_NAME, INEXACT_DOB, INEXACT_ALL, REVIEW_INEXACT_NAME, REVIEW_INEXACT_DOB)

# Join iisf data to exact and fuzzy matches
setDT(INEXACT_FINAL)

INEXACT_FINAL <- INEXACT_FINAL[iisf, on=c('ID', 'FNAMED', 'MNAMED', 'LNAMED', 'DOBD'), nomatch=NULL]
EXACT <- iisf[EXACT %>% select(CASE_ID:BIRTH_DATE, ID, Tag_1), on='ID', nomatch=NULL]

# Combine iisf matches to create final dataset of matches to IIS;
IISMATCHES <- rbind(INEXACT_FINAL, EXACT)

# Clean IIS phone numbers:
#Remove duplicates, and move "good" numbers toward beginning of the PAT_PHONE_1, PAT_PHONE_2, PAT_PHONE_3, etc. columns by combining and re-separating columns
IISMATCHES <- IISMATCHES %>% 
  mutate(across(.cols = PAT_PHONE_1:PAT_PHONE_9, .fns = as.character)) %>% 
  mutate(PAT_PHONE_9 = case_when(is.na(PAT_PHONE_9) ~ NA_character_, str_detect(paste(PAT_PHONE_1, PAT_PHONE_2, PAT_PHONE_3, PAT_PHONE_4, PAT_PHONE_5, PAT_PHONE_6, PAT_PHONE_7, PAT_PHONE_8), fixed(PAT_PHONE_9)) ~ NA_character_, T ~ PAT_PHONE_9),
         PAT_PHONE_8 = case_when(is.na(PAT_PHONE_8) ~ NA_character_, str_detect(paste(PAT_PHONE_1, PAT_PHONE_2, PAT_PHONE_3, PAT_PHONE_4, PAT_PHONE_5, PAT_PHONE_6, PAT_PHONE_7), fixed(PAT_PHONE_8)) ~ NA_character_, T ~ PAT_PHONE_8),
         PAT_PHONE_7 = case_when(is.na(PAT_PHONE_7) ~ NA_character_, str_detect(paste(PAT_PHONE_1, PAT_PHONE_2, PAT_PHONE_3, PAT_PHONE_4, PAT_PHONE_5, PAT_PHONE_6), fixed(PAT_PHONE_7)) ~ NA_character_, T ~ PAT_PHONE_7),
         PAT_PHONE_6 = case_when(is.na(PAT_PHONE_6) ~ NA_character_, str_detect(paste(PAT_PHONE_1, PAT_PHONE_2, PAT_PHONE_3, PAT_PHONE_4, PAT_PHONE_5), fixed(PAT_PHONE_6)) ~ NA_character_, T ~ PAT_PHONE_6),
         PAT_PHONE_5 = case_when(is.na(PAT_PHONE_5) ~ NA_character_, str_detect(paste(PAT_PHONE_1, PAT_PHONE_2, PAT_PHONE_3, PAT_PHONE_4), fixed(PAT_PHONE_5)) ~ NA_character_, T ~ PAT_PHONE_5),
         PAT_PHONE_4 = case_when(is.na(PAT_PHONE_4) ~ NA_character_, str_detect(paste(PAT_PHONE_1, PAT_PHONE_2, PAT_PHONE_3), fixed(PAT_PHONE_4)) ~ NA_character_, T ~ PAT_PHONE_4),
         PAT_PHONE_3 = case_when(is.na(PAT_PHONE_3) ~ NA_character_, str_detect(paste(PAT_PHONE_1, PAT_PHONE_2), fixed(PAT_PHONE_3)) ~ NA_character_, T ~ PAT_PHONE_3),
         PAT_PHONE_2 = case_when(PAT_PHONE_2 == PAT_PHONE_1 ~ NA_character_, T ~ PAT_PHONE_2)) %>%
  #Convert bad numbers to NA:
  mutate(across(starts_with("PAT_PHONE_"), function(x) gsub('\\D+', '', x))) %>% 
  mutate(across(starts_with("PAT_PHONE_"), badPhone)) %>%
  mutate(across(.cols = PAT_PHONE_1:PAT_PHONE_9, .fns = empty_as_na)) %>% 
  #Combine separated numbers back into one column:
  tidyr::unite("PAT_PHONE_ALL", paste0("PAT_PHONE_", 1:9), na.rm=TRUE, sep = ", ") %>%
  tidyr::separate(col = PAT_PHONE_ALL, into = paste0("Patient_Phone", 1:9), 
                  sep = ", ", remove = TRUE, extra = "drop", fill = "right") %>% 
  mutate(across(.cols = Patient_Phone1:Patient_Phone9, .fns = empty_as_na))


# Create final dataset of matches
IISMATCHES <- IISMATCHES %>% 
  mutate(SOURCE = "IIS",
         STREETF = STREET_ADDRESS,
         CITYF = CITY,
         COUNTYF = COUNTY,
         STATEF = STATE,
         ZIPF = ZIP,
         LASTUPDATE = UPDATE_DATE,
         Event_ID = CASE_ID,
         Last_Name = LNAME,
         First_Name = FNAME,
         #Birth_Date = DOB,
         Patient_Phone_Number = Patient_Phone1,
         Patient_Address = STREETF,
         Patient_City = CITYF,
         IIS_COUNTY = toupper(COUNTYF),
         Patient_State = STATEF,
         Patient_Zipcode = ZIPF,
         Info_on_ordering_provider = ' ',
         submitter_name_phone = ' ',
         Last_updated_on_Accurint = ' ',
         Last_updated_on_IIS = ' ',
         Comment = ' '
         
  )

# **Recode Counties if WA Counties;
# ** TSF 11/11/21: added corrections for truncated counties observed in previous proc freq step here; 
# ** Added truncated PACI to Pacific County - mm, 12/22/21; 
# ** Added truncated CLAL to Clallam County, WHIT to Whitman, SKAM to Skamaish- mm, 12/28/21; 
IISMATCHES <- IISMATCHES %>% 
  mutate(Patient_County = case_when(IIS_COUNTY == 'ASOTIN' ~ 'Asotin County',
                                    IIS_COUNTY %in% c('ADAMS', 'ADAM', 'ADA') ~ 'Adams County',
                                    IIS_COUNTY %in% c('BENTON', 'BENT') ~ 'Benton County',
                                    IIS_COUNTY %in% c('CHELAN', 'CHEL') ~ 'Chelan County',
                                    IIS_COUNTY %in% c('CLALLAM', 'CLAL') ~ 'Clallam County',
                                    IIS_COUNTY %in% c( 'CLARK', 'CLAR') ~ 'Clark County',
                                    IIS_COUNTY == 'COLUMBIA' ~ 'Columbia County',
                                    IIS_COUNTY %in% c( 'COWLITZ', 'COWL') ~ 'Cowlitz County',
                                    IIS_COUNTY %in% c('DOUGLAS', 'DOUG') ~ 'Douglas County',
                                    IIS_COUNTY == 'FERRY' ~ 'Ferry County',
                                    IIS_COUNTY %in% c('FRANKLIN', 'FRAN') ~ 'Franklin County',
                                    IIS_COUNTY == 'GARFIELD' ~ 'Garfield County',
                                    IIS_COUNTY %in% c('GRANT', 'GRAN') ~ 'Grant County',
                                    grepl('GRAYS HA|GREYS HA|^GRAY$', IIS_COUNTY) ~ 'Grays Harbor County',
                                    IIS_COUNTY %in% c( 'ISLAND', 'ISLA') ~ 'Island County',
                                    IIS_COUNTY %in% c('JEFFERSON', 'JEFF') ~ 'Jefferson County',
                                    IIS_COUNTY == 'KING' ~ 'King County',
                                    IIS_COUNTY %in% c( 'KITSAP', 'KITS') ~ 'Kitsap County',
                                    IIS_COUNTY %in% c( 'KITTITAS', 'KITT') ~ 'Kittitas County',
                                    IIS_COUNTY == 'KLICKITAT' ~ 'Klickitat County',
                                    IIS_COUNTY %in% c( 'LEWIS', 'LEWI') ~ 'Lewis County',
                                    IIS_COUNTY == 'LINCOLN' ~ 'Lincoln County',
                                    IIS_COUNTY %in% c( 'MASON', 'MASO') ~ 'Mason County',
                                    IIS_COUNTY %in% c( 'OKAN', 'OKANOGAN') ~ 'Okanogan County',
                                    IIS_COUNTY == 'PACI' ~ 'Pacific County',
                                    IIS_COUNTY == 'PACIFIC' ~ 'Pacific County',
                                    IIS_COUNTY %in% c('PEND OREILLE', 'PEND') ~ 'Pend Oreille County',
                                    IIS_COUNTY %in% c('PEND OREI', 'PEND OREILL') ~ 'Pend Oreille County',
                                    IIS_COUNTY %in% c( 'PIERCE', 'PIER') ~ 'Pierce County',
                                    IIS_COUNTY %in% c( 'SAN JUAN', 'SAN ') ~ 'San Juan County',
                                    IIS_COUNTY %in% c('SKAGIT', 'SKAG') ~ 'Skagit County',
                                    IIS_COUNTY %in% c('SKAMANIA', 'SKAM') ~ 'Skamania County',
                                    IIS_COUNTY %in% c( 'SNOHOMISH', 'SNOHO','SNOH') ~ 'Snohomish County',
                                    IIS_COUNTY %in% c( 'SPOKANE', 'SPOK') ~ 'Spokane County',
                                    IIS_COUNTY %in% c( 'STEVENS', 'STEV') ~ 'Stevens County',
                                    IIS_COUNTY %in% c( 'THURSTON', 'THUR') ~ 'Thurston County',
                                    IIS_COUNTY == 'WAHKIAKUM' ~ 'Wahkiakum County',
                                    IIS_COUNTY == 'WALLA WALLA' ~ 'Walla Walla County',
                                    IIS_COUNTY %in% c('WALLA WALL', 'WALLA WAL') ~ 'Walla Walla County',
                                    IIS_COUNTY %in% c( 'WHATCOM', 'WHAT') ~ 'Whatcom County',
                                    IIS_COUNTY %in% c('WHITMAN', 'WHIT') ~ 'Whitman County',
                                    IIS_COUNTY %in% c( 'YAKIMA', 'YAKI') ~ 'Yakima County',
                                    T ~ COUNTYF),
         IIS_COUNTY = NULL)

IISMATCHES$BIRTH_DATE <- as.Date(IISMATCHES$BIRTH_DATE)


# *Create a final dataset of matches with variables ordered to original line list;
MATCHESF <- IISMATCHES %>% 
  select(Event_ID, 
         Last_Name, 
         First_Name, 
         Birth_Date = BIRTH_DATE, 
         Patient_Phone_Number, 
         Patient_Address, 
         Patient_City, 
         Patient_County, 
         Patient_State, 
         Patient_Zipcode, 
         Info_on_ordering_provider, 
         submitter_name_phone, 
         Last_updated_on_Accurint, 
         Last_updated_on_IIS, 
         Comment,
         Patient_Phone2,
         Patient_Phone3,
         Patient_Phone4,
         Patient_Phone5)

# Save IIS matches for use in the final Part 3 script:
saveRDS(MATCHESF, paste0('filepath/match_IIS_', today, '.rds'))