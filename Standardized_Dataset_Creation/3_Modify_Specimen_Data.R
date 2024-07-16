#################################################################################################
#  SPECIMEN TABLE MODIFICATIONS 
#  MODIFYING INFORMATION FROM A SPECIFIC DATA TABLE WITHIN A DATABASE FOR R OBJECT CREATION PART 3
################################################################################################
################################################################################################
#
#  This R Markdown script is used to modify information from a specific WDRS table, SpecTable (which focuses on specimen collection data), 
#  in order to be made into an R Object standardized dataset. 
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


#### SpecTable modifications ###
#### Description: This code is used to modify SpecTable and save the R object
#### Author: Updated/Modified by Dianna Hergott 
#### Original script by Chunyi Wu and DIQA team


######Replace missing/invalid SPECIMEN__COLLECTION__DTTM with valid dates
SpecTable$INVESTIGATION_CREATE_DATE<-as.Date(SpecTable$INVESTIGATION_CREATE_DATE)


setDT(SpecTable)
cols<- c("SPECIMEN__COLLECTION__DTTM", "RESULT__REPORT__DTTM", "ANALYSIS__DTTM", "OBSERVATION__DTTM")

#if any date columns listed above are before pandemic_start_date (set in mise_en_place and usually 01-01-2020), replace date with NA
#updated filter to replace Sys.Date with INVESTIGATION_CREATE_DATE on 12-22-2022, DH
SpecTable[, (cols) :=  lapply(.SD, function(x) if_else(x < pandemic_start_date | x > INVESTIGATION_CREATE_DATE, NA_Date_, x)), .SDcols = cols]

# Create replacement date for when SPECIMEN_COLLECTION_DTTM is missing
# Hierarchy is: replace missing SPECIMEN_COLLECTION_DTTM with first non-missing among:
# OBSERVATION__DTTM, ANALYSIS__DTTM, RESULT__REPORT__DTTM, WELRS__PROCESSED__DTTM, INVESTIGATION_CREATE_DATE

SpecTable[, SPECIMEN__COLLECTION__DTTM2 := fcase(
  !is.na(SPECIMEN__COLLECTION__DTTM), SPECIMEN__COLLECTION__DTTM,
  !is.na(OBSERVATION__DTTM),  OBSERVATION__DTTM,
  !is.na(ANALYSIS__DTTM),  ANALYSIS__DTTM,
  !is.na(RESULT__REPORT__DTTM), RESULT__REPORT__DTTM,
  !is.na(WELRS__PROCESSED__DTTM), WELRS__PROCESSED__DTTM,
  !is.na(INVESTIGATION_CREATE_DATE), INVESTIGATION_CREATE_DATE,
  default = NA_Date_ )] 

SpecTable[,`:=`(SPECIMEN__COLLECTION__DTTM = SPECIMEN__COLLECTION__DTTM2)][,c('SPECIMEN__COLLECTION__DTTM2'):=NULL]

#Keep only labs from 2023 or later in SpecTable
SpecTable <- SpecTable[SPECIMEN__COLLECTION__DTTM >= filter_date,]

setDF(SpecTable)

#Keep post-mortem antigen and sequencing only tests from 2023 or later
#note that there should be no sequenciny only tests with dummy labs prior to 2023, but this filter will remove them just in case
SpecTable <- SpecTable %>% dplyr::filter(!(WDRS__TEST__PERFORMED == 'Antigen by Immunocytochemistry (autopsy specimen)' & CREATE_DATE < cste_filter_date))
SpecTable <- SpecTable %>% dplyr::filter(!(WDRS__TEST__PERFORMED == 'SARS CoV-2 positive by sequencing only' & CREATE_DATE < cste_filter_date))

#timecheck
log<- c(log, paste0("SpecTable created ", Sys.time(), " - pulled ", nrow(SpecTable), "records"))


#save
#save(SpecTable, file = paste0(Output_tables_wdrs,"filepath.RData"))
