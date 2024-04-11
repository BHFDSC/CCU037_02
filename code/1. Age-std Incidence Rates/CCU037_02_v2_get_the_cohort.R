# This script extracts tables from Databricks with an ethicity field
# Author: Marta Pineda

rm(list = ls())

library(dplyr)

# Setup Databricks connection --------------------------------------------------
library("DBI")
con <- DBI::dbConnect(odbc::odbc(),
                      "Databricks",
                      timeout = 60,
                      PWD = rstudioapi::askForPassword("Databricks personal access token:"))

# Get data ready ------------------------------------------------
fixed_code = "NO"     #Options: "NO"/"YES". Use "YES" when code labels from Databriks for " mydata$PrimaryCode " are correct and doesn't require an update:
fixed_11_code = "YES" #Options: "NO"/"YES". Use "YES" when code labels from Databriks for " mydata$ethnicity_11_group " are correct and doesn't require an update:
# Transfer data from DataBricks
mydata<- dbGetQuery(con,"SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_02_cohort") #updated_table
#END_OF_FOLLOW_UP_DATE, COVID_DATE (ie, startdate) + yes/no outcomes [except VTE,30CVD and hosp_admission]:
mydata2<- dbGetQuery(con,"SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_follow_up_availability") #dates
mydata = left_join(mydata,mydata2[c(1:4,9)]) #, by=c("NHS_NUMBER_DEID"="person_id_deid"))
#Dates of CVD outcomes and binary_variables
mydata2<- dbGetQuery(con,"SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_cvd_outcomes_with_dates_3") 
mydata = left_join(mydata,mydata2[-c(2:7,13)] ,by=c("NHS_NUMBER_DEID"="NHS_NUMBER_DEID")) #, by=c("NHS_NUMBER_DEID"="person_id_deid"))
rm(mydata2)
if (fixed_code == "NO"){
    # Tranform 1-9 codes to A-Z:
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '0','C',mydata$PrimaryCode_ethnicity)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '1','M',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '2','N',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '3','P',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '4','H',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '5','J',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '6','K',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '7','R',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '8','S',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == '9','Z',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity =='99','Z',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(mydata$PrimaryCode_ethnicity == 'X','Z',mydata$PrimaryCode)
    mydata$PrimaryCode <- ifelse(is.na(mydata$PrimaryCode_ethnicity),'Z',mydata$PrimaryCode)
  } else {mydata$PrimaryCode[mydata$PrimaryCode == "X"] <- "Z" }
  # table(mydata$PrimaryCode)
# Fix follow_up_days for people who died
mydata$follow_up_days[!is.na(mydata$REG_DATE_OF_DEATH)] <- (as.Date(mydata$REG_DATE_OF_DEATH[!is.na(mydata$REG_DATE_OF_DEATH)], "%Y%m%d")) - mydata$covid_date[!is.na(mydata$REG_DATE_OF_DEATH)]
# Exclude individuals whose date of death happened before date of covid diagnosis:
mydata <- mydata[mydata$follow_up_days >= 0,]
# Recalculate follow-up variables:
mydata$follow_up_all <- 1
mydata$follow_up_30d <- ifelse(mydata$follow_up_days >= 30,1,0) 
mydata$follow_up_90d <- ifelse(mydata$follow_up_days >= 90,1,0) 
mydata$follow_up_180d<- ifelse(mydata$follow_up_days >= 180,1,0) 
mydata$follow_up_1y  <- ifelse(mydata$follow_up_days >= 365,1,0) 
mydata$follow_up_2y  <- ifelse(mydata$follow_up_days >= 730,1,0) 
if (fixed_11_code == "NO"){
  #Fix individuals with ethnicity_11_group=Unknown but have an old notation in Primary Code: # table(mydata$ethnicity_5_group,mydata$ethnicity_qrisk3)
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "1"] <- "Black Caribbean"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "2"] <- "Black African"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "3"] <- "Other Black"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "4"] <- "Indian"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "5"] <- "Pakistani"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "6"] <- "Bangladeshi"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "7"] <- "Chinese"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "8"] <- "Other Ethnic Group"
  mydata$ethnicity_11_group[mydata$PrimaryCode_ethnicity == "W"] <- "Other Ethnic Group"
  }
  # table(mydata$PrimaryCode_ethnicity[mydata$ethnicity_11_group == "Unknown"]) 
#Ethnicity like QRISK3: White or not stated, Indian, Pakistani, Bangladeshi, Other Asian, Black Caribean, Black African, Chinese, Other Ethnic group
mydata$ethnicity_qrisk3 <- mydata$ethnicity_11_group
mydata$ethnicity_qrisk3[mydata$ethnicity_11_group == "White" | mydata$ethnicity_11_group == "Unknown"] <- "White or not stated"
mydata$ethnicity_qrisk3[mydata$ethnicity_11_group == "Mixed" | mydata$ethnicity_11_group == "Other Black" | mydata$ethnicity_11_group == "Other Ethnic Group"] <- "Other Ethnic group"
# Combine "schizophrenia","bipolardisorder","depression" in one variable =  severe_mentalillness
mydata$severe_mentalillness = ifelse(mydata$schizophrenia == 1 | mydata$bipolardisorder == 1 | mydata$depression == 1 ,1,0)

#Exclude patients that covid_diagnosis date is earlier than start_date
start_date = as.Date("2020-01-23")
mydata = mydata[mydata$covid_date>=start_date,]

#'Save data ready to use as rds ------------------------------------------------
#'Unsilence saveRDS() line when sure path and cohort name are correct in filename (beware no not overwrite another rds)
filename1 = paste("/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/CCU037_02_cohort", ".rds", sep="")
#saveRDS(mydata,filename1)

#LOAD data saved ---------------------------------------------------------------
mydata = readRDS("/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/CCU037_02_cohort.rds")

# Legend -----------------------------------------------------------------------
# Sex: 1=Male; 2=Female

# Outcome variables:
# "covid_death"                    (death within 30 days after covid diagnosis = YES/NO)
# "post_covid_cvd_event_90d"        OLD NAME: "post_covid_event_90_day"
# "post_covid_cvd_event_180d"       OLD NAME: "post_covid_cvd_event_180_day" 
# "post_covid_cvd_event_1y"         OLD_NAME: "post_covid_event_1_year"
# "post_covid_cvd_event_2y"         OLD NAME: "post_covid_cvd_event_2_years"

# Ethnicity variables:
# "ethnicity_5_group"           
# "ethnicity_11_group" 
# "PrimaryCode_ethnicity"
# "SNOMED_ethnicity

# Follow-up variables:
# "covid_date"            date format - date of covid diagnosis
# "REG_DATE_OF_DEATH"     char format (date of death) - is the same than covid_death date, also includes alll cause of death date. Included in the follow_up_date along with end data availability.
# "follow_up_date"        char format - earliest date: death or end data availability  (ie., 29th of June)
# "follow_up_90d"         patient have 90 days of follow-up  0/1 = No/Yes
# "follow_up_180d"        patient have 180 days of follow-up 0/1 = No/Yes
# "follow_up_1y"          patient have 1 year of follow-up   0/1 = No/Yes
# "follow_up_2y"          patient have 2 years of follow-up  0/1 = No/Yes
# "follow_up_days"        int format - days from covid diagnosis to "29th of June OR death(inlcuing covid_death if there the case)"

#Predictors
# "pre_covid_event_1_year" history of CVD events during the year prior to covid diagnosis
