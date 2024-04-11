# This script generates the baseline table for Incidence-prevalence paper 
# Author: Marta Pineda

rm(list = ls())

library(dplyr)

# Setup Databricks connection --------------------------------------------------
library("DBI")
connect_db = "YES"
token= ""
if (connect_db == "YES"){
  con <- DBI::dbConnect(odbc::odbc(),
                        "Databricks",
                        timeout = 60,
                        PWD = token)
}
# Legend -----------------------------------------------------------------------
# Sex: 1=Male; 2=Female

# LOAD DATABASE (---saved in qrisk syntax: CCU037_02_cohort---) ----------------
#CCU037_02_final_cohort // CCU037_02_cohort
mydata <- readRDS("/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/CCU037_02_cohort.rds")
mydata$PrimaryCode[mydata$PrimaryCode == "X"] <- "Z"

## Prepare time/waives for IR
#time (stratify date covid diagnosis by years [i.e., diagnosis in 2020, 2021 or 2022])
mydata$time[mydata$covid_date>="2020-01-23" & mydata$covid_date<="2020-12-31"] = "2020" #"23 Jan 2020" = 1st reported case of covid
mydata$time[mydata$covid_date>="2021-01-01" & mydata$covid_date<="2021-12-31"] = "2021" 
mydata$time[mydata$covid_date>="2022-01-01" & mydata$covid_date<="2022-06-29"] = "2022" #"2022-06-29" = end data availability, but there are no cases of covid diagnosis once the home LFT were not free (1st Apr 2022).
mydata$time = factor(mydata$time, levels = c("2020","2021","2022"), labels = c("2020","2021","2022"))
#waives: every 6 months
mydata$months[mydata$covid_date>="2020-01-23" & mydata$covid_date<="2020-06-30"] = "23 Jan 2020 to 30 Jun 2020" 
mydata$months[mydata$covid_date>="2020-07-01" & mydata$covid_date<="2020-12-31"] = "01 Jul 2020 to 31 Dec 2020"
mydata$months[mydata$covid_date>="2021-01-01" & mydata$covid_date<="2021-06-30"] = "01 Jan 2021 to 30 Jun 2021"
mydata$months[mydata$covid_date>="2021-07-01" & mydata$covid_date<="2021-12-31"] = "01 Jul 2021 to 31 Dec 2021"
mydata$months[mydata$covid_date>="2022-01-01" & mydata$covid_date<="2022-06-29"] = "01 Jan 2022 to 01 Apr 2022" #end data availability was "29 Jun 2022", but there are no cases of covid diagnosis once the home LFT were not free in the UK (1st Apr 2022).
mydata$months = factor(mydata$months, 
                       levels = c("23 Jan 2020 to 30 Jun 2020","01 Jul 2020 to 31 Dec 2020","01 Jan 2021 to 30 Jun 2021","01 Jul 2021 to 31 Dec 2021", "01 Jan 2022 to 01 Apr 2022"), 
                       labels = c("23 Jan 2020 to\n30 Jun 2020","01 Jul 2020 to\n31 Dec 2020","01 Jan 2021 to\n30 Jun 2021","01 Jul 2021 to\n31 Dec 2021", "01 Jan 2022 to\n01 Apr 2022") )

mydata$ONS_9eth = factor(mydata$PrimaryCode, levels = c("H","J","K","R","L","N","M","P","D","E","F","G","W","S","A","B","T","C", "Z"),
                         labels = c("Indian", "Pakistani", "Bangladeshi", "Chinese", "Other",
                                    "Black African", "Black Caribbean", "Other",
                                    "Mixed", "Mixed", "Mixed", "Mixed",
                                    "Other","Other",
                                    "White British","White other","White other","White other",                                      
                                    "Other" ))   #Option B: "Unknown/Not stated"
#Order 9cat
ord =c("Bangladeshi","Chinese","Indian","Pakistani","Black African","Black Caribbean","Mixed","Other","White British","White other")        
mydata$ONS_9eth = fct_relevel(mydata$ONS_9eth, ord)
#mydata$ONS_9eth = relevel(mydata$ONS_9eth, ref = "White British")

#Label 19 cat by names:
mydata$PrimaryCode_19cat = factor(mydata$PrimaryCode, 
                                  levels = c('H','J','K','R','L',
                                             'N','M','P',
                                             'D','E','F','G',
                                             'W','S',
                                             'A','B','T','C',
                                             'Z'),
                                  labels = c('Indian','Pakistani','Bangladeshi','Chinese','Any other Asian background',
                                             'African','Caribbean','Any other Black background',
                                             'White and Black Caribbean','White and Black African','White and Asian','Any other Mixed background',
                                             'Arab','Any other ethnic group',
                                             'British','Irish','Gypsy or Irish Traveller','Any other White background',
                                             'Unknown/Not stated'))

## Prepare variables for covid death outcomes ------------------------------------------------
#' Get IR covid-death at 28 and 90 days from covid diagnosis by time/waves: -----
#Death variable
mydata$death28d = ifelse(mydata$covid_death==1 & mydata$follow_up_days<=28, 1,0) #during next 28 dates from covid diagnosis
mydata$death90d = ifelse(mydata$covid_death==1 & mydata$follow_up_days<=90, 1,0) #manuscript definition + 90 days window
mydata = mydata[mydata$sex == "1",]

#Add pregnancy flag:
mydata2<- dbGetQuery(con,"SELECT NHS_NUMBER_DEID,preg_flag FROM dars_nic_391419_j3w9t_collab.ccu037_02_pregnancy_flag") #
mydata$preg_flag = ifelse(mydata$NHS_NUMBER_DEID %in% mydata2$NHS_NUMBER_DEID,1,0) #mydata = left_join(mydata,mydata2) #, by=c("NHS_NUMBER_DEID"="person_id_deid"))
rm(mydata2)

#Add vaccinaiton status
#Add vaccination flag:
mydata3<- dbGetQuery(con,"SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_02_vaccination_gdppr_men") 
data = inner_join(mydata[c(1,49)],mydata3[c(4:6)]) %>% filter(covid_date>vaccination_dose1_date) #mydata[c(1,49)] = Patient_ID and covid_date
data$vaccination_flag = 1

mydata = left_join(mydata,data)   
mydata$vaccination_flag[is.na(mydata$vaccination_flag)] = 0
rm(mydata3,data)


#Limit to age group we are analyzing in IR and HR (30 to 100)
mydata = mydata[mydata$age >= 30 & mydata$age <= 100,]



# BASELINE TABLES tableone() ---------------------------------------------------
install.packages("tableone")
library(tableone) 

myVars = c("age","sex","IMD_quintile","LSOA","vaccination_flag","preg_flag","smoking","AF","CKD","diabetes","schizophrenia","bipolardisorder","depression","RA","anti_hypertensive_drugs","antipsychotic","erectiledysfunction",
           "autoimmune_liver_disease","cancer","copd","dementia","hypertension","alcohol_problems","alcoholic_liver_disease","anti_coagulant_drugs","anti_diabetic_drugs","anti_platelet_drugs","statins","obesity",
           "osteoporosis","fracture_of_hip","fracture_of_wrist","pre_covid_cvd_event","pre_covid_cvd_event_1y", "death28d","death90d","post_covid_cvd_event_30d","post_covid_cvd_event_1y")

catVars = c("ethnicity_5_group","ONS_9eth","PrimaryCode_19cat","sex","preg_flag","vaccination_flag","IMD_quintile","LSOA","smoking","AF","CKD","diabetes","schizophrenia","bipolardisorder","depression","RA","anti_hypertensive_drugs","antipsychotic","erectiledysfunction",
            "autoimmune_liver_disease","cancer","copd","dementia","hypertension","alcohol_problems","alcoholic_liver_disease","anti_coagulant_drugs","anti_diabetic_drugs","anti_platelet_drugs","statins","obesity","osteoporosis",
            "fracture_of_hip","fracture_of_wrist","pre_covid_cvd_event","pre_covid_cvd_event_1y", "death28d","death90d","post_covid_cvd_event_30d","post_covid_cvd_event_1y")
            

tab6 <- CreateTableOne(vars = myVars, strata = "ethnicity_5_group" , data = mydata[mydata$sex != 'Unknow',], factorVars = catVars)
tab6Mat <- print(tab6, exact = "stage", quote = FALSE, noSpaces = TRUE, printToggle = FALSE)
write.table (tab6Mat , "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/baseline_tables_IR_manuscript/baselinetable_6gr_tableone_MPM.csv", col.names = T, row.names=T, append= F, sep=',')

tab9 <- CreateTableOne(vars = myVars, strata = "ONS_9eth" , data = mydata[mydata$sex != 'Unknow',], factorVars = catVars)
tab9Mat <- print(tab9, exact = "stage", quote = FALSE, noSpaces = TRUE, printToggle = FALSE)
write.table (tab9Mat , "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/baseline_tables_IR_manuscript/baselinetable_9gr_tableone_MPM.csv", col.names = T, row.names=T, append= F, sep=',')


tab19 <- CreateTableOne(vars = myVars, strata = "PrimaryCode_19cat" , data = mydata[mydata$sex != 'Unknow',], factorVars = catVars)
tab19Mat <- print(tab19, exact = "stage", quote = FALSE, noSpaces = TRUE, printToggle = FALSE)
write.table (tab19Mat , "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/baseline_tables_IR_manuscript/baselinetable_18gr_tableone_MPM.csv", col.names = T, row.names=T, append= F, sep=',')


#Variables that need attention for low number of individuals!
    #+factor(SLE) # excluded
    
    #9 groups:
    #To low number of individuals <5#  
    # + factor(alcoholic_liver_disease)

    #19 groups:
    #To low number of individuals <5#  
    # + factor(alcoholic_liver_disease) 
    # + factor(autoimmune_liver_disease)
    #Low n for Unknown in factor(IMD_quintile) 

#GO To "/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Clean R codes" for coding to export save tables (i.e., applying round rules...) 

#==============================================================================#


