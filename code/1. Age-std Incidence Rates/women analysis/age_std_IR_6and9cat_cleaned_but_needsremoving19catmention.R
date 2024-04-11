# This script uses a table saved in 'CCU037_02_v1_QRisk_model.R', which loads tables from Databricks  ('CCU037_02_cohort' plus two other, and with few fixed/modified changes)
# Author: Marta Pineda

library(epiR); library(plyr); library(dplyr); library(ggplot2); library(forcats)

rm(list = ls())
run = "NO"      #Options: "YES" or "NO" = for running the checks for the df/df1/df2 tables (see that the IR estimates are correctly stratified by cheking the last row values)

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

# LOAD DATABASE (---saved in qrisk syntax: CCU037_02_cohort---) -----------------------------------
#CCU037_02_final_cohort // CCU037_02_cohort
mydata <- readRDS("/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/CCU037_02_cohort.rds")
mydata$PrimaryCode[mydata$PrimaryCode == "X"] <- "Z"

# CENSOR DATA UNTIL End of Free "Lateral flow test kits" for general population
#dim(mydata[mydata$covid_date<"2022-04-01",]) # No COVID diagnosis made after 1st April 2022.


# Prepare data ----------------------------------------------------------------- 
## Prepare general variables ---------------------------------------------------
## Prepare time/waives/months for IR 
#time (stratify date covid diagnosis by years [i.e., diagnosis in 2020, 2021 or 2022])
mydata$time[mydata$covid_date>="2020-01-23" & mydata$covid_date<="2020-12-31"] = "2020" #"23 Jan 2020" = 1st reported case of covid
mydata$time[mydata$covid_date>="2021-01-01" & mydata$covid_date<="2021-12-31"] = "2021" 
mydata$time[mydata$covid_date>="2022-01-01" & mydata$covid_date<="2022-06-29"] = "2022" #"2022-06-29" = end data availability, but there are no cases of covid diagnosis once the home LFT were not free (1st Apr 2022).
mydata$time = factor(mydata$time, levels = c("2020","2021","2022"), labels = c("2020","2021","2022"))
#waives (stratify date covid diagnosis by year bimestesters [i.e., every 6 months of the year])
mydata$waives[mydata$covid_date>="2020-01-23" & mydata$covid_date<="2020-08-31"] = "23 Jan 2020 to 31 Aug 2020" 
mydata$waives[mydata$covid_date>="2020-09-01" & mydata$covid_date<="2020-12-31"] = "01 Sep 2020 to 31 Dec 2020"
mydata$waives[mydata$covid_date>="2021-01-01" & mydata$covid_date<="2021-08-31"] = "01 Jan 2021 to 31 Aug 2021"
mydata$waives[mydata$covid_date>="2021-09-01" & mydata$covid_date<="2021-12-31"] = "01 Sep 2021 to 31 Dec 2021"
mydata$waives[mydata$covid_date>="2022-01-01" & mydata$covid_date<="2022-06-29"] = "01 Jan 2022 to 01 Apr 2022" #end data availability was "29 Jun 2022", but there are no cases of covid diagnosis once the home LFT were not free in the UK (1st Apr 2022).
mydata$waives = factor(mydata$waives, 
                       levels = c("23 Jan 2020 to 31 Aug 2020","01 Sep 2020 to 31 Dec 2020","01 Jan 2021 to 31 Aug 2021","01 Sep 2021 to 31 Dec 2021", "01 Jan 2022 to 01 Apr 2022"), 
                       labels = c("23 Jan 2020 to\n31 Aug 2020","01 Sep 2020 to\n31 Dec 2020","01 Jan 2021 to\n31 Aug 2021","01 Sep 2021 to\n31 Dec 2021", "01 Jan 2022 to\n01 Apr 2022") )
length(mydata$covid_date[mydata$covid_date<as.Date("2020-01-23")]) #individuals with covid_date < 23 April 2020
start_date = as.Date("2020-01-23")


#every 6 months
mydata$months[mydata$covid_date>="2020-01-23" & mydata$covid_date<="2020-06-30"] = "23 Jan 2020 to 30 Jun 2020" 
mydata$months[mydata$covid_date>="2020-07-01" & mydata$covid_date<="2020-12-31"] = "01 Jul 2020 to 31 Dec 2020"
mydata$months[mydata$covid_date>="2021-01-01" & mydata$covid_date<="2021-06-30"] = "01 Jan 2021 to 30 Jun 2021"
mydata$months[mydata$covid_date>="2021-07-01" & mydata$covid_date<="2021-12-31"] = "01 Jul 2021 to 31 Dec 2021"
mydata$months[mydata$covid_date>="2022-01-01" & mydata$covid_date<="2022-06-29"] = "01 Jan 2022 to 01 Apr 2022" #end data availability was "29 Jun 2022", but there are no cases of covid diagnosis once the home LFT were not free in the UK (1st Apr 2022).
mydata$months = factor(mydata$months, 
                       levels = c("23 Jan 2020 to 30 Jun 2020","01 Jul 2020 to 31 Dec 2020","01 Jan 2021 to 30 Jun 2021","01 Jul 2021 to 31 Dec 2021", "01 Jan 2022 to 01 Apr 2022"), 
                       labels = c("23 Jan 2020 to\n30 Jun 2020","01 Jul 2020 to\n31 Dec 2020","01 Jan 2021 to\n30 Jun 2021","01 Jul 2021 to\n31 Dec 2021", "01 Jan 2022 to\n01 Apr 2022") )

## Outcomes as numeric:
outc_list <- names(mydata[c(41,43,46)])
for (i in 1:length(outc_list)){
  mydata[[outc_list[i]]] <- as.numeric(as.character(mydata[[outc_list[i]]])) }

## Ethnic groups
#'Legend labels for ethnicity as ONS https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/updatingethniccontrastsindeathsinvolvingthecoronaviruscovid19englandandwales/10january2022to16february2022
{ #L="Any other Asian background", ONS is "Other"
  #P="Any other Black background", ONS is "Other"
  #D="White and Black Caribbean", E="White and Black African", F="White and Asian", G="Any other Mixed background", ONS is "Mixed"
  #W="Arab",S="Any other ethnic group", ONS is "Other"
  #B="Irish", T="Gypsy or Irish Traveller",C="Any other White background" ONS is "White other"
  #Z = "Unknown/Not stated" --> I'm unsure, should go to Others or keep it as it is? 
}
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
#mydata$PrimaryCode_19cat = relevel(mydata$PrimaryCode_19cat, ref = "British")

#Mapping from ONS_9eth and Primary Codes_19cat to 6-high level groups
eth_mapping = data.frame(names =c("Asian or Asian British","Black or Black British","Mixed","Other Ethnic Group","White","Unknown",                                      #6cat  
                                  "Bangladeshi","Chinese","Indian","Pakistani","Black African","Black Caribbean","Mixed","Other","White British","White other",          #9cat
                                  "Pakistani","Bangladeshi","Chinese","Indian","Any other Asian background","African","Caribbean","Any other Black background",          #18cat
                                  "White and Asian","White and Black African","White and Black Caribbean","Any other Mixed background","Arab","Any other ethnic group",
                                  "British","Irish","Gypsy or Irish Traveller","Any other White background","Unknown/Not stated"),
                         mapping_6cat = c("Asian or Asian British","Black or Black British","Mixed","Other Ethnic Group","White","Unknown",                                      #6cat            
                                          rep("Asian or Asian British",4),rep("Black or Black British",2),"Mixed","Other Ethnic Group",rep("White",2),                          #9cat
                                          rep("Asian or Asian British",5),rep("Black or Black British",3),rep("Mixed",4),rep("Other Ethnic Group",2),rep("White",4),"Unknown"), #18cat
                         classification = c( rep("6cat",6), rep("9cat",10), rep("19cat",19) ))

#Colours orders needs to be changed if relevel() syntax are run

#colours = list(ethnicity_5_group = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BA38"),
#               ONS_9eth = c(rep("#F8766D",4),"#619CFF",rep("#F564E3",2),"#B79F00","#00BFC4"),
#               PrimaryCode_19cat = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BFC4",4),"#00BA38") )

#colours_labels = list(ethnicity_5_group = data.frame( code = names(table(mydata$ethnicity_5_group)), colours = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BFC4","#00BA38") ),
#                               ONS_9eth = data.frame( code = names(table(mydata$ONS_9eth)) , colours = c(rep("#F8766D",4),"#619CFF",rep("#F564E3",2),"#B79F00",rep("#00BA38",2)) ),
#                      PrimaryCode_19cat = data.frame( code = names(table(mydata$PrimaryCode_19cat)) , colours = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BA38",4),"#00BFC4") ) )
#Mortality models only adjusted by age, for 6_cat and 19_cat: ------------------
#library(survival); library(dplyr); library(forcats); library(ggplot2)
ethnicity = c("ethnicity_5_group","ONS_9eth","PrimaryCode_19cat")
colours_labels = list(ethnicity_5_group = data.frame( code = names(table(mydata$ethnicity_5_group)), color = c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00","#F8766D") ),
                      ONS_9eth = data.frame( code = names(table(mydata$ONS_9eth)) , color = c(rep("#F564E3",4),rep("#619CFF",2),"#00BFC4","#00BA38",rep("#F8766D",2)) ),
                      PrimaryCode_19cat = data.frame( code = names(table(mydata$PrimaryCode_19cat)) , color = c( rep("#F564E3",5),rep("#619CFF",3), rep("#00BFC4",4),rep("#00BA38",2), rep("#F8766D",4),"#B79F00") ) )




#Age categories:
#Age decils
mydata$catAGE <- with(mydata, cut( as.numeric(age), breaks = c(-1,17,seq(29.999, 89.999, by = 10),116), 
                                   labels = c('0-17','18-29','30-39','40-49', '50-59', '60-69', '70-79', '80-89', '90+')))
#Age by ESP2013 age groups:
mydata$ageESP2013 <- with(mydata, cut( as.numeric(age), breaks = c(-1,19.999,seq(24.999, 89.999, by = 5),116), 
                                       labels = c( '18-19','20-24','25-29','30-34','35-39','40-44','45-49', '50-54','55-59','60-64','65-69',
                                                   '70-74','75-79','80-84','85-89','90+')))

ESP2013 <- data.frame(Age_group = c("<1","1-4","5-9","10-14","15-19","20-24","25-29","30-34","35-39","40-44","45-49","50-54","55-59","60-64","65-69","70-74","75-79","80-84","85-89","90+"),
                      ESP = c(1000,4000,5500,5500,5500,6000,6000,6500,7000,7000,7000,7000,6500,6000,5500,5000,4000,2500,1500,1000))
#for calculating upper age band of 95+ (instead of 90+) 
run_95 = "NO"
if (run_95 == "YES"){
  mydata$ageESP2013 <- with(mydata, cut( as.numeric(age), breaks = c(-1,19.999,seq(24.999, 94.999, by = 5),116), 
                                         labels = c( '18-19','20-24','25-29','30-34','35-39','40-44','45-49', '50-54','55-59','60-64','65-69',
                                                     '70-74','75-79','80-84','85-89','90-94','95+')))
  ESP2013 <- data.frame(Age_group = c("<1","1-4","5-9","10-14","15-19","20-24","25-29","30-34","35-39","40-44","45-49","50-54","55-59","60-64","65-69","70-74","75-79","80-84","85-89","90-94","95+"),
                        ESP = c(1000,4000,5500,5500,5500,6000,6000,6500,7000,7000,7000,7000,6500,6000,5500,5000,4000,2500,1500,800,200))
}

#' End prepare general variables '############################################################

#' Censor analysis up to 31 Dec 2021
#dim(mydata)
# mydata = mydata[mydata$covid_date<="2021-12-31",]




## Prepare variables for covid death outcomes ------------------------------------------------
#' Get IR covid-death at 28 and 90 days from covid diagnosis by time/waves: -----
#Death variable
mydata$death28d = ifelse(mydata$covid_death==1 & mydata$follow_up_days<=28, 1,0) #during next 28 dates from covid diagnosis
mydata$death90d = ifelse(mydata$covid_death==1 & mydata$follow_up_days<=90, 1,0) #manuscript definition + 90 days window
#Time daysto_death censored at 28 and 90 days
mydata$daysto_death28d = ifelse(mydata$follow_up_days>28,28,mydata$follow_up_days)
mydata$daysto_death90d = ifelse(mydata$follow_up_days>90,90,mydata$follow_up_days)
## Prepare variables for CVD outcomes --------------------------------------------------------
#' Get IR CVE at 30 and 365 days from covid diagnosis by time/waves: ------------
# Include event day for CVE and mantain follow_up_days for those who did not had an event
mydata$daysto_30dCVE     =  ifelse(mydata$post_covid_cvd_event_30d == 1, mydata$date_cvd_30d-mydata$covid_date,mydata$follow_up_days)
mydata$daysto_1yCVE      =  ifelse(mydata$post_covid_cvd_event_1y  == 1, mydata$date_cvd_1y-mydata$covid_date,mydata$follow_up_days)
# Censor the follow-up/the days t:o 1 year for covid death and 1yCVD, and 30d for 30dCVD:
mydata$daysto_30dCVE =  ifelse(mydata$daysto_30dCVE>30,  30, mydata$daysto_30dCVE)
mydata$daysto_1yCVE  =  ifelse(mydata$daysto_1yCVE>365,  365,mydata$daysto_1yCVE)

## Prepare lists for the IR loops, were each loop will measure the IR by the ethnic classification -------------------
#ethnicity = c("ethnicity_5_group","ONS_9eth","PrimaryCode_19cat")
outcomes_list = c("death28d","death90d",c(outc_list)[2:3])
day_to_event_var = c("daysto_death28d","daysto_death90d","daysto_30dCVE","daysto_1yCVE")
periods = c("time", "months")


sex_cat = names(table(mydata$sex))

#Include only individuals from 30 to 100 years old and columns of interest #exchange "waives" for "months"
columns_of_interest = c("NHS_NUMBER_DEID","sex","ageESP2013","ethnicity_5_group","ONS_9eth","PrimaryCode_19cat","time", "months",
                        "death28d","death90d",c(outc_list)[2:3],"daysto_death28d","daysto_death90d","daysto_30dCVE","daysto_1yCVE")

mydata2 =  mydata[mydata$age>=30 & mydata$age<=100,columns_of_interest]
age_cat = names(table(factor(mydata2$ageESP2013))) #include age_cat after age group exclusions

# GET Incidence Rates by -------------------------------------------------------
## 6 ethnicity groups ---------------------------------------------------------
### 6 ONS_classification (ie.,high level categories)
eth_cat = names(table(mydata$ethnicity_5_group))
used_AGEcat = "ageESP2013"         #Alternatives: ageESP2013 / catAGE
used_ETHcat = "ethnicity_5_group"
groups      = "6 groups"
#'Start point: data frame with 6 ethnicity and outcomes
df = data.frame(classification = groups,outcome=rep(outcomes_list,each=length(eth_cat)), code=rep(eth_cat,length(outcomes_list)), sum_events="",sum_years="")
#df = do.call("rbind", replicate(length(age_cat), df, simplify = FALSE))
df = data.frame(df, AGE= rep(age_cat,each=length(df$code)))
#'Add sex and age (no time/waive)
df0= do.call("rbind", replicate(length(sex_cat), df, simplify = FALSE))
df0= data.frame(df0, SEX= rep(sex_cat,each=length(df$code)))
#'Add sex, age and time #levels(mydata2$time)
df1= do.call("rbind", replicate(length(levels(mydata2$time)), df0, simplify = FALSE))
df1= data.frame(df1, time= rep(levels(mydata2$time),each=length(df0$code)) )
#df1= df1[order(df1$time,df1$SEX),]
#'Add sex, age and waive #levels(mydata2$months)
df2=do.call("rbind", replicate(length(levels(mydata2$months)), df0, simplify = FALSE))
df2= data.frame(df2, time= rep(levels(mydata2$months),each=length(df0$code)) )
#'Add sex, age and waive #levels(mydata2$waive)
#df2=do.call("rbind", replicate(length(levels(mydata2$waive)), df0, simplify = FALSE))
#df2= data.frame(df2, time= rep(levels(mydata2$waive),each=length(df0$code)) )
#df2= df2[order(df2$SEX,df2$outcome,df2$time),] 
#'sum_events and sum_years as numeric
df0$sum_events<- as.numeric(df0$sum_events); df1$sum_events<- as.numeric(df1$sum_events); df2$sum_events<- as.numeric(df2$sum_events); 
df0$sum_years <- as.numeric(df0$sum_years);  df1$sum_years <- as.numeric(df1$sum_years);  df2$sum_years <- as.numeric(df2$sum_years); 
dim(df); dim(df0); dim(df1); dim(df2)
### GET df0 = age and sex ------
#'the silenced prints help seeing the loop structure
j=1
for (s in sex_cat){
  #print(s)
  for (age in age_cat){    
    #print(age)
    mydata_subset = mydata2[mydata2[used_AGEcat] == age & mydata2$sex == s ,]
    for (i in 1:length(outcomes_list)){
      #print(outcomes_list[i])
      for (e in eth_cat){
        mydata_subset2 = mydata_subset[mydata_subset[used_ETHcat] == e,]
        df0[j,4] <- sum(mydata_subset2[[outcomes_list[i]]])
        df0[j,5] <- sum(mydata_subset2[[day_to_event_var[i]]])/365  #From days to years
        print(j)
        j=j+1
      }
    }
  }
}
#'Manual check of the last two values of df0: _it works_
#run = "NO"
if (run == "YES"){
  print(tail(df0,1))
  print(sum(mydata2$post_covid_cvd_event_1y[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == "White" & mydata2$sex == "2"]))
  print(sum(mydata2$daysto_1yCVE[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == "White" & mydata2$sex == "2"])/365)  
}
#'Now get the IR
data = as.matrix(df0[(4:5)])
t = epi.conf(data, ctype = "inc.rate", method = "exact", N = 1000, design = 1, conf.level = 0.95) *1000
IR_table_6cat = cbind(df0,t)
#IR_table_6cat[c(8:10)] = round(IR_table_6cat[c(8:10)],3) #Scale: cases/1000 persons-year

### GET df1 = age, sex and time -----
#'the silenced prints help seeing the loop structure
j=1
used_period = periods[1]
for (p in levels(mydata2[[used_period]])){
  for (s in sex_cat){
    #print(s)
    for (age in age_cat){    
      #print(age)
      mydata_subset = mydata2[mydata2[used_AGEcat] == age & mydata2$sex == s & mydata2[used_period] == p,]
      for (i in 1:length(outcomes_list)){
        #print(outcomes_list[i])
        for (e in eth_cat){
          mydata_subset2 = mydata_subset[mydata_subset[used_ETHcat] == e,]
          df1[j,4] <- sum(mydata_subset2[[outcomes_list[i]]])
          df1[j,5] <- sum(mydata_subset2[[day_to_event_var[i]]])/365  #From days to years
          print(j)
          j=j+1
        }
      }
    }
  }
}
#'Manual check of the last two values of df1: _it works_
#run = "NO"
if (run == "YES"){
  print(tail(df1,1))
  print(sum(mydata2$post_covid_cvd_event_1y[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])) #e = "White"
  print(sum(mydata2$daysto_1yCVE[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])/365)        #p = "2022"
}
#'Now get the IR
data = as.matrix(df1[(4:5)])
t = epi.conf(data, ctype = "inc.rate", method = "exact", N = 1000, design = 1, conf.level = 0.95) *1000
IR1_table_6cat = cbind(df1,t)
#IR1_table_6cat[c(9:11)] = round(IR1_table_6cat[c(9:11)],3) #Scale: cases/1000 persons-year

### GET df2 = age, sex and waive/months ----
#'the silenced prints help seeing the loop structure
j=1
used_period = periods[2]
for (p in levels(mydata2[[used_period]])){
  for (s in sex_cat){
    #print(s)
    for (age in age_cat){    
      #print(age)
      mydata_subset = mydata2[mydata2[used_AGEcat] == age & mydata2$sex == s & mydata2[used_period] == p,]
      for (i in 1:length(outcomes_list)){
        #print(outcomes_list[i])
        for (e in eth_cat){
          mydata_subset2 = mydata_subset[mydata_subset[used_ETHcat] == e,]
          df2[j,4] <- sum(mydata_subset2[[outcomes_list[i]]])
          df2[j,5] <- sum(mydata_subset2[[day_to_event_var[i]]])/365  #From days to years
          print(j)
          j=j+1
        }
      }
    }
  }
}
#'Manual check of the last two values of df2: _it works_
#run = "NO"
if (run == "YES"){
  print(tail(df2,1))
  print(sum(mydata2$post_covid_cvd_event_1y[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])) #e = "White"
  print(sum(mydata2$daysto_1yCVE[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])/365)        #p = "Jan-Jun 2022"
}
#'Now get the IR
data = as.matrix(df2[(4:5)])
t = epi.conf(data, ctype = "inc.rate", method = "exact", N = 1000, design = 1, conf.level = 0.95) *1000
IR2_table_6cat = cbind(df2,t)
#IR2_table_6cat[c(9:11)] = round(IR2_table_6cat[c(9:11)],3) #Scale: cases/1000 persons-year


## 9 ethnicity groups (ONS_9eth) -----------------------------------------------
eth_cat = names(table(mydata$ONS_9eth))
used_AGEcat = "ageESP2013" #Alternative: ageESP2013 / catAGE
used_ETHcat = "ONS_9eth"
groups      = "10 groups"
#'Start point: data frame with 6 ethnicity and outcomes
df = data.frame(classification = groups,outcome=rep(outcomes_list,each=length(eth_cat)), code=rep(eth_cat,length(outcomes_list)), sum_events="",sum_years="")
#df = do.call("rbind", replicate(length(age_cat), df, simplify = FALSE))
df = data.frame(df, AGE= rep(age_cat,each=length(df$code)))
#'Add sex and age (no time/waive)
df0= do.call("rbind", replicate(length(sex_cat), df, simplify = FALSE))
df0= data.frame(df0, SEX= rep(sex_cat,each=length(df$code)))
#'Add sex, age and time #levels(mydata2$time)
df1= do.call("rbind", replicate(length(levels(mydata2$time)), df0, simplify = FALSE))
df1= data.frame(df1, time= rep(levels(mydata2$time),each=length(df0$code)) )
#df1= df1[order(df1$time,df1$SEX),]
#'Add sex, age and waive #levels(mydata2$months)
df2=do.call("rbind", replicate(length(levels(mydata2$months)), df0, simplify = FALSE))
df2= data.frame(df2, time= rep(levels(mydata2$months),each=length(df0$code)) )
#'Add sex, age and waive #levels(mydata2$waive)
#df2=do.call("rbind", replicate(length(levels(mydata2$waive)), df0, simplify = FALSE))
#df2= data.frame(df2, time= rep(levels(mydata2$waive),each=length(df0$code)) )
#df2= df2[order(df2$SEX,df2$outcome,df2$time),] 
#'sum_events and sum_years as numeric
df0$sum_events<- as.numeric(df0$sum_events); df1$sum_events<- as.numeric(df1$sum_events); df2$sum_events<- as.numeric(df2$sum_events); 
df0$sum_years <- as.numeric(df0$sum_years);  df1$sum_years <- as.numeric(df1$sum_years);  df2$sum_years <- as.numeric(df2$sum_years); 
dim(df); dim(df0); dim(df1); dim(df2)
### GET df0 = age and sex ----
#'the silenced prints help seeing the loop structure
j=1
for (s in sex_cat){
  #print(s)
  for (age in age_cat){    
    #print(age)
    mydata_subset = mydata2[mydata2[used_AGEcat] == age & mydata2$sex == s ,]
    for (i in 1:length(outcomes_list)){
      #print(outcomes_list[i])
      for (e in eth_cat){
        mydata_subset2 = mydata_subset[mydata_subset[used_ETHcat] == e,]
        df0[j,4] <- sum(mydata_subset2[[outcomes_list[i]]])
        df0[j,5] <- sum(mydata_subset2[[day_to_event_var[i]]])/365  #From days to years
        print(j)
        j=j+1
      }
    }
  }
}

#'Manual check of the last two values of df0: _it works_
#run = "NO"
if (run == "YES"){
  print(tail(df0,1))
  print(sum(mydata2$post_covid_cvd_event_1y[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2"])) #e = "White other"
  print(sum(mydata2$daysto_1yCVE[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2"])/365)
}
#'Now get the IR
data = as.matrix(df0[(4:5)])
t = epi.conf(data, ctype = "inc.rate", method = "exact", N = 1000, design = 1, conf.level = 0.95) *1000
IR_table_9cat = cbind(df0,t)
#IR_table_9cat[c(8:10)] = round(IR_table_9cat[c(8:10)],3) #Scale: cases/1000 persons-year

### GET df1 = age, sex and time -----
#'the silenced prints help seeing the loop structure
j=1
used_period = periods[1]
for (p in levels(mydata2[[used_period]])){
  for (s in sex_cat){
    #print(s)
    for (age in age_cat){    
      #print(age)
      mydata_subset = mydata2[mydata2[used_AGEcat] == age & mydata2$sex == s & mydata2[used_period] == p,]
      for (i in 1:length(outcomes_list)){
        #print(outcomes_list[i])
        for (e in eth_cat){
          mydata_subset2 = mydata_subset[mydata_subset[used_ETHcat] == e,]
          df1[j,4] <- sum(mydata_subset2[[outcomes_list[i]]])
          df1[j,5] <- sum(mydata_subset2[[day_to_event_var[i]]])/365  #From days to years
          print(j)
          j=j+1
        }
      }
    }
  }
}
#'Manual check of the last two values of df1: _it works_
#run = "NO"
if (run == "YES"){
  print(tail(df1,1))
  print(sum(mydata2$post_covid_cvd_event_1y[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])) #e = "White other"
  print(sum(mydata2$daysto_1yCVE[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])/365)        #p = "2022"
}
#'Now get the IR
data = as.matrix(df1[(4:5)])
t = epi.conf(data, ctype = "inc.rate", method = "exact", N = 1000, design = 1, conf.level = 0.95) *1000
IR1_table_9cat = cbind(df1,t)
#IR1_table_9cat[c(9:11)] = round(IR1_table_9cat[c(9:11)],3) #Scale: cases/1000 persons-year

### GET df2 = age, sex and waive/months ----
#'the silenced prints help seeing the loop structure
j=1
used_period = periods[2]
for (p in levels(mydata2[[used_period]])){
  for (s in sex_cat){
    #print(s)
    for (age in age_cat){    
      #print(age)
      mydata_subset = mydata2[mydata2[used_AGEcat] == age & mydata2$sex == s & mydata2[used_period] == p,]
      for (i in 1:length(outcomes_list)){
        #print(outcomes_list[i])
        for (e in eth_cat){
          mydata_subset2 = mydata_subset[mydata_subset[used_ETHcat] == e,]
          df2[j,4] <- sum(mydata_subset2[[outcomes_list[i]]])
          df2[j,5] <- sum(mydata_subset2[[day_to_event_var[i]]])/365  #From days to years
          print(j)
          j=j+1
        }
      }
    }
  }
}
#'Manual check of the last two values of df2: _it works_
#run = "NO"
if (run == "YES"){
  print(tail(df2,1))
  print(sum(mydata2$post_covid_cvd_event_1y[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])) #e = "White other"
  print(sum(mydata2$daysto_1yCVE[mydata2[used_AGEcat] == "90+" & mydata2[used_ETHcat] == e & mydata2$sex == "2" & mydata2[used_period] == p])/365)        #p = "Jan-Jun 2022"
}
#'Now get the IR
data = as.matrix(df2[(4:5)])
t = epi.conf(data, ctype = "inc.rate", method = "exact", N = 1000, design = 1, conf.level = 0.95) *1000
IR2_table_9cat = cbind(df2,t)
#IR2_table_9cat[c(9:11)] = round(IR2_table_9cat[c(9:11)],3) #Scale: cases/1000 persons-year


#Remove temporary variables that helped getting the IR and release memory:
rm(data,t,df,df0,df1,df2,mydata_subset,mydata_subset2)
gc()


##Save censored IR tables (not age-std) ---------------------------------------------------------------
tail(IR_table_6cat)   #' IR by  6 eth, age, sex 
tail(IR1_table_6cat)  #' IR by  6 eth, age, sex & time
tail(IR2_table_6cat)  #' IR by  6 eth, age, sex & waives or months

head(IR_table_9cat)   #' IR by  9 eth  age, sex
head(IR1_table_9cat)  #' IR by  9 eth  age, sex & time
head(IR2_table_9cat)  #' IR by  9 eth, age, sex & waives or months

head(IR_table_18cat)  #' IR by 18 eth  age, sex
head(IR1_table_18cat) #' IR by 18 eth  age, sex & time
head(IR2_table_18cat) #' IR by 18 eth, age, sex & waives or months

#Censor tables to export out of the TRE:
#library(plyr)
#Create a cpy for the censor tables:
IR_table_6cat_censored = IR_table_6cat[c(1:3,6,7,4,8:10)];  IR1_table_6cat_censored = IR1_table_6cat[c(1:3,6:8,4,9:11)]; IR2_table_6cat_censored = IR2_table_6cat[,c(1:3,6:8,4,9:11)]; 
IR_table_9cat_censored = IR_table_9cat[c(1:3,6,7,4,8:10)];  IR1_table_9cat_censored = IR1_table_9cat[c(1:3,6:8,4,9:11)]; IR2_table_9cat_censored = IR2_table_9cat[,c(1:3,6:8,4,9:11)]; 
IR_table_18cat_censored=IR_table_18cat[,c(1:3,6,7,4,8:10)];IR1_table_18cat_censored=IR1_table_18cat[,c(1:3,6:8,4,9:11)]; IR2_table_18cat_censored=IR2_table_18cat[,c(1:3,6:8,4,9:11)]
#Include them into a list:
IR_list = list(IR_table_6cat_censored,IR_table_9cat_censored,IR_table_18cat_censored); 
names(IR_list) = c("IR_table_6cat_censored","IR_table_9cat_censored","IR_table_18cat_censored")
IR12_list = list(IR1_table_6cat_censored,IR1_table_9cat_censored,IR1_table_18cat_censored,  IR2_table_6cat_censored,IR2_table_9cat_censored,IR2_table_18cat_censored)
names(IR12_list)=c("IR1_table_6cat_censored","IR1_table_9cat_censored","IR1_table_18cat_censored",  "IR2_table_6cat_censored","IR2_table_9cat_censored","IR2_table_18cat_censored")
# Censor tables:
#'  censor cells that have counts <10! (no IR estimates)
#'  and round to the nearest multiple of 5
#'  Extra: round IR rates (non necessary)
#'  Change NA for "<10" (later than round as we change from num to char)
for (i in 1:length(IR_list)){
  IR_list[[i]][IR_list[[i]]$sum_events<10 & IR_list[[i]]$sum_events!=0,c(6)]  <- NA  #c(6) = $sum_events
  IR_list[[i]]$sum_events <- plyr::round_any( IR_list[[i]]$sum_events, 5, f = round) 
  IR_list[[i]][c(7:9)] <- round(IR_list[[i]][c(7:9)] ,3)             #Optional   #c(7:9)=IR[95CI]
  IR_list[[i]]$sum_events[is.na(IR_list[[i]]$sum_events)] <-"<10"
}
for (i in 1:length(IR12_list)){
  IR12_list[[i]][IR12_list[[i]]$sum_events<10 & IR12_list[[i]]$sum_events!=0,c(7)]  <- NA  #c(6) = $sum_events
  IR12_list[[i]]$sum_events <- plyr::round_any( IR12_list[[i]]$sum_events, 5, f = round) 
  IR12_list[[i]][c(8:10)] <- round(IR12_list[[i]][c(8:10)] ,3)         #Optional #c(8:10)=IR[95CI]
  IR12_list[[i]]$sum_events[is.na(IR12_list[[i]]$sum_events)] <-"<10"
}

# Syntax to save IR table ~ needs prior censor:
run_save_tables = "NO" #Options "NO"/"YES" default "NO" to avoid append overwriting)
years_restriction = "a"#"20-21" #delete or include a random character to "20-21" when including also 2023 results

if (run_save_tables == "YES"){
  if (years_restriction == "20-21") {path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/IR_2020_2021_subset/IR_eth&age&sex_censored.csv"} else {
    path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/IR_eth&age&sex_censored.csv"}
  for (i in 1:length(IR_list)){
    if (i == 1){write.table(IR_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(IR_list[[i]],  path_filename, col.names = F, row.names=F, append= T, sep=',')}
  }
  if (years_restriction == "20-21") {path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/IR_2020_2021_subset/IR_eth&age&sex&months_censored.csv"} else{
    path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/IR_eth&age&sex&months_censored.csv"}
  for (i in 1:length(IR12_list)){
    if (i == 1){write.table(IR12_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(IR12_list[[i]],  path_filename, col.names = F, row.names=F, append= T, sep=',')}
  }
}



#Calculate age-standardized IR tables ---------------------------------------------------------------------
#'Desired scale:  rates per 100,000 population
#'The provided IR are per 1,000 person-year, thus, to report 100,000 population, we only need to multiply by 100
scale_std = 100 
#by ethnicity, age and sex 
IR_list = list(IR_table_6cat, IR_table_9cat,IR_table_18cat); 
names(IR_list) = c("IR_table_6cat","IR_table_9cat","IR_table_18cat")
for (i in 1:length(IR_list)){
  #get age standardised columns per ESP2013 age group
  IR_list[[i]]<-  left_join(IR_list[[i]], ESP2013, by=c("AGE"="Age_group")) 
  IR_list[[i]]$age_std = ((IR_list[[i]]$sum_events/IR_list[[i]]$sum_years)*scale_std)*IR_list[[i]]$ESP
  #get age_std IR
  IR_list[[i]]  = IR_list[[i]] %>% 
    group_by(outcome,SEX,code) %>% 
    summarise(sum_sum_events = sum(sum_events,na.rm=T), sum_personyears = sum(sum_years,na.rm=T), sum_age_std = sum(age_std,na.rm=T),sum_ESP = sum(ESP,na.rm=T))
  IR_list[[i]]$IR_std   = IR_list[[i]]$sum_age_std/IR_list[[i]]$sum_ESP
  IR_list[[i]]$Low_std  = IR_list[[i]]$IR_std -1.96*(IR_list[[i]]$IR_std /sqrt(IR_list[[i]]$sum_sum_events))
  IR_list[[i]]$High_std = IR_list[[i]]$IR_std +1.96*(IR_list[[i]]$IR_std /sqrt(IR_list[[i]]$sum_sum_events))
}  
#by ethnicity, age, sex and period
IR12_list = list(IR1_table_6cat,IR1_table_9cat,IR1_table_18cat,  IR2_table_6cat,IR2_table_9cat,IR2_table_18cat)
names(IR12_list)=c("IR1_table_6cat","IR1_table_9cat","IR1_table_18cat",  "IR2_table_6cat","IR2_table_9cat","IR2_table_18cat")
for (i in 1:length(IR12_list)){
  #get age standardised columns per ESP2013 age group
  IR12_list[[i]]<-  left_join(IR12_list[[i]], ESP2013, by=c("AGE"="Age_group")) 
  IR12_list[[i]]$age_std = ((IR12_list[[i]]$sum_events/IR12_list[[i]]$sum_years)*scale_std)*IR12_list[[i]]$ESP
  #get age_std IR
  IR12_list[[i]]  = IR12_list[[i]] %>% 
    group_by(outcome,SEX,time,code) %>% 
    summarise(sum_sum_events = sum(sum_events,na.rm=T), sum_personyears = sum(sum_years,na.rm=T), sum_age_std = sum(age_std,na.rm=T),sum_ESP = sum(ESP,na.rm=T))
  IR12_list[[i]]$IR_std   = IR12_list[[i]]$sum_age_std/IR12_list[[i]]$sum_ESP
  IR12_list[[i]]$Low_std  = IR12_list[[i]]$IR_std -1.96*(IR12_list[[i]]$IR_std /sqrt(IR12_list[[i]]$sum_sum_events))
  IR12_list[[i]]$High_std = IR12_list[[i]]$IR_std +1.96*(IR12_list[[i]]$IR_std /sqrt(IR12_list[[i]]$sum_sum_events))
}  


#Calculate age-standardised IR tables including Poisson's method when outcomes < 100 -------------------------------------
#'Desired scale:  rates per 100,000 population
#'The provided IR are per 1,000 person-year, thus, to report 100,000 population, we only need to multiply by 100
library(epitools)
scale_std = 100 
#by ethnicity, age and sex 
IR_list = list(IR_table_6cat, IR_table_9cat,IR_table_18cat); 
names(IR_list) = c("IR_table_6cat","IR_table_9cat","IR_table_18cat")
for (i in 1:length(IR_list)){
  #get age standardised columns per ESP2013 age group
  IR_list[[i]]<-  left_join(IR_list[[i]], ESP2013, by=c("AGE"="Age_group")) 
  IR_list[[i]]$age_std = ((IR_list[[i]]$sum_events/IR_list[[i]]$sum_years)*scale_std)*IR_list[[i]]$ESP
  #get age_std IR
  IR_list[[i]]  = IR_list[[i]] %>% 
    group_by(outcome,SEX,code) %>% 
    summarise(sum_sum_events = sum(sum_events,na.rm=T), sum_personyears = sum(sum_years,na.rm=T), sum_age_std = sum(age_std,na.rm=T),
              sum_ESP = sum(ESP,na.rm=T), varASR = var(age_std,na.rm=T), var_events = var(sum_years,na.rm=T))
  IR_list[[i]]$IR_std   = IR_list[[i]]$sum_age_std/IR_list[[i]]$sum_ESP
  IR_list[[i]]$Low_std  = IR_list[[i]]$IR_std -1.96*(IR_list[[i]]$IR_std /sqrt(IR_list[[i]]$sum_sum_events))
  IR_list[[i]]$High_std = IR_list[[i]]$IR_std +1.96*(IR_list[[i]]$IR_std /sqrt(IR_list[[i]]$sum_sum_events))
  IR_list[[i]]$LowCI_poisson = pois.byar(x=IR_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$lower  #for Byar's approx to Poisson method
  IR_list[[i]]$HighCI_poisson= pois.byar(x=IR_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$upper  #for Byar's approx to Poisson method
}   
#by ethnicity, age, sex and period
IR12_list = list(IR1_table_6cat,IR1_table_9cat,IR1_table_18cat,  IR2_table_6cat,IR2_table_9cat,IR2_table_18cat)
names(IR12_list)=c("IR1_table_6cat","IR1_table_9cat","IR1_table_18cat",  "IR2_table_6cat","IR2_table_9cat","IR2_table_18cat")
for (i in 1:length(IR12_list)){
  #get age standardised columns per ESP2013 age group
  IR12_list[[i]]<-  left_join(IR12_list[[i]], ESP2013, by=c("AGE"="Age_group")) 
  IR12_list[[i]]$age_std = ((IR12_list[[i]]$sum_events/IR12_list[[i]]$sum_years)*scale_std)*IR12_list[[i]]$ESP
  #get age_std IR
  IR12_list[[i]]  = IR12_list[[i]] %>% 
    group_by(outcome,SEX,time,code) %>% 
    summarise(sum_sum_events = sum(sum_events,na.rm=T), sum_personyears = sum(sum_years,na.rm=T), sum_age_std = sum(age_std,na.rm=T),sum_ESP = sum(ESP,na.rm=T),
              sum_ESP = sum(ESP,na.rm=T), varASR = var(age_std,na.rm=T), var_events = var(sum_years,na.rm=T))
  IR12_list[[i]]$IR_std   = IR12_list[[i]]$sum_age_std/IR12_list[[i]]$sum_ESP
  IR12_list[[i]]$Low_std  = IR12_list[[i]]$IR_std -1.96*(IR12_list[[i]]$IR_std /sqrt(IR12_list[[i]]$sum_sum_events))
  IR12_list[[i]]$High_std = IR12_list[[i]]$IR_std +1.96*(IR12_list[[i]]$IR_std /sqrt(IR12_list[[i]]$sum_sum_events))
  IR12_list[[i]]$LowCI_poisson = pois.byar(x=IR12_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$lower  #for Byar's poisson approximation
  IR12_list[[i]]$HighCI_poisson= pois.byar(x=IR12_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$upper  #for Byar's poisson approximation
} 

#Replace the CI for those cases where event is < 100 (Dobson's method):
#1.1 LowCI_poisson = (qchisq(0.025,2*sum_sum_events)/2)/sum_personyears --"OR"-- pois.byar()$lower
#1.2 ASRLow_CI =  ASR + (LowCI_poisson - num_events_in_a_year) * sqrt( varASR / var_events)
#2.1 HighCI_poisson = (qchisq(0.975,2*(sum_sum_events+1))/2)/sum_personyears  --"OR"-- pois.byar()$upper
#2.2 ASRHigh_CI =  ASR + (HighCI_poisson - num_events_in_a_year) * sqrt( varASR / var_events)
#doubt num_events_in_a_year ==  
#IR_list[[i]]$Low_std2 = ifelse(IR_list[[i]]$sum_sum_events < 100, IR_list[[i]]$IR_std + ((IR_list[[i]]$LowCI_poisson-IR_list[[i]]$sum_sum_events)* sqrt(IR_list[[i]]$varASR/IR_list[[i]]$var_events)/100000), IR_list[[i]]$Low_std) #1.
#IR_list[[i]]$High_std2 =ifelse(IR_list[[i]]$sum_sum_events < 100, IR_list[[i]]$IR_std + ((IR_list[[i]]$HighCI_poisson-IR_list[[i]]$sum_sum_events)*sqrt(IR_list[[i]]$varASR/IR_list[[i]]$var_events)/100000), IR_list[[i]]$High_std) #2.

#IR12_list[[i]]$Low_std2 = ifelse(IR12_list[[i]]$sum_sum_events < 100, IR12_list[[i]]$IR_std + (IR12_list[[i]]$LowCI_poisson-IR12_list[[i]]$sum_sum_events)* (sqrt(IR12_list[[i]]$varASR/IR12_list[[i]]$var_events)/100000), IR12_list[[i]]$Low_std)  #1.
#IR12_list[[i]]$High_std2 = ifelse(IR12_list[[i]]$sum_sum_events < 100,IR12_list[[i]]$IR_std + (IR12_list[[i]]$HighCI_poisson-IR12_list[[i]]$sum_sum_events)*sqrt(IR12_list[[i]]$varASR/IR12_list[[i]]$var_events)/100000, IR12_list[[i]]$High_std) #2.

for (i in 1:length(IR_list)){
  #include Byar's <100
  #IR_list[[i]]$Low_std =ifelse(IR_list[[i]]$sum_sum_events < 100, IR_list[[i]]$LowCI_poisson, IR_list[[i]]$Low_std) 
  #IR_list[[i]]$High_std=ifelse(IR_list[[i]]$sum_sum_events < 100, IR_list[[i]]$HighCI_poisson,IR_list[[i]]$High_std)
  #include Byar's <100 >10
  IR_list[[i]]$Low_std =ifelse(IR_list[[i]]$sum_sum_events < 100 & IR_list[[i]]$sum_sum_events >= 10, IR_list[[i]]$LowCI_poisson, IR_list[[i]]$Low_std) 
  IR_list[[i]]$High_std=ifelse(IR_list[[i]]$sum_sum_events < 100 & IR_list[[i]]$sum_sum_events >= 10, IR_list[[i]]$HighCI_poisson,IR_list[[i]]$High_std)
  #include _exact <10
  IR_list[[i]]$Low_std =ifelse(IR_list[[i]]$sum_sum_events < 10,pois.exact(x=IR_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$lower,IR_list[[i]]$Low_std)  #exact
  IR_list[[i]]$High_std=ifelse(IR_list[[i]]$sum_sum_events < 10,pois.exact(x=IR_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$upper,IR_list[[i]]$High_std) #exact
} 

for (i in 1:length(IR12_list)){
  #include Byar's <100
  #IR12_list[[i]]$Low_std =ifelse(IR12_list[[i]]$sum_sum_events < 100,IR12_list[[i]]$LowCI_poisson, IR12_list[[i]]$Low_std) 
  #IR12_list[[i]]$High_std=ifelse(IR12_list[[i]]$sum_sum_events < 100,IR12_list[[i]]$HighCI_poisson,IR12_list[[i]]$High_std)   
  #include Byar's <100 >10
  IR12_list[[i]]$Low_std =ifelse(IR12_list[[i]]$sum_sum_events < 100 & IR12_list[[i]]$sum_sum_events >= 10,IR12_list[[i]]$LowCI_poisson, IR12_list[[i]]$Low_std) 
  IR12_list[[i]]$High_std=ifelse(IR12_list[[i]]$sum_sum_events < 100 & IR12_list[[i]]$sum_sum_events >= 10,IR12_list[[i]]$HighCI_poisson,IR12_list[[i]]$High_std)   
  #include _exact <10
  IR12_list[[i]]$Low_std =ifelse(IR12_list[[i]]$sum_sum_events < 10,pois.exact(x=IR12_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$lower,IR12_list[[i]]$Low_std)  #exact
  IR12_list[[i]]$High_std=ifelse(IR12_list[[i]]$sum_sum_events < 10,pois.exact(x=IR12_list[[i]]$IR_std, pt= 1, conf.level = 0.95)$upper,IR12_list[[i]]$High_std) #exact  
}

##Produce and save plots -------------------------------------------------------------------------------------
main_title = c("death at 28 days", "death at 90 days", "CVE at 30 days", "CVE at 1 year")

sex_labs <- c("Men", "Women"); names(sex_labs) <- c("1", "2")
colours_labels #defined at "Prepare data" section
eth_order_list = list(ethnicity_5_group = levels(factor(mydata$ethnicity_5_group)),
                      ONS_9eth = levels(mydata$ONS_9eth),
                      PrimaryCode_19cat = levels(mydata$PrimaryCode_19cat)  )
###by ethnicity, age and sex: ----
j=1
IR_plots = vector("list",length(IR_list)*length(outcomes_list)); names(IR_plots) <- outer(main_title,names(IR_list),paste, sep = " - ")
for (i in 1:length(IR_list)){
  #print(names(IR_list[i]))
  for (o in 1:length(outcomes_list)){
    main_text = paste("Age-standarized Incidence rate of ",main_title[o],sep="")
    xlab_text = paste("Age-standarized Incidence of ",main_title[o], "\n(per 100,000 population)","\n",sep="")
    df = IR_list[[i]]  %>% ungroup()
    df<-  left_join(df, colours_labels[[i]], by=c("code"="code")) 
    col = (df$color[df$outcome == outcomes_list[o]] )
    eth_order = eth_order_list[[i]]
    df$code2 = fct_rev( fct_relevel(df$code, eth_order) )   
    #Bar plot for 'eth' (age-standarized by ethnicity)
    IR_plots[[j]] <- df[df$outcome == outcomes_list[o],] %>% 
      ggplot(aes( x=IR_std, y=code2, fill=code ))+
      geom_bar(stat="identity", alpha=.6, width=.4)+ 	
      scale_fill_manual(values=c(col), labels=df$code2) +
      geom_errorbar(aes(xmin = Low_std, xmax = High_std), width=.2, alpha=0.6) +
      facet_grid( ~ SEX, labeller = labeller(SEX=sex_labs) )+
      #facet_wrap( ~ SEX, scales = "free", nrow = 1, labeller = labeller(SEX=sex_labs) )+
      theme_bw() + labs(title=main_text, x=xlab_text, y = "Ethnicity groups")+
      theme(legend.position = "none")    
    #count
    #print(outcomes_list[o]) 
    j=j+1
  }
}

###by ethnicity, age, sex and period: ----
colours_labels= rep(colours_labels,2) #run only once!!
eth_order_list= rep(eth_order_list,2) #run only once!!
names(IR12_list[c(4:6)])
for (i in 4:length(IR12_list)){IR12_list[[i]]$time <- fct_relevel(IR12_list[[i]]$time, levels(mydata$months)) } #set the order for waives or months

j=1
IR12_plots = vector("list",length(IR12_list)*length(outcomes_list)); names(IR12_plots) <- outer(main_title,names(IR12_list),paste, sep = ": ")
for (i in 1:length(IR12_list)){
  #print(names(IR_list[i]))
  for (o in 1:length(outcomes_list)){
    main_text = paste("Age-standarized Incidence rate of ",main_title[o],sep="")
    xlab_text = paste("Age-standarized Incidence of ",main_title[o], "\n(per 100,000 population)","\n",sep="")
    df = IR12_list[[i]]  %>% ungroup()
    df<-  left_join(df, colours_labels[[i]], by=c("code"="code")) 
    col = (df$color[df$outcome == outcomes_list[o]] )
    eth_order = eth_order_list[[i]]
    df$code2 = fct_rev( fct_relevel(df$code, eth_order) )   
    # axis limit [xlim()]
    min_val = min(df$Low_std[!is.na(df$Low_std) & df$outcome == outcomes_list[o]])
    max_val =  max(df$High_std[!is.na(df$High_std)& df$outcome == outcomes_list[o]])
    min_val = ifelse(min_val < 0,-0.5,min_val)
    max_val = ifelse(max_val > 200,200,max_val)
    #Bar plot for 'eth' (age-standarized by ethnicity)
    IR12_plots[[j]] <- df[df$outcome == outcomes_list[o],] %>% 
      ggplot(aes( x=IR_std, y=code2, fill=code ))+
      geom_bar(stat="identity", alpha=.6, width=.4)+ 	
      scale_fill_manual(values=c(col), labels=df$code) +
      geom_errorbar(aes(xmin = Low_std, xmax = High_std), width=.2, alpha=0.6) +
      coord_cartesian(xlim = c(min_val, max_val) ) +
      #coord_cartesian(xlim = c(0, 100) ) +
      facet_grid(time ~ SEX, labeller = labeller(SEX=sex_labs) )+
      #facet_wrap(time ~ SEX, scales = "free", nrow = 3, labeller = labeller(SEX=sex_labs) )+
      theme_bw() + labs(title=main_text, x=xlab_text, y = "Ethnicity groups")+
      theme(legend.position = "none")    
    #count
    #print(outcomes_list[o]) 
    j=j+1
  }
}

##Save plots for age-std IR   ---------------------------------------------------------------

#SAVE All individuals AGE_calibration PLOTS: plot dimentions #with=1222 , height=576
#1.Create pdf where each page is a separate plot USING A LOOP.
# folder_path = "/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_eth_age_sex_period_censored/"
# pdf_name = "age_std_IR_test1.pdf"
#pdf(paste(folder_path,pdf_name, sep=""),width=13.85, height=5.95,)
#for (i in c(1:length(IR_plots))) {
#  print(IR_plots[[i]])
#}
#dev.off()

#1.Create pdf where each page is a separate plot (cairo_pdf() does not require a loop).
save_plots = "NO"

if(save_plots == "YES"){
  
  folder_path = "/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/Individual plots ie., only 6, 9 or 18 cat/"
  pdf_name = "age_std_IR_test1_correct_CI.pdf"
  cairo_pdf(paste(folder_path,pdf_name, sep=""),width=8.27, height=8.98,onefile=T)  
  print(IR_plots)
  print(IR12_plots[c(1:2,4:5,7:8,10:11,13:14,16:17,19:20,22:23)])
  dev.off()
  
  pdf_name = "age_std_IR_test2_correct_CI.pdf"
  cairo_pdf(paste(folder_path,pdf_name, sep=""),width=11.90, height=8.49,onefile=T)  
  print(IR12_plots[c(3,6,9,12,15,18,21,24)])
  dev.off()
  
  
  pdf_name = "age_std_IR_correct_CI.pdf"
  cairo_pdf(paste(folder_path,pdf_name, sep=""),width=8.0, height=9.50,onefile=T)  
  print(IR_plots)
  print(IR12_plots)
  dev.off()
}

#SAVE all calibration figures with the plots in R, along with the model, split_dataset, and pred_values:
# filename1 = paste("/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/QRISk3_all_models_AND_calibration_plots_by_age_&_age_ethnicity", ".rds", sep="")
# datalist = list(models = models, split_datasets=split_datasets, pred_values= pred_values, calibration_plots_all_individuals=calibration_plots_all_individuals, calibration_plots_by_ethnicity = calibration_plots_by_ethnicity)
# saveRDS(datalist,filename1)
# rm(datalist)


##Save censored age-std IR tables ---------------------------------------------------------------
names(IR_list)
names(IR12_list)

if(run=="YES"){
  tail(IR_list[[1]])   #' IR by  6 eth, age, sex 
  tail(IR12_list[[1]])  #' IR by  6 eth, age, sex & time
  tail(IR12_list[[4]])  #' IR by  6 eth, age, sex & waives
  
  head(IR_list[[2]])   #' IR by  9 eth  age, sex
  head(IR12_list[[2]])  #' IR by  9 eth  age, sex & time
  head(IR12_list[[5]])  #' IR by  9 eth, age, sex & waives
  
  head(IR_list[[3]])  #' IR by 18 eth  age, sex
  head(IR12_list[[3]]) #' IR by 18 eth  age, sex & time
  head(IR12_list[[6]]) #' IR by 18 eth, age, sex & waives
}


#Censor tables to export out of the TRE:
#library(plyr)
#Create a cpy for the censor tables:
IR_table_6cat_censored = IR_list[[1]][c(1,3,7,2,4,10:12)]; IR1_table_6cat_censored = IR12_list[[1]][c(1,4,8,2,3,5,11:13)]; IR2_table_6cat_censored = IR12_list[[4]][c(1,4,8,2,3,5,11:13)]; 
IR_table_9cat_censored = IR_list[[2]][c(1,3,7,2,4,10:12)]; IR1_table_9cat_censored = IR12_list[[2]][c(1,4,8,2,3,5,11:13)]; IR2_table_9cat_censored = IR12_list[[5]][c(1,4,8,2,3,5,11:13)]; 
IR_table_18cat_censored= IR_list[[3]][c(1,3,7,2,4,10:12)]; IR1_table_18cat_censored= IR12_list[[3]][c(1,4,8,2,3,5,11:13)]; IR2_table_18cat_censored= IR12_list[[6]][c(1,4,8,2,3,5,11:13)];
#Tag to Add which ethnicity classification is
tag_6cat  = rep("6cat",length(IR_table_6cat_censored$SEX))
tag_9cat = rep("9cat",length(IR_table_9cat_censored$SEX))
tag_19cat= rep("19cat",length(IR_table_18cat_censored$SEX))
tag1_6cat  = rep("6cat",length(IR1_table_6cat_censored$SEX))
tag1_9cat = rep("9cat",length(IR1_table_9cat_censored$SEX))
tag1_19cat= rep("19cat",length(IR1_table_18cat_censored$SEX))
tag2_6cat  = rep("6cat",length(IR2_table_6cat_censored$SEX))
tag2_9cat = rep("9cat",length(IR2_table_9cat_censored$SEX))
tag2_19cat= rep("19cat",length(IR2_table_18cat_censored$SEX))   
#Include them into a list:
IR_list_censored = list(cbind(classification=tag_6cat,IR_table_6cat_censored),cbind(classification=tag_9cat,IR_table_9cat_censored),cbind(classification=tag_19cat,IR_table_18cat_censored)); 
names(IR_list_censored) = c("IR_table_6cat_censored","IR_table_9cat_censored","IR_table_18cat_censored")
IR12_list_censored = list(cbind(classification=tag1_6cat,IR1_table_6cat_censored),cbind(classification=tag1_9cat,IR1_table_9cat_censored),cbind(classification=tag1_19cat,IR1_table_18cat_censored),  
                          cbind(classification=tag2_6cat,IR2_table_6cat_censored),cbind(classification=tag2_9cat,IR2_table_9cat_censored),cbind(classification=tag2_19cat,IR2_table_18cat_censored))
names(IR12_list_censored)=c("IR1_table_6cat_censored","IR1_table_9cat_censored","IR1_table_18cat_censored",  "IR2_table_6cat_censored","IR2_table_9cat_censored","IR2_table_18cat_censored")

# Censor tables:
#'  censor cells that have counts <10! (no IR estimates)
#'  and round to the nearest multiple of 5
#'  Extra: round IR rates (non necessary)
#'  Change NA for "<10" (later than round as we change from num to char)
for (i in 1:length(IR_list_censored)){
  IR_list_censored[[i]][IR_list_censored[[i]]$sum_sum_events<10 & IR_list_censored[[i]]$sum_sum_events!=0,c(6)]  <- NA  #c(6) = $sum_sum_events
  IR_list_censored[[i]]$sum_sum_events <- plyr::round_any( IR_list_censored[[i]]$sum_sum_events, 5, f = round) 
  IR_list_censored[[i]][c(7:9)] <- round(IR_list_censored[[i]][c(7:9)] ,3)             #Optional   #c(7:9)=IR[95CI]
  IR_list_censored[[i]]$sum_sum_events[is.na(IR_list_censored[[i]]$sum_sum_events)] <-"<10"
}
for (i in 1:length(IR12_list_censored)){
  IR12_list_censored[[i]][IR12_list_censored[[i]]$sum_sum_events<10 & IR12_list_censored[[i]]$sum_sum_events!=0,c(7)]  <- NA  #c(6) = $sum_sum_events
  IR12_list_censored[[i]]$sum_sum_events <- plyr::round_any( IR12_list_censored[[i]]$sum_sum_events, 5, f = round) 
  IR12_list_censored[[i]][c(8:10)] <- round(IR12_list_censored[[i]][c(8:10)] ,3)         #Optional #c(8:10)=IR[95CI]
  IR12_list_censored[[i]]$sum_sum_events[is.na(IR12_list_censored[[i]]$sum_sum_events)] <-"<10"
}

#Replace "\n" for " "
for (i in 1:length(IR12_list_censored)){ IR12_list_censored[[i]]$time = gsub("\n", " ", IR12_list_censored[[i]]$time)}

# Syntax to save IR tables censored:
run_save_tables = "NO" #Options "NO"/"YES" (default "NO" to avoid append overwriting)
if (run_save_tables == "YES"){
  path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/age-std IR - Plots and censored tables/age_std_IR_correct_CI_censored.csv"
  for (i in 1:length(IR_list_censored)){
    if (i == 1){write.table(IR_list_censored[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(IR_list_censored[[i]],  path_filename, col.names = F, row.names=F, append= T, sep=',')}
  }
  path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/age-std IR - Plots and censored tables/age_std_IR_correct_CI_byperiod_IR12_censored.csv"
  for (i in 1:length(IR12_list_censored)){
    if (i == 1){write.table(IR12_list_censored[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(IR12_list_censored[[i]],  path_filename, col.names = F, row.names=F, append= T, sep=',')}
  }
}

#IR12_list_censored by parts:
if (run_save_tables == "YES"){
  path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/age-std IR - Plots and censored tables/age_std_IR_correct_CI_byyears_IR1_censored.csv"
  for (i in 1:3){
    if (i == 1){write.table(IR12_list_censored[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(IR12_list_censored[[i]],  path_filename, col.names = F, row.names=F, append= T, sep=',')}
  }
  
  path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/age-std IR - Plots and censored tables/age_std_IR_correct_CI_every6months_IR2_censored_Part1_6&9cat.csv"
  for (i in 4:5){
    if (i == 4){write.table(IR12_list_censored[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(IR12_list_censored[[i]],  path_filename, col.names = F, row.names=F, append= T, sep=',')}
  }
  path_filename = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/age-std IR - Plots and censored tables/age_std_IR_correct_CI_every6months_IR2_censored_Part2_19cat.csv"
  for (i in length(IR12_list_censored)){ write.table(IR12_list_censored[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
}


##Produce age-std IR plots for better visualization ---------------------------------------------------------------
colours_labels2 = list(ethnicity_5_group = data.frame( code = names(table(mydata$ethnicity_5_group)), colours = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BFC4","#00BA38") ),
                       ONS_9eth = data.frame( code = names(table(mydata$ONS_9eth)) , colours = c(rep("#F8766D",4),rep("#F564E3",2),"#B79F00","#619CFF",rep("#00BA38",2)) ), 
                       # #ONS_9eth = data.frame( code = names(table(mydata$ONS_9eth)) , colours = c(rep("#F8766D",4),"#619CFF",rep("#F564E3",2),"#B79F00",rep("#00BA38",2)) ),
                       PrimaryCode_19cat = data.frame( code = names(table(mydata$PrimaryCode_19cat)) , colours = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BA38",4),"#00BFC4") ) )

###6 high-level ethnicity + 9 subgroups within in the same plot:
names(IR_list)
names(IR12_list)

eth_plot_order = c("Asian or Asian British","Bangladeshi","Chinese","Indian","Pakistani","Black or Black British","Black African","Black Caribbean","Mixed",#"Mixed",
                   "Other + Unknown","Other Ethnic Group","Unknown","White","White British","White other")  


main_title = c("death at 28 days", "death at 90 days", "CVE at 30 days", "CVE at 1 year")
sex_labs <- c("Men", "Women"); names(sex_labs) <- c("1", "2")
#instead of colours_labels #defined at "Prepare data" section, use  colours_labels2
eth_order_list = list(ethnicity_5_group = levels(factor(mydata$ethnicity_5_group)),
                      ONS_9eth = levels(mydata$ONS_9eth),
                      PrimaryCode_19cat = levels(mydata$PrimaryCode_19cat)  )
###by age and sex: mixing 6 and 9 cat ----
##Prepare data for the loop
#Save plots
IR_plots2 = vector("list",length(outcomes_list)); names(IR_plots2) <- outer("6 and 9 cat",main_title,paste, sep = ": ")
#Select 6 and 9 cat IR
df = data.frame( rbind(IR_list[[1]],IR_list[[2]]), class=c( rep("6cat",length(IR_list[[1]]$outcome)), rep("9cat",length(IR_list[[2]]$outcome)) ) )
#Exclude duplicates between 6 and 9 cat: Mixed
df = df[!(df$code == "Mixed" & df$class == "9cat"),]
#In 9 cat, Other = "Other + Unknown"
df$code[(df$code == "Other" & df$class == "9cat")] = "Other + Unknown"
#Mapping from 9 to 6 cat
df = left_join(df, eth_mapping[c(1:2)], by = c("code"="names"))
df$mapping_6cat[df$code == "Other + Unknown" ] = "Other + Unknown"
#Barplot order
df$code2 = fct_rev( fct_relevel(df$code, eth_plot_order) )   
#Colors bars
#If using the "colours_labels2"
temp = (left_join(df, colours_labels2[[2]], by=c("code"="code")))$colours
df$colours = (left_join(df, colours_labels2[[1]], by=c("code"="code")))$colours 
df$colours = ifelse(is.na(df$colours),temp, df$colours)
#If using the "colours_labels"
#  temp = (left_join(df, colours_labels[[2]], by=c("code"="code")))$color 
#  df$colours = (left_join(df, colours_labels[[1]], by=c("code"="code")))$color 
#  df$colours = ifelse(is.na(df$colours),temp, df$colours)
#additional changes
df$colours = ifelse(is.na(df$colours),"#c49c94", df$colours)  #Include colour to those missing == "Other + Unknown"
df$colours[df$code == "Other Ethnic Group" | df$code == "Unknown"] <- "#c49c94" #assign same colour for this two groups since will go under "Other + Unknown"
#Bar plots colour brightness (a), and axis.text.y face = italic, bold... (f)
#a = c(rep(0.8,3), rep(0.2,2), 0.8, rep(0.2,5), 0.8, rep(0.2,3)) #See order clearer: 0.8(As,Bl,Mix), 0.2(O,U), 0.8(W), (Bangl,Ch,Ind,Pak,Bl Afr), 0.8(Other+U), 0.2(plus 3 more)   || repeat x2
a = rep( c(rep(0.8,3), rep(0.4,2), 0.8, rep(0.4,5), 0.8, rep(0.4,3)) ,2)
#Vectorized face = () is not officially supported yet (Nov 2022) 
#needs to be manually included as order is the rev of the displayed in the y axis 
#i.e., this would not work# f = ifelse(a == 0.8, "bold.italic","italic"); f = f[c(1:15)]  
#face options: ("plain", "italic", "bold", "bold.italic").
f = rev( c("bold",rep("italic",4),  "bold",rep("italic",2), "bold","bold",rep("italic",2), "bold",rep("italic",2))) 
#colours ald labels
col = colnames(table(df$code2,df$colours))
col = c("#F8766D","#00BFC4","#00BA38","#619CFF","#F564E3")
labels_col = rownames(table(df$code2,df$colours))
#axis limit [xlim()] -> here we use the max and lowest xlim directly
##Loop
j=1
for (o in 1:length(outcomes_list)){
  main_text = paste("Age-standarized Incidence rate of ",main_title[o],sep="")
  xlab_text = paste("Age-standarized Incidence of ",main_title[o], "\n(per 100,000 population)","\n",sep="")
  #Bar plot for 'eth' (age-standarized by ethnicity)
  IR_plots2[[j]] <- df[df$outcome == outcomes_list[o] ,] %>% distinct() %>%
    # ggplot(aes( x=IR_std, y=code2,group=factor(mapping_6cat), fill=colours))+
    ggplot(aes( x=IR_std, y=code2, fill=colours ))+
    scale_fill_manual(values=c(rep(col,2)), labels=c(rep(labels_col))) +
    geom_bar(stat="identity",alpha=a, width=0.8)+ 	  #position = position_dodge(preserve = 'single')
    geom_errorbar(aes(xmin = Low_std, xmax = High_std), width=.2, alpha=0.5) +
    facet_grid( ~ SEX, labeller = labeller(SEX=sex_labs) )+
    #facet_wrap( ~ SEX, scales = "free", nrow = 1, labeller = labeller(SEX=sex_labs) )+
    theme_bw() + labs(title=main_text, x=xlab_text, y = "Ethnicity groups")+
    theme(axis.text.y = element_text(face = f)) + 
    theme(legend.position = "none")    
  #count
  #print(outcomes_list[o]) 
  j=j+1
}
-
  
  
  ###by age, sex and time: mixing 6 and 9 cat ----
##Prepare data for the loop
#Save plots
period_text = c(" by time"," by months") #waives
IR_plots2b = vector("list",length(outcomes_list)*2); 
names(IR_plots2b) <- outer( outer("6 and 9 cat",main_title,paste, sep = ": "), period_text, paste, sep = "") 
#Select 6 and 19 cat IR ~ by period:
df1 = data.frame( rbind(IR12_list[[1]],IR12_list[[2]]), class=c( rep("6cat",length(IR12_list[[1]]$outcome)), rep("9cat",length(IR12_list[[2]]$outcome)) ) )
df2 = data.frame( rbind(IR12_list[[4]],IR12_list[[5]]), class=c( rep("6cat",length(IR12_list[[4]]$outcome)), rep("9cat",length(IR12_list[[5]]$outcome)) ) )
#Exclude duplicates between 6 and 9 cat: Mixed
df1 = df1[!(df1$code == "Mixed" & df1$class == "9cat"),]
df2 = df2[!(df2$code == "Mixed" & df2$class == "9cat"),]
#In 9 cat, Other = "Other + Unknown"
df1$code[(df1$code == "Other" & df1$class == "9cat")] = "Other + Unknown"
df2$code[(df2$code == "Other" & df2$class == "9cat")] = "Other + Unknown"
#Mapping from 9 to 6 cat
df1 = left_join(df1, eth_mapping[c(1:2)], by = c("code"="names"))
df2 = left_join(df2, eth_mapping[c(1:2)], by = c("code"="names"))
df1$mapping_6cat[df1$code == "Other + Unknown" | df1$code == "Unknown" | df1$code == "Other Ethnic Group"] = "Other + Unknown"
df2$mapping_6cat[df2$code == "Other + Unknown" | df2$code == "Unknown" | df2$code == "Other Ethnic Group"] = "Other + Unknown"
#Barplot order
df1$code2 = fct_rev( fct_relevel(df1$code, eth_plot_order) )   
df2$code2 = fct_rev( fct_relevel(df2$code, eth_plot_order) )   

#Colors bars
col2 = rev(c("#F8766D","#00BA38","#00BFC4","#619CFF","#F564E3")) #reverse for red-salmon, green, cian, blue, pink # See: library(scales); show_col(hue_pal()(6))

#time order dor df2:
df2$time =  fct_relevel(df2$time,levels(mydata$months)) #waives

#Bar plots colour brightness (a), and axis.text.y face = italic, bold... (f)
a = rep( c(rep(0.8,3), rep(0.4,2), 0.8, rep(0.4,5), 0.8, rep(0.4,3)) ,2)   #See order clearer: 0.8(As,Bl,Mix), 0.2(O,U), 0.8(W), (Bangl,Ch,Ind,Pak,Bl Afr), 0.8(Other+U), 0.2(plus 3 more)   || repeat x2
f = rev( c("bold",rep("italic",4),  "bold",rep("italic",2), "bold","bold",rep("italic",2), "bold",rep("italic",2))) #face options: ("plain", "italic", "bold", "bold.italic").
df_12 = list(df1,df2)

##Loop
j=1
for (d in 1:length(df_12)){
  df = df_12[[d]]
  a2 = rep(a, length(names(table(df$time))) )
  f2 = rep(f, length(names(table(df$time))) )
  for (o in 1:length(outcomes_list)){
    main_text = paste("Age-standarized Incidence rate of ",main_title[o],sep="")
    xlab_text = paste("Age-standarized Incidence of ",main_title[o], "\n(per 100,000 population)","\n",sep="")
    # axis limit [xlim()]
    min_val = min(df$Low_std[!is.na(df$Low_std) & df$outcome == outcomes_list[o]])
    max_val =  max(df$High_std[!is.na(df$High_std)& df$outcome == outcomes_list[o]])
    min_val = ifelse(min_val < 0,-0.5,min_val)
    max_val = ifelse(max_val > 150,150,max_val)
    #Bar plot for 'eth' (age-standarized by ethnicity)
    IR_plots2b[[j]] <- df[df$outcome == outcomes_list[o] ,] %>% distinct() %>%
      #ggplot(aes( x=IR_std, y=code2, group=factor(mapping_6cat), fill=colours ))+
      ggplot(aes( x=IR_std, y=code2, fill=mapping_6cat ))+
      geom_bar(stat="identity",alpha=a2, width=0.8)+ 	  #position = position_dodge(preserve = 'single')
      scale_fill_manual(values=c(rep(col2,2))) +
      geom_errorbar(aes(xmin = Low_std, xmax = High_std), width=.2, alpha=0.5) +
      coord_cartesian(xlim = c(min_val, max_val) ) +
      facet_grid(time ~ SEX, labeller = labeller(SEX=sex_labs) )+
      #facet_wrap(time ~ SEX, scales = "free", nrow = 1, labeller = labeller(SEX=sex_labs) )+
      theme_bw() + labs(title=main_text, x=xlab_text, y = "Ethnicity groups")+
      theme(axis.text.y = element_text(face = f)) + 
      theme(legend.position = "none")    
    #count
    #print(outcomes_list[o]) 
    j=j+1
  }
}


## Attempt: Plots removing 2022 for CVE1y
df1s =  df1[!(df1$outcome == "post_covid_cvd_event_1y" & df1$time == "2022") ,] 
df2s =  df2[!(df2$outcome == "post_covid_cvd_event_1y" & df2$time == "01 Sep 2021 to\n31 Dec 2021") & !(df2$outcome == "post_covid_cvd_event_1y" & df2$time == "01 Jan 2022 to\n01 Apr 2022"),] 
df_12s = list(df1s,df2s)

IR_plots2bs = vector("list",length(outcomes_list)*2); 
names(IR_plots2bs) <- outer( outer("6 and 9 cat",main_title,paste, sep = ": "), period_text, paste, sep = "") 

##Loop
j=1
for (d in 1:length(df_12s)){
  df = df_12s[[d]]
  a2 = rep(a, length(names(table(df$time))) )
  f2 = rep(f, length(names(table(df$time))) )
  for (o in 1:length(outcomes_list)){
    main_text = paste("Age-standarized Incidence rate of ",main_title[o],sep="")
    xlab_text = paste("Age-standarized Incidence of ",main_title[o], "\n(per 100,000 population)","\n",sep="")
    # axis limit [xlim()]
    min_val = min(df$Low_std[!is.na(df$Low_std) & df$outcome == outcomes_list[o]])
    max_val =  max(df$High_std[!is.na(df$High_std)& df$outcome == outcomes_list[o]])
    min_val = ifelse(min_val < 0,-0.5,min_val)
    max_val = ifelse(max_val > 150,150,max_val)
    #Bar plot for 'eth' (age-standarized by ethnicity)
    IR_plots2bs[[j]] <- df[df$outcome == outcomes_list[o] ,] %>% distinct() %>%
      #ggplot(aes( x=IR_std, y=code2, group=factor(mapping_6cat), fill=colours ))+
      ggplot(aes( x=IR_std, y=code2, fill=mapping_6cat ))+
      geom_bar(stat="identity",alpha=a2, width=0.8)+ 	  #position = position_dodge(preserve = 'single')
      scale_fill_manual(values=c(rep(col2,2))) +
      geom_errorbar(aes(xmin = Low_std, xmax = High_std), width=.2, alpha=0.5) +
      coord_cartesian(xlim = c(min_val, max_val) ) +
      facet_grid(time ~ SEX, labeller = labeller(SEX=sex_labs) )+
      #facet_wrap(time ~ SEX, scales = "free", nrow = 1, labeller = labeller(SEX=sex_labs) )+
      theme_bw() + labs(title=main_text, x=xlab_text, y = "Ethnicity groups")+
      theme(axis.text.y = element_text(face = f)) + 
      theme(legend.position = "none")    
    #count
    #print(outcomes_list[o]) 
    j=j+1
  }
}

####SAVE plots ----
save_plots = "NO"

if(save_plots == "YES"){
  folder_path = "/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/IR_by_time_6months_censored/age-std IR - Plots and censored tables/combined age-std IR plots 6cat vs10 or 19/"
  
  pdf_name = "age_std_IR_6&9together_correct_CI.pdf"
  cairo_pdf(paste(folder_path,pdf_name, sep=""),width=6.57, height=6.55,onefile=T)  
  print(IR_plots2)
  dev.off()
  
  pdf_name = "age_std_IR_6&9by_period_correct_CI.pdf"
  cairo_pdf(paste(folder_path,pdf_name, sep=""),width=6.57, height=10.55,onefile=T)  
  print(IR_plots2b)
  dev.off()
  
}








