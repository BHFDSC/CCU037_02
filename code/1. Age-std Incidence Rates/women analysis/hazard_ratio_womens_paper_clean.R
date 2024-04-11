# This script uses a table saved in 'CCU037_02_v1_QRisk_model.R', which loads tables from Databricks  ('CCU037_02_cohort' plus two other, and with few fixed/modified changes)
# Author: Marta Pineda

rm(list = ls())
run = "YES"      #Options: "YES" or "NO" = for running the checks for the df/df1/df2 tables (see that the IR estimates are correctly stratified by cheking the last row values)

library(survival); library(dplyr); library(forcats); library(ggplot2)

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

# Load database saved in qrisk syntax: CCU037_02_cohort -------------------------
#CCU037_02_final_cohort // CCU037_02_cohort
mydata <- readRDS("/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/CCU037_02_cohort.rds")
mydata$PrimaryCode[mydata$PrimaryCode == "X"] <- "Z"


#Add pregnancy flag:
mydata2<- dbGetQuery(con,"SELECT NHS_NUMBER_DEID,preg_flag FROM dars_nic_391419_j3w9t_collab.ccu037_02_pregnancy_flag") #
mydata$preg_flag = ifelse(mydata$NHS_NUMBER_DEID %in% mydata2$NHS_NUMBER_DEID,1,0) #mydata = left_join(mydata,mydata2) #, by=c("NHS_NUMBER_DEID"="person_id_deid"))
rm(mydata2)

#Smaller database for easier managment:
#mydata2 = mydata[,c(69,1,3,70,9,59,41,42,46)] #needs to be defined!

#HR from ONS: 
#Details source:
#https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/articles/updatingethniccontrastsindeathsinvolvingthecoronaviruscovid19englandandwales/24january2020to31march2021
#https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/methodologies/deathsinvolvingcovid19byreligiousgroupandethnicgroupenglandmethodology#age-standardisation-method
#Details: men and women HR are measured separatedly.
#         include ages between 30 and 100
#         adjusted by: a) age
#                      b) age, plus location, measures of disadvantage, occupation, living arrangements, and pre-pandemic health status (fully-adjusted).
#         * Include age as a second-order polynomial * (to account for the non-linear relationship between age and the hazard of death involving COVID-19)
#         * they must include ethnicity in the adjustment aswell *
#         Reference group is White British

#Adjustment details Specific to first link: 
# residence type                            (private household, care home, other communal establishments) - from 2019 NHS Patient Register
# geographical factors                      postcodes held in GPES
# geographical factors                      population density of the Lower layer Super Output Area (LSOA) * population density as a second-order polynomial, allowing for different slopes for the top 1% of the population density distribution to account for outliers *
# deprivation &  socio-economic status      Index of Multiple Deprivation (IMD) 
# household deprivation                     highest level of qualification (degree, A-level or equivalent, GCSE or equivalent, no qualification)
# household deprivation                     National Statistics Socio-Economic Classification (NS-SEC) of the household reference person (higher managerial, administrative and professional occupations, intermediate occupations, routine and manual occupations, never worked or long-term unemployed, not applicable)
# household composition and circumstances:    number of people in the household
#                                             the family type (not a family, couple with children, lone parent)
#                                             household composition (single-adult household, two-adult household, multi-generational household (households with at least one person aged 65 years or over and someone at least 20 years younger), child aged 18 years or under in household)
#                                             tenure of the household (owned outright, owned with mortgage, social rented, private rented, other)
#                                             We include an additional "not in a household" level for all household variables for people living in a care home or other communal establishment.
# occupational exposure                     if the individual is a key worker, and if so, what type
# exposure to disease and contact           exposure to disease and contact with others using scores ranging from 0 (no exposure) to 100 (maximum exposure)                                          
# pre-pandemic health status                *number of hospital admissions over the past three years, (from HES)
# pre-pandemic health status                *number of days spent in admitted patient care over the past three years, (from GDPPR)                                         
#                                           ¦--> *To allow for the effect of all these health-related factors to vary depending on the age of the individuals, we interact each of them with a binary variable indicating if the individual is aged 70 years or over.

#What we have:
# age,sex, ethnicity, lsoa_code, IMD_quintile, 
# We could include number of prior comorbidities as "health status before pandemic" / also can ask FA to compute number of GP visits?

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

outcomes = c("death28d","death90d","post_covid_cvd_event_30d","post_covid_cvd_event_1y") #,"covid_hospitalisation" = we don't have date of hospitalisation
days_variables  = c("daysto_death28d","daysto_death90d","daysto_30dCVE","daysto_1yCVE")

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

mydata$ONS_9eth = relevel(mydata$ONS_9eth, ref = "White British")

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
mydata$PrimaryCode_19cat = relevel(mydata$PrimaryCode_19cat, ref = "British")


## Prepare time/waives 
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


## Censor analysis up to 31 Dec 2021 -------------------------------------------
#dim(mydata)
#mydata = mydata[mydata$covid_date<="2021-12-31",]

#AGE-adjusted models____________________________________________________________----

##Mortality models only adjusted by age, for 6_cat and 19_cat: ------------------
#library(survival); library(dplyr); library(forcats); library(ggplot2)
ethnicity = c("ethnicity_5_group","ONS_9eth","PrimaryCode_19cat")
#colours = list(ethnicity_5_group = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BA38"),
#               ONS_9eth = c(rep("#F8766D",4),rep("#F564E3",2),"#619CFF","#B79F00","#00BFC4"),
#               PrimaryCode_19cat = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BFC4",3),"#00BA38") )


colours_labels = list(ethnicity_5_group = data.frame( code = names(table(mydata$ethnicity_5_group)), color = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BFC4","#00BA38") ),
                      ONS_9eth = data.frame( code = names(table(mydata$ONS_9eth)) , color = c(rep("#F8766D",4),rep("#F564E3",2),"#619CFF","#B79F00",rep("#00BA38",2)) ),
                      PrimaryCode_19cat = data.frame( code = names(table(mydata$PrimaryCode_19cat)) , color = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BA38",4),"#00BFC4") ) )

reference_group = c("White","White British","British")
outcomes_text = c("COVID-19 death at 28 days","COVID-19 death at 90 days") #, "CVE at 30 days", "CVE at 1 year")
followup = days_variables[1:2] 

mod_names = c("male_6_cat","male_9_cat",'male_18_cat', "female_6_cat","female_9_cat","female_18_cat")
cox_mortality  = vector("list",12) ; names(cox_mortality)  = c(paste("28-days death",mod_names,sep=" "),paste("90-days death",mod_names,sep=""))
plots_mortality= vector("list",12) ; names(plots_mortality)= c(paste("28-days death",mod_names,sep=" "),paste("90-days death",mod_names,sep=""))
#Mortality loop:
j= 1
for (i in 1:2){  # outcomes (28 and 90 days). i = 1 #to select 1st outcome (28-days mortality)
  for (sex in 1:2){
    r=1
    for (eth in ethnicity){
      #print(r)
      #prepare data
      df = data.frame(outcome = mydata[[outcomes[i]]], time = mydata[[followup[i]]], age = mydata$age, sex = mydata$sex, ethnicity = mydata[[eth]])
      df = df[df$sex == sex & df$age>= 30 & df$age <= 100,-4]
      if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")}        # else {df$ethnicity = relevel(factor(df$ethnicity), ref = "A")}
      if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")} # else {df$ethnicity = relevel(factor(df$ethnicity), ref = "A")}
      if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      # else {df$ethnicity = relevel(factor(df$ethnicity), ref = "A")}
      #run survival model
      S <- Surv(df$time, df$outcome)
      Ib = coxph(S ~ age + I(age^2) + ethnicity , data = df)
      #prepare data for the plot
      data = data.frame(ethnicity = factor(levels(df$ethnicity)[-1]), HR = summary(Ib)$conf.int[-c(1,2),1], low = summary(Ib)$conf.int[-c(1,2),3], high = summary(Ib)$conf.int[-c(1,2),4])
      cox_mortality[[j]] = data
      main_text = c(paste("Risk of", outcomes_text[i],"by ethnicity in males ranged 30 to 100 years\n(Reference group:",reference_group[r],")"),
                    paste("Risk of", outcomes_text[i],"by ethnicity in females ranged 30 to 100 years\n(Reference group:",reference_group[r],")"))
      min_val = min(data$low) ; max_val =  max(data$high)
      #data$color = colours[[eth]]
      data<-  left_join(data, colours_labels[[eth]], by=c("ethnicity"="code")) 
      eth_order = levels(df$ethnicity)[-1]
      data$ethnicity = fct_rev( fct_relevel(data$ethnicity, eth_order))
      #plot
      plots_mortality[[j]] = data %>%
        ggplot(aes(x=HR, y=ethnicity, fill=color)) + 
        geom_bar(stat="identity", alpha=.6, width=.4)+  #, position='dodge'
        geom_errorbar(aes(xmin = low, xmax = high), width=.2, alpha=0.6,colour="grey35") + #position=position_dodge(width = .45),
        geom_vline(xintercept=c(1), linetype="dotted") +
        coord_cartesian(xlim = c(0.75, max_val) ) +
        theme_bw() + labs(title=main_text[sex], x="HR of covid death (95CI%)", y = "Ethnicity groups")+ 
        theme(legend.position = "null")
      j = j+1 #number for saving models and plots
      r = r+1 #number for moving the ref group title
    }}}


# For women's paper: ___________________________________________________________ ----
mydata2 = mydata[mydata$sex == "2",]
mydata2$chronic_mental_health_disorders = ifelse((mydata2$schizophrenia==1)|(mydata2$bipolardisorder==1)|(mydata2$depression==1),1,0)
mydata2$CVD_prevention_medication = ifelse((mydata2$anti_hypertensive_drugs==1)|(mydata2$anti_coagulant_drugs==1)|(mydata2$anti_platelet_drugs==1)|(mydata2$statins==1),1,0)    
mydata2$LSOA_region = ifelse(is.na(mydata2$region_name),"Unknown",mydata2$region_name)    
#Empty memory space:
#rm(mydata); gc()


#BASED ON "Prepare variables for covid death outcomes"
#starttime_diff: difference(date_enrollment - date_origin) #date_origin = start_date
mydata2$starttime_diff = mydata2$covid_date-start_date

#stoptime_diff: difference(date_stop - date_origin) #date_origin = start_date
#Alternative to get the same: since we have time censored and will use more lines, we use days_to_EVENT and add the days from starttime_diff
mydata2$stoptime_diff_28dDeath = mydata2$daysto_death28d + mydata2$starttime_diff +0.1
mydata2$stoptime_diff_90dDeath = mydata2$daysto_death90d  + mydata2$starttime_diff +0.1
mydata2$stoptime_diff_30dCVD   = mydata2$daysto_30dCVE + mydata2$starttime_diff +0.1
mydata2$stoptime_diff_1yCVD    = mydata2$daysto_1yCVE  + mydata2$starttime_diff +0.1

#Necessary varibales for the cox models and plots
outcomes = c("death28d","post_covid_cvd_event_30d","post_covid_cvd_event_1y")
days_variables = c("daysto_death28d","daysto_30dCVE","daysto_1yCVE")
stoptime_diff_variables = c("stoptime_diff_28dDeath","stoptime_diff_30dCVD","stoptime_diff_1yCVD")
outcomes_text = c("COVID-19 death at 28 days", "CVE at 30 days", "CVE at 1 year")
ethnicity = c("ONS_9eth") #c("ethnicity_5_group","ONS_9eth","PrimaryCode_19cat")
models = c('(female 9_cat)')      #c('(female 6_cat)','(female 9_cat),'(female 19_cat)')
reference_group = c("British")     #c("White","British")


adj = "reduced" #(options "larger" or "reduced". "larger" for not merged variables; "reduced" for merged chronic mental disordes and CVD medication)

if (adj == "larger")  {myVars = c("preg_flag","IMD_quintile","waives","months","AF","CKD","diabetes","schizophrenia","bipolardisorder","depression","RA","anti_hypertensive_drugs","antipsychotic","cancer","copd","dementia","hypertension","alcoholic_liver_disease","anti_coagulant_drugs","anti_platelet_drugs","statins","obesity")}
if (adj == "reduced") {myVars = c("preg_flag","IMD_quintile","waives","months","AF","CKD","diabetes","RA","chronic_mental_health_disorders","CVD_prevention_medication","antipsychotic","cancer","copd","dementia","hypertension","alcoholic_liver_disease","obesity")}
dim(mydata2[myVars])

##All models no competing risk using "days_variables"----
#library(survival); library(dplyr); library(forcats); library(ggplot2)
#Variables that are necessary for the loop:
followup = days_variables 
#grouping as mapped to 5 cat
colours = list(ethnicity_5_group = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BA38"),
               ONS_9eth = c(rep("#F8766D",4),rep("#F564E3",2),"#619CFF","#B79F00","#00BFC4"),
               PrimaryCode_19cat = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BFC4",3),"#00BA38") )
#plot colour
#for 19 cat:# col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#F8766D","#B79F00")) #reverse of (pink, blue, cian, green, red-salmon and yellow)  # See: library(scales); show_col(hue_pal()(6))
eth="PrimaryCode_19cat" #include witch one you want to run
if(eth == "ethnicity_5_group"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00"))}  #reverse of (pink, blue, cian, green, and yellow) red-salmon excluded  # See: library(scales); show_col(hue_pal()(6))
if(eth == "ONS_9eth"){col2 = c("#F8766D","#00BFC4","#00BA38","#619CFF","#F564E3")} #order: red-salmon,green,cian,blue and pink # See: library(scales); show_col(hue_pal()(6))
if(eth == "PrimaryCode_19cat"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#F8766D","#B79F00"))} #reverse of (pink, blue, cian, green, red-salmon and yellow)  # See: library(scales); show_col(hue_pal()(6))

cox_fADJ     = vector("list",length(models)*length(outcomes))
plots_fcoxADJ = vector("list",length(models)*length(outcomes))
test.ph       = vector("list",length(models)*length(outcomes))
###Loop:  - Surv(df$time, df$outcome) ----
j= 1;
for (i in 1:length(outcomes)){
  print(outcomes_text[i])
  r=1
  for (eth in ethnicity){
    print(eth)
    ##prepare data
    df = data.frame(outcome = mydata2[[outcomes[i]]], time = mydata2[[followup[i]]], age = mydata2$age, ethnicity = mydata2[[eth]], LSOA_region=mydata2$LSOA_region, mydata2[myVars])
    df = df[df$age>= 30 & df$age <= 100,]
    if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
    if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
    if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
    ##run survival model
    S <- Surv(df$time, df$outcome)
    if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + preg_flag + ethnicity + IMD_quintile +LSOA_region+ months +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
    if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + preg_flag + ethnicity + IMD_quintile +LSOA_region+ months + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
    data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
    row.names(data) = NULL
    cox_fADJ[[j]] = data
    ##test proportional hazards
    #test.ph[[j]] <- cox.zph(Ib)
    ##prepare data for the plot (include only ethnicity estimates)
    data = data.frame(ethnicity=factor(levels(df$ethnicity)[-1]), HR=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),1], low=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),3], high=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),4] )
    main_text = c(paste0("Risk of ", outcomes_text[i]," by ethnicity in women ranged 30 to 100 years\n(Reference group: ",reference_group[r],") [Adjusted]"))
    min_val = min(data$low) ; max_val =  max(data$high)
    ##ethnicity order and color in the plot
    eth_order = levels(df$ethnicity)[-1]
    data$ethnicity = fct_rev( fct_relevel(data$ethnicity, eth_order) )
    data$color = colours[[eth]]
    ##plot
    plots_fcoxADJ[[j]] = data %>%
      ggplot(aes(x=HR, y=ethnicity, fill=color)) + 
      geom_bar(stat="identity", alpha=.6, width=.4)+  #, position='dodge'
      geom_errorbar(aes(xmin = low, xmax = high), width=.2, alpha=0.6,colour="grey35") + #position=position_dodge(width = .45),
      geom_vline(xintercept=c(1), linetype="dotted") +
      coord_cartesian(xlim = c(0.75, max_val) ) +
      scale_fill_manual(values=c(rep(col2,2))) +
      theme_bw() + labs(title=main_text, x=paste("HR of", outcomes_text[i], "(95CI%)"), y = "Ethnicity groups")+ 
      theme(legend.position = "null")
    ##name saved plot
    names(cox_fADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    names(plots_fcoxADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    ##number for saving models and plots
    print(paste("loop number:",j, sep=" "))
    j = j+1 #number for saving models and plots
    r = r+1 #number for moving the ref group title
  }}

rm(S, data, Ib); gc()


##All models no competing risk using "#starttime_diff & stoptime_diff_variables" for Surv(startTime, stopTime, event)----
#library(survminer) #for ggsurvplot
#Variables that are necessary for the loop:
followup = stoptime_diff_variables 
head(mydata2$starttime_diff)
outcomes_text = c("COVID-19 death at 28 days", "CVE at 30 days", "CVE at 1 year") #"COVID-19 death at 90 days"

#grouping as mapped to 5 cat
colours = list(ethnicity_5_group = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BA38"),
               ONS_9eth = c(rep("#F8766D",4),rep("#F564E3",2),"#619CFF","#B79F00","#00BFC4"),
               PrimaryCode_19cat = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BFC4",3),"#00BA38") )
#plot colour
#for 19 cat:# col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#F8766D","#B79F00")) #reverse of (pink, blue, cian, green, red-salmon and yellow)  # See: library(scales); show_col(hue_pal()(6))
eth="PrimaryCode_19cat" #include witch one you want to run
if(eth == "ethnicity_5_group"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00"))}  #reverse of (pink, blue, cian, green, and yellow) red-salmon excluded  # See: library(scales); show_col(hue_pal()(6))
if(eth == "ONS_9eth"){col2 = c("#F8766D","#00BFC4","#00BA38","#619CFF","#F564E3")} #order: red-salmon,green,cian,blue and pink # See: library(scales); show_col(hue_pal()(6))
if(eth == "PrimaryCode_19cat"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#F8766D","#B79F00"))} #reverse of (pink, blue, cian, green, red-salmon and yellow)  # See: library(scales); show_col(hue_pal()(6))

cox_fAGE      = vector("list",length(models)*length(outcomes))
plots_fevent  = vector("list",length(models)*length(outcomes))
plots_feventzoom= vector("list",length(models)*length(outcomes))
plots_fKM     = vector("list",length(models)*length(outcomes))
cox_fADJ      = vector("list",length(models)*length(outcomes))
plots_fcoxADJ = vector("list",length(models)*length(outcomes))
#test.ph      = vector("list",length(models)*length(outcomes))
### K-m plots and Loop age adjusted----
library(survminer)
adj = "age"
y = c(0.03,0.03,0.07) #in 19cat women: ylimit plots_feventzoom for each of the outcomes
#y = c(0.07,0.07,0.15) #in 19cat men
j= 1;
for (i in 1:length(outcomes)){
  print(outcomes_text[i])
  r=1
  for (eth in ethnicity){
    print(eth)
    ##prepare data
    df = data.frame(outcome = mydata2[[outcomes[i]]], origin_time=mydata2$starttime_diff, time = mydata2[[followup[i]]], age = mydata2$age, ethnicity = mydata2[[eth]], LSOA_region=mydata2$LSOA_region, mydata2[myVars])
    df = df[df$age>= 30 & df$age <= 100,]
    if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
    if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
    if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
    ##run survival model
    ## S <- Surv((df$time-df$origin_time), df$outcome) #for using same origin 
    S <- Surv(df$origin_time,df$time, df$outcome)
    if (adj == "age" ) {Ib = coxph(S ~ ethnicity + age + I(age^2), data = df)}
    #if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + IMD_quintile +LSOA_region+ months +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
    #if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + IMD_quintile +LSOA_region+ months + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
    data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
    row.names(data) = NULL
    cox_fAGE[[j]] = data
    ##Kapplan Meyer or Cumulative Hazard
    S <- Surv(df$origin_time,df$time, df$outcome, type = "interval")
    surv <- survfit(S ~ ethnicity, data = df); 
    plots_fevent[[j]] =     ggsurvplot(surv, fun ="event", conf.int = F, legend = "right", break.time.by = 180, title = outcomes_text[i])
    plots_feventzoom[[j]] = ggsurvplot(surv, fun ="event", conf.int = F, legend = "right", ylim=c(0,y[i]),break.time.by = 180, title = outcomes_text[i])
    ##plots_fKM[[j]] = ggsurvplot(surv, conf.int = F )
    ##number for saving models and plots
    print(paste("loop number:",j, sep=" "))
    j = j+1 #number for saving models and plots
    r = r+1 #number for moving the ref group title
  }}

###Loop full adjusted: ----
adj = "reduced"
j= 1;
for (i in 1:length(outcomes)){
  print(outcomes_text[i])
  r=1
  for (eth in ethnicity){
    print(eth)
    #prepare data
    df = data.frame(outcome = mydata2[[outcomes[i]]], origin_time=mydata2$starttime_diff, time = mydata2[[followup[i]]], age = mydata2$age, ethnicity = mydata2[[eth]], LSOA_region=mydata2$LSOA_region, mydata2[myVars])
    df = df[df$age>= 30 & df$age <= 100,]
    if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
    if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
    if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
    #run survival model
    S <- Surv(df$origin_time,df$time, df$outcome)
    if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag  + IMD_quintile +LSOA_region+ months +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
    if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + IMD_quintile +LSOA_region+ months + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
    data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
    row.names(data) = NULL
    cox_fADJ[[j]] = data
    ##test proportional hazards
    # test.ph[[j]] <- cox.zph(Ib)
    #prepare data for the plot (include only ethnicity estimates)
    data = data.frame(ethnicity=factor(levels(df$ethnicity)[-1]), HR=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),1], low=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),3], high=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),4] )
    main_text = c(paste0("Risk of ", outcomes_text[i]," by ethnicity in women ranged 30 to 100 years\n(Reference group: ",reference_group[r],") [Adjusted]"))
    min_val = min(data$low) ; max_val =  max(data$high)
    #ethnicity order and color in the plot
    eth_order = levels(df$ethnicity)[-1]
    data$ethnicity = fct_rev( fct_relevel(data$ethnicity, eth_order) )
    data$color = colours[[eth]]
    #plot
    plots_fcoxADJ[[j]] = data %>%
      ggplot(aes(x=HR, y=ethnicity, fill=color)) + 
      geom_bar(stat="identity", alpha=.6, width=.4)+  #, position='dodge'
      geom_errorbar(aes(xmin = low, xmax = high), width=.2, alpha=0.6,colour="grey35") + #position=position_dodge(width = .45),
      geom_vline(xintercept=c(1), linetype="dotted") +
      coord_cartesian(xlim = c(0.75, max_val) ) +
      scale_fill_manual(values=c(rep(col2,2))) +
      theme_bw() + labs(title=main_text, x=paste("HR of", outcomes_text[i], "(95CI%)"), y = "Ethnicity groups")+ 
      theme(legend.position = "null")
    #name saved plot
    names(cox_fADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    names(plots_fcoxADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    #number for saving models and plots
    print(paste("loop number:",j, sep=" "))
    j = j+1 #number for saving models and plots
    r = r+1 #number for moving the ref group title
    gc()
  }}

rm(S, data, Ib); gc()


##EXPORT _______________________________________________________________________-----
#PLOTS: plots_fcumhaz,plots_fKM, plots_fcoxADJ
#TABLES: cox_fAGE, cox_fADJ  (no need for censoring as there are pure estimates)
#No export for Competing risk (Error: cannot allocate vector of size 7.9 Gb)

save_output = "YES"
path_file = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/Survival analysis/"

#_____________________
#PLOTS K-M and cumHaz
s_plot_list = c(plots_fKM,plots_fcumhaz)
if(save_output == "YES"){
  pdf_name = "women_paper_KM_CumHaz_plots_Feb2023_sizetest.pdf"
  cairo_pdf(paste0(path_file,pdf_name),width=13.71, height=6.95,onefile=T)  
  print(s_plot_list)
  dev.off()
}

#PLOTS event
s_plot_list = c(plots_fevent,plots_feventzoom)
if(save_output == "YES"){
  pdf_name = "women_paper_Cumulative_event_plots_Feb2023_sizetest2.pdf"
  cairo_pdf(paste0(path_file,pdf_name),width=18.31, height=8.17,onefile=T)  
  print(s_plot_list)
  dev.off()
}   

#PLOTS HR
s_plot_list = c(plots_fcoxADJ)
if(save_output == "YES"){
  pdf_name = "women_paper_HR_plots_Feb2023_sizetest.pdf"
  cairo_pdf(paste0(path_file,pdf_name),width=8.67, height=4.68,onefile=T)  
  print(s_plot_list)
  dev.off()
} 


#TABLES 
suvival_list = c(cox_fAGE, cox_fADJ); 
name = "women_cox_byAGE_byfullyADJ.csv"
if (save_output == "YES"){
  path_filename = paste0(path_file,name)
  
  for (i in 1:length(suvival_list)){
    if (i == 1){write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
  }
}


#
#plots_fKM[[1]]$data.survtable
head(plots_fKM[[1]]$data.survtable[c(1:3,6,9,10)])


#print(surv)
#path_file = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/Survival analysis/"
#name = "risk_table_K-M"
#path_filename = paste0(path_file,name)
write.table(outcomes_text[i],  path_filename, col.names = T, row.names=T, append= T, sep=',')
write.table((surv),  path_filename, col.names = T, row.names=T, append= T, sep=',')





# Explore vaccination status: ----

#Add vaccination flag:
mydata3<- dbGetQuery(con,"SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_02_vaccination_gdppr_women") 
data = inner_join(mydata2[c(1,49)],mydata3[c(4:6)]) %>% filter(covid_date>vaccination_dose1_date)
data$vaccination_flag = 1

mydata3 = left_join(mydata2,data)   
mydata3$vaccination_flag[is.na(mydata3$vaccination_flag)] = 0

## Include vaccination flag to HR ---      
#' No competing risk using "#starttime_diff & stoptime_diff_variables" for Surv(startTime, stopTime, event)
#library(survminer) #for ggsurvplot
#Variables that are necessary for the loop:
followup = stoptime_diff_variables 
outcomes_text = c("COVID-19 death at 28 days", "CVE at 30 days", "CVE at 1 year") #"COVID-19 death at 90 days"
#plot colour
if(eth == "ethnicity_5_group"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00"))} #reverse of (pink, blue, cian, green,  and yellow) red-salmon excluded  # See: library(scales); show_col(hue_pal()(6))
if(eth == "ONS_9eth"){col2 = c("#F8766D","#00BFC4","#00BA38","#619CFF","#F564E3")} #order: red-salmon,green,cian,blue and pink # See: library(scales); show_col(hue_pal()(6))
if(eth == "PrimaryCode_19cat"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#F8766D","#B79F00"))} #reverse of (pink, blue, cian, green, red-salmon and yellow)  # See: library(scales); show_col(hue_pal()(6))
cox_fADJ      = vector("list",length(models)*length(outcomes))
plots_fcoxADJ = vector("list",length(models)*length(outcomes))

###Loop full adjusted: ----
adj = "reduced"
j= 1;
for (i in 1:length(outcomes)){
  print(outcomes_text[i])
  r=1
  for (eth in ethnicity){
    print(eth)
    ##prepare data
    df = data.frame(outcome = mydata3[[outcomes[i]]], origin_time=mydata3$starttime_diff, time = mydata3[[followup[i]]], vaccination_flag = mydata3$vaccination_flag,age = mydata3$age, ethnicity = mydata3[[eth]], LSOA_region=mydata3$LSOA_region, mydata3[myVars])
    df = df[df$age>= 30 & df$age <= 100,]
    if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
    #if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
    if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
    ##run survival model
    S <- Surv(df$origin_time,df$time, df$outcome)
    #if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region+ months +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
    if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + vaccination_flag + IMD_quintile +LSOA_region+ months + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
    data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
    row.names(data) = NULL
    cox_fADJ[[j]] = data
    #prepare data for the plot (include only ethnicity estimates)
    data = data.frame(ethnicity=factor(levels(df$ethnicity)[-1]), HR=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),1], low=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),3], high=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),4] )
    main_text = c(paste0("Risk of ", outcomes_text[i]," by ethnicity in women ranged 30 to 100 years\n(Reference group: White",reference_group[r],") [Adjusted]"))
    min_val = min(data$low) ; max_val =  max(data$high)
    #ethnicity order and color in the plot
    eth_order = levels(df$ethnicity)[-1]
    data$ethnicity = fct_rev( fct_relevel(data$ethnicity, eth_order) )
    data$color = colours[[eth]]
    #plot
    plots_fcoxADJ[[j]] = data %>%
      ggplot(aes(x=HR, y=ethnicity, fill=color)) + 
      geom_bar(stat="identity", alpha=.6, width=.4)+  #, position='dodge'
      geom_errorbar(aes(xmin = low, xmax = high), width=.2, alpha=0.6,colour="grey35") + #position=position_dodge(width = .45),
      geom_vline(xintercept=c(1), linetype="dotted") +
      coord_cartesian(xlim = c(0.75, max_val) ) +
      scale_fill_manual(values=c(rep(col2,2))) +
      theme_bw() + labs(title=main_text, x=paste("HR of", outcomes_text[i], "(95CI%)"), y = "Ethnicity groups")+ 
      theme(legend.position = "null")
    #name saved plot
    names(cox_fADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    names(plots_fcoxADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    #number for saving models and plots
    print(paste("loop number:",j, sep=" "))
    j = j+1 #number for saving models and plots
    r = r+1 #number for moving the ref group title
    gc()
  }}
##EXPORT _______________________________________________________________________-----
#PLOTS: plots_fcoxADJ
#TABLES:cox_fADJ  (no need for censoring as there are pure estimates)
save_output = "YES"
path_file = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/Survival analysis/"
#PLOTS HR
s_plot_list = c(plots_fcoxADJ)
if(save_output == "YES"){
  pdf_name = "women_paper_HR_plots_incl_vaccination_Feb2023.pdf"
  cairo_pdf(paste0(path_file,pdf_name),width=8.67, height=4.68,onefile=T)  
  print(s_plot_list)
  dev.off()
}
#TABLES 
suvival_list = c(cox_fADJ); 
name = "women_cox_yfullyADJ_incl_vaccination.csv"
if (save_output == "YES"){
  path_filename = paste0(path_file,name)
  
  for (i in 1:length(suvival_list)){
    if (i == 1){write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
  }
}


#________________________
#
#Stratify by 1 year and next as Cum event plot
#
#_____________
###Loop full adjusted: ----
adj = "reduced"
j= 1;
for (i in 1:length(outcomes)){
  print(outcomes_text[i])
  r=1
  for (eth in ethnicity){
    print(eth)
    ##prepare data
    df = data.frame(outcome = mydata3[[outcomes[i]]], origin_time=mydata3$starttime_diff, time = mydata3[[followup[i]]], vaccination_flag = mydata3$vaccination_flag,age = mydata3$age, ethnicity = mydata3[[eth]], LSOA_region=mydata3$LSOA_region, mydata3[myVars])
    #df = df %>% filter(origin_time<=365)
    df = df %>% filter(origin_time>365)
    df = df[df$age>= 30 & df$age <= 100,]
    #if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
    #if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
    if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
    ##run survival model
    S <- Surv(df$origin_time,df$time, df$outcome)
    #if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region+ months +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
    if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region+ months + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
    data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
    row.names(data) = NULL
    cox_fADJ[[j]] = data
    #prepare data for the plot (include only ethnicity estimates)
    data = data.frame(ethnicity=factor(levels(df$ethnicity)[-1]), HR=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),1], low=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),3], high=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),4] )
    main_text = c(paste0("Risk of ", outcomes_text[i]," by ethnicity in women ranged 30 to 100 years\n(Reference group: White ",reference_group[r],") [Adjusted]"))
    min_val = min(data$low) ; max_val =  max(data$high)
    #ethnicity order and color in the plot
    eth_order = levels(df$ethnicity)[-1]
    data$ethnicity = fct_rev( fct_relevel(data$ethnicity, eth_order) )
    data$color = colours[[eth]]
    #plot
    plots_fcoxADJ[[j]] = data %>%
      ggplot(aes(x=HR, y=ethnicity, fill=color)) + 
      geom_bar(stat="identity", alpha=.6, width=.4)+  #, position='dodge'
      geom_errorbar(aes(xmin = low, xmax = high), width=.2, alpha=0.6,colour="grey35") + #position=position_dodge(width = .45),
      geom_vline(xintercept=c(1), linetype="dotted") +
      coord_cartesian(xlim = c(0.75, max_val) ) +
      scale_fill_manual(values=c(rep(col2,2))) +
      theme_bw() + labs(title=main_text, x=paste("HR of", outcomes_text[i], "(95CI%)"), y = "Ethnicity groups")+ 
      theme(legend.position = "null")
    #name saved plot
    names(cox_fADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    names(plots_fcoxADJ)[j] = print(paste(outcomes_text[i],": women",eth, sep = ""))
    #number for saving models and plots
    print(paste("loop number:",j, sep=" "))
    j = j+1 #number for saving models and plots
    r = r+1 #number for moving the ref group title
    gc()
  }}
#p=list(); c=list()

p = c(p,plots_fcoxADJ)
c = c(c,cox_fADJ)

#TABLES:cox_fADJ  (no need for censoring as there are pure estimates)
save_output = "YES"
path_file = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/Survival analysis/"
#PLOTS HR
if(save_output == "YES"){
  pdf_name = "women_paper_HR_plots_incl_vaccination_by365strata.pdf"
  cairo_pdf(paste0(path_file,pdf_name),width=8.67, height=4.68,onefile=T)  
  print(p)
  dev.off()
}
#TABLES 
suvival_list = c; 
name = "women_cox_yfullyADJ_incl_vaccination_by365strata.csv"
if (save_output == "YES"){
  path_filename = paste0(path_file,name)
  
  for (i in 1:length(suvival_list)){
    if (i == 1){write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    else {write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
  }
}
