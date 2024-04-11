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

## Load database CCU037_02_cohort ------------------------
## (replace this section code to load your data, I added the pregnancy flag and vaccination later, that's why it's not included in the RData file): 
#CCU037_02_final_cohort // CCU037_02_cohort
    mydata <- readRDS("/mnt/efs/marta.pinedamoncusi/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/CCU037_02_cohort.rds")
    mydata$PrimaryCode[mydata$PrimaryCode == "X"] <- "Z"
    #Add pregnancy flag:
    mydata2<- dbGetQuery(con,"SELECT NHS_NUMBER_DEID,preg_flag FROM dars_nic_391419_j3w9t_collab.ccu037_02_pregnancy_flag") #
    mydata$preg_flag = ifelse(mydata$NHS_NUMBER_DEID %in% mydata2$NHS_NUMBER_DEID,1,0) #mydata = left_join(mydata,mydata2) #, by=c("NHS_NUMBER_DEID"="person_id_deid"))
    rm(mydata2)
    #Add vaccination flag:
    mydata3<- dbGetQuery(con,"SELECT * FROM dars_nic_391419_j3w9t_collab.ccu037_02_vaccination_gdppr_women") 
    data = inner_join(mydata[c(1,49)],mydata3[c(4:6)]) %>% filter(covid_date>vaccination_dose1_date)
    data$vaccination_flag = 1
    mydata = left_join(mydata,data)   
    mydata$vaccination_flag[is.na(mydata3$vaccination_flag)] = 0

## Prepare variables for covid death outcomes ------------------------------------------------
## NOTE: For each outcome I create a binary variable (had the event, yes or no), and a numeric variable with time to event). 
##       If you create the variables already, delete this section.
##       We will only use covid-death at 28 , and CVD at 30 days.
#' Get IR covid-death at 28 and 90 days from covid diagnosis by time/waves: -----
  #Death variable
    mydata$death28d = ifelse(mydata$covid_death==1 & mydata$follow_up_days<=28, 1,0) #during next 28 dates from covid diagnosis
    #mydata$death90d = ifelse(mydata$covid_death==1 & mydata$follow_up_days<=90, 1,0) #manuscript definition + 90 days window
  #Time daysto_death censored at 28 and 90 days
    mydata$daysto_death28d = ifelse(mydata$follow_up_days>28,28,mydata$follow_up_days)
    #mydata$daysto_death90d = ifelse(mydata$follow_up_days>90,90,mydata$follow_up_days)

#' Prepare variables for CVD outcomes --------------------------------------------------------
  #' Get IR CVE at 30 and 365 days from covid diagnosis by time/waves: ------------
  # Include event day for CVE and mantain follow_up_days for those who did not had an event
    mydata$daysto_30dCVE     =  ifelse(mydata$post_covid_cvd_event_30d == 1, mydata$date_cvd_30d-mydata$covid_date,mydata$follow_up_days)
    #mydata$daysto_1yCVE      =  ifelse(mydata$post_covid_cvd_event_1y  == 1, mydata$date_cvd_1y-mydata$covid_date,mydata$follow_up_days)
  # Censor the follow-up/the days t:o 1 year for covid death and 1yCVD, and 30d for 30dCVD:
    mydata$daysto_30dCVE =  ifelse(mydata$daysto_30dCVE>30,  30, mydata$daysto_30dCVE)
    #mydata$daysto_1yCVE  =  ifelse(mydata$daysto_1yCVE>365,  365,mydata$daysto_1yCVE)

    
## List the outcomes we want to run:
## from "death28d","death90d","post_covid_cvd_event_30d","post_covid_cvd_event_1y", we only want  "death28d" and "post_covid_cvd_event_30d"
outcomes = c("death28d","post_covid_cvd_event_30d") #,"covid_hospitalisation" = we don't have date of hospitalisation
days_variables  = c("daysto_death28d","daysto_30dCVE")


## Include the legend to ethnicity variable: from number to text. 
## The following lines is how I defined the 9 categories.
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
  
  ## NOTE:
  ## The order is important for the Plots later. The order relies on the 6 main groups categories: Asian, Black, Mixed, Other and White (and then Unknown).
  ## Thus, the first are the Asian subgroups alphabetically ordered, then the Black subgroups, etc. 
  ## The 6 categories hasn't been transformed in the code as I assumed they are already generated with their labels, but if not, please create a text and order them alphabetically.
  ## Finally, line 83 state that the reference group is the White British (for the comparisons).
  #Order 9cat
  ord =c("Bangladeshi","Chinese","Indian","Pakistani","Black African","Black Caribbean","Mixed","Other","White British","White other")        
  mydata$ONS_9eth = fct_relevel(mydata$ONS_9eth, ord)
  mydata$ONS_9eth = relevel(mydata$ONS_9eth, ref = "White British")
  

##' Prepare time/waives 
##' NOTE: please add the last time period, as you mentioned your data goes up to end of 2022 isn't it?
  #every 6 months
  mydata$months[mydata$covid_date>="2020-01-23" & mydata$covid_date<="2020-06-30"] = "23 Jan 2020 to 30 Jun 2020" #from covid start at 23 Jan 2020
  mydata$months[mydata$covid_date>="2020-07-01" & mydata$covid_date<="2020-12-31"] = "01 Jul 2020 to 31 Dec 2020"
  mydata$months[mydata$covid_date>="2021-01-01" & mydata$covid_date<="2021-06-30"] = "01 Jan 2021 to 30 Jun 2021"
  mydata$months[mydata$covid_date>="2021-07-01" & mydata$covid_date<="2021-12-31"] = "01 Jul 2021 to 31 Dec 2021"
  mydata$months[mydata$covid_date>="2022-01-01" & mydata$covid_date<="2022-06-29"] = "01 Jan 2022 to 01 Apr 2022" #end data availability was "29 Jun 2022", but there are no cases of covid diagnosis once the home LFT were not free in the UK (1st Apr 2022).
  mydata$months = factor(mydata$months, 
                         levels = c("23 Jan 2020 to 30 Jun 2020","01 Jul 2020 to 31 Dec 2020","01 Jan 2021 to 30 Jun 2021","01 Jul 2021 to 31 Dec 2021", "01 Jan 2022 to 01 Apr 2022"), 
                         labels = c("23 Jan 2020 to\n30 Jun 2020","01 Jul 2020 to\n31 Dec 2020","01 Jan 2021 to\n30 Jun 2021","01 Jul 2021 to\n31 Dec 2021", "01 Jan 2022 to\n01 Apr 2022") )
  list_months = levels(mydata$months) #needed for HR stratified by months

## Censor analysis up to 31 Dec 2021 -------------------------------------------
#dim(mydata)
#mydata = mydata[mydata$covid_date<="2021-12-31",]



#AGE-adjusted models____________________________________________________________----

## Mortality models only adjusted by age, for 6_cat and 9_cat: ------------------
## NOTE: Thus runs for both, men and women. This is the "simplest model, only age adjusted".
##       The ONS reported in their analysis that Age needed a second order interaction. I checked in our data and we need it as well for our England data.  
## NOTE: The lines 115-117 include the colors for the bars in ggplot syntax. As a legacy, I kept the 19 color codes. But line 114 determines which categories we include (i.e., only 6 and 9).
  ethnicity = c("ethnicity_5_group","ONS_9eth") #,"PrimaryCode_19cat"
  colours_labels = list(ethnicity_5_group = data.frame( code = names(table(mydata$ethnicity_5_group)), color = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BFC4","#00BA38") ),
                        ONS_9eth = data.frame( code = names(table(mydata$ONS_9eth)) , color = c(rep("#F8766D",4),rep("#F564E3",2),"#619CFF","#B79F00",rep("#00BA38",2)) ),
                        PrimaryCode_19cat = data.frame( code = names(table(mydata$PrimaryCode_19cat)) , color = c( rep("#F8766D",5),rep("#F564E3",3), rep("#B79F00",4),rep("#619CFF",2), rep("#00BA38",4),"#00BFC4") ) )
    
  reference_group = c("White","White British","British")
  outcomes_text = c("COVID-19 death at 28 days","CVE at 30 days") #, "COVID-19 death at 90 days", "CVE at 1 year")
  followup = days_variables[1:2] 
  #Create two lists to store the results: one for tables and one for plots. Include the names of the outcomes for easier checking later.
  mod_names = c("male_6_cat","male_9_cat","female_6_cat","female_9_cat")
  cox_AGEadj  = vector("list",length(mod_names)*2) ; names(cox_AGEadj)  = c(paste("28-days death",mod_names,sep=" "),paste("30-days CVD",mod_names,sep=" "))
  plots_AGEadj= vector("list",length(mod_names)*2) ; names(plots_AGEadj)= c(paste("28-days death",mod_names,sep=" "),paste("30-days CVD",mod_names,sep=" "))

#Age adjustment loop:
j= 1
for (i in 1:length(outcomes)){  # outcomes defined in line 55
  for (sex in 1:2){
    r=1
    for (eth in ethnicity){
      #print(r)
      #prepare data
      df = data.frame(outcome = mydata[[outcomes[i]]], time = mydata[[followup[i]]], age = mydata$age, sex = mydata$sex, ethnicity = mydata[[eth]])
      df = df[df$sex == sex & df$age>= 30 & df$age <= 100,-4]
      if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")}        # else {df$ethnicity = relevel(factor(df$ethnicity), ref = "A")}
      if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")} # else {df$ethnicity = relevel(factor(df$ethnicity), ref = "A")}
      #if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      # else {df$ethnicity = relevel(factor(df$ethnicity), ref = "A")}
      #run survival model
      S <- Surv(df$time, df$outcome)
      Ib = coxph(S ~ age + I(age^2) + ethnicity , data = df)
      #prepare data for the plot
      data = data.frame(ethnicity = factor(levels(df$ethnicity)[-1]), HR = summary(Ib)$conf.int[-c(1,2),1], low = summary(Ib)$conf.int[-c(1,2),3], high = summary(Ib)$conf.int[-c(1,2),4])
      cox_AGEadj[[j]] = data
      main_text = c(paste("Risk of", outcomes_text[i],"by ethnicity in males ranged 30 to 100 years\n(Reference group:",reference_group[r],")"),
                    paste("Risk of", outcomes_text[i],"by ethnicity in females ranged 30 to 100 years\n(Reference group:",reference_group[r],")"))
      min_val = min(data$low) ; max_val =  max(data$high)
      #data$color = colours[[eth]]
      data<-  left_join(data, colours_labels[[eth]], by=c("ethnicity"="code")) 
      eth_order = levels(df$ethnicity)[-1]
      data$ethnicity = fct_rev( fct_relevel(data$ethnicity, eth_order))
      #plot
      plots_AGEadj[[j]] = data %>%
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


# Running Survival analysis in women, adjusting for different confounders: ___________________________________________________________ ----
  ## Create a subset:
    mydata2 = mydata[mydata$sex == "2",]
  ## Merge some conditions into the same variable 
  ## NOTE: you might already have this merged in your dataset.
    mydata2$chronic_mental_health_disorders = ifelse((mydata2$schizophrenia==1)|(mydata2$bipolardisorder==1)|(mydata2$depression==1),1,0)
    mydata2$CVD_prevention_medication = ifelse((mydata2$anti_hypertensive_drugs==1)|(mydata2$anti_coagulant_drugs==1)|(mydata2$anti_platelet_drugs==1)|(mydata2$statins==1),1,0)    
    mydata2$LSOA_region = ifelse(is.na(mydata2$region_name),"Unknown",mydata2$region_name)    
  #Empty memory space:
  #rm(mydata); gc()


  ## NOTE: To correct the different time when covid was diagnosed, we include the "months" variable but also we have into account the time from the start of the pandemic until the positive test, and the time at risk for the event.
  ##       To do so, 
  ##       we are including in the cox model the difference in days from the start of the pandemic (date_origin = start_date = 23 Jan 2020) to the date when the patient tested positive for covid (date_enrollment = covid_date) 
  ##       and the time difference between the end of follow up and the start of the pandemic.
  #starttime_diff: difference(date_enrollment - date_origin) #date_origin = start_date
  start_date = "2020-01-23"
  mydata2$starttime_diff = mydata2$covid_date-start_date
  #stoptime_diff: difference(date_stop - date_origin) #date_origin = start_date
  #Alternative to get the same: since we have time censored and will use more lines, we use days_to_EVENT and add the days from starttime_diff
  mydata2$stoptime_diff_28dDeath = mydata2$daysto_death28d + mydata2$starttime_diff +0.1
  mydata2$stoptime_diff_30dCVD   = mydata2$daysto_30dCVE + mydata2$starttime_diff +0.1


  ## Necessary variables for the cox models and plots
  ## Note: includes which models are we running, which are the variables we will need in the loop, the text in the plots, etc. 
  outcomes = c("death28d","post_covid_cvd_event_30d")
  days_variables = c("daysto_death28d","daysto_30dCVE")
  stoptime_diff_variables = c("stoptime_diff_28dDeath","stoptime_diff_30dCVD")
  outcomes_text = c("COVID-19 death at 28 days", "CVE at 30 days")
  ethnicity = c("ethnicity_5_group","ONS_9eth") #c("ethnicity_5_group","ONS_9eth","PrimaryCode_19cat")
  models = c('(female 6_cat)','(female 9_cat)') #c('(female 6_cat)','(female 9_cat)','(female 19_cat)')
  reference_group = c("White","British")        #c("White","British")

  ## Select which clinical variables  we want to include int he adjustment (this list does not include demographics): use the "reduced"
  ## Note: the reduce uses the merged variables, like chronic_mental_health_disorders rather than ["schizophrenia","bipolardisorder","depression"] alone.  For legacy, I have kept the larger list. 
  ##       Please check the variables included in the reduce vector and remove those who are not available in SAIL.
  ##       Later we also include vaccination, LSOA, age and ethnicity, manually.  
  adj = "reduced" #(options "larger" or "reduced". "larger" for not merged variables; "reduced" for merged chronic mental disordes and CVD medication)
  if (adj == "larger")  {myVars = c("preg_flag","IMD_quintile","months","AF","CKD","diabetes","schizophrenia","bipolardisorder","depression","RA","anti_hypertensive_drugs","antipsychotic","cancer","copd","dementia","hypertension","alcoholic_liver_disease","anti_coagulant_drugs","anti_platelet_drugs","statins","obesity")}
  if (adj == "reduced") {myVars = c("preg_flag","IMD_quintile","months","AF","CKD","diabetes","RA","chronic_mental_health_disorders","CVD_prevention_medication","antipsychotic","cancer","copd","dementia","hypertension","alcoholic_liver_disease","obesity")}
  dim(mydata2[myVars])
  
  #Check the follow-up variables:
  followup = stoptime_diff_variables 
  #create empty lists to store results
  cox_fADJ      = vector("list",length(models)*length(outcomes))
  plots_fcoxADJ = vector("list",length(models)*length(outcomes))

  ###Loop full adjusted: ----
  ## Note: The ONS reported in their analysis that Age needed a second order interaction. I checked in our data and we need it as well for our England data.  
  adj = "reduced"
  j= 1;
  for (i in 1:length(outcomes)){
    print(outcomes_text[i])
    r=1
    for (eth in ethnicity){
      print(eth)
     #plot colour (includes the colours of the bars)
      if(eth == "ethnicity_5_group"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00"))} #reverse of (pink, blue, cian, green,  and yellow) red-salmon excluded  # See: library(scales); show_col(hue_pal()(6))
      if(eth == "ONS_9eth"){col2 = c("#F8766D","#00BFC4","#00BA38","#619CFF","#F564E3")} #order: red-salmon,green,cian,blue and pink # See: library(scales); show_col(hue_pal()(6))
      #if(eth == "PrimaryCode_19cat"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#F8766D","#B79F00"))} #reverse of (pink, blue, cian, green, red-salmon and yellow)  # See: library(scales); show_col(hue_pal()(6))
      ##prepare data
      df = data.frame(outcome = mydata2[[outcomes[i]]], origin_time=mydata2$starttime_diff, time = mydata2[[followup[i]]], vaccination_flag = mydata2$vaccination_flag,age = mydata2$age, ethnicity = mydata2[[eth]], LSOA_region=mydata2$LSOA_region, mydata2[myVars])
      df = df[df$age>= 30 & df$age <= 100,]
      if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
      if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
      #if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
      ##run survival model
      S <- Surv(df$origin_time,df$time, df$outcome)
      #if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region+ months +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
      if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region+ months + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
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
  #Stratify by months
  #
  #_____________
  ###Loop full adjusted: ----
  list_months = levels(mydata$months)
  adj = "reduced"
  j= 1;
  for (i in 1:length(outcomes)){
    print(outcomes_text[i])
  for (m in list_months){
    r=1
    for (eth in ethnicity){
      print(eth)
      ##prepare data
      df = data.frame(outcome = mydata2[[outcomes[i]]], origin_time=mydata2$starttime_diff, time = mydata2[[followup[i]]], vaccination_flag = mydata2$vaccination_flag,age = mydata2$age, ethnicity = mydata2[[eth]], LSOA_region=mydata2$LSOA_region, mydata2[myVars])
      df = df %>% filter(months=m)
      df = df[df$age>= 30 & df$age <= 100,]
      if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
      if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
      #if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
      ##run survival model
      S <- Surv(df$origin_time,df$time, df$outcome)
      #if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
      if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
      data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
      row.names(data) = NULL
      cox_fADJ[[j]] = data
      #prepare data for the plot (include only ethnicity estimates)
      data = data.frame(ethnicity=factor(levels(df$ethnicity)[-1]), HR=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),1], low=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),3], high=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),4] )
      #main_text = c(paste0("Risk of ", outcomes_text[i]," by ethnicity in women ranged 30 to 100 years\n(Reference group: White ",reference_group[r],") [Adjusted]"))
      main_text = c(paste("Enrolment period:",gsub("\n"," ",m)))
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
    }}}
  
  plots_28death = ggarrange(plots_fcoxADJ[[1]],plots_fcoxADJ[[2]],plots_fcoxADJ[[3]],plots_fcoxADJ[[4]],plots_fcoxADJ[[5]], ncol = 2, nrow = 3)
  cox_28death   = cox_fADJ[c(1:5)]
  plots_30CVD = ggarrange(plots_fcoxADJ[[6]],plots_fcoxADJ[[7]],plots_fcoxADJ[[8]],plots_fcoxADJ[[9]],plots_fcoxADJ[[10]], ncol = 2, nrow = 3)
  cox_30CVD   = cox_fADJ[c(6:10)]
  
  #TABLES:cox_fADJ  (no need for censoring as there are pure estimates)
  save_output = "YES"
  path_file = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/Survival analysis/"
  #PLOTS HR
  if(save_output == "YES"){
    pdf_name = "women_paper_HR_plots_incl_vaccination_bymonths_enrolment.pdf"
    cairo_pdf(paste0(path_file,pdf_name),width=8.67, height=4.68,onefile=T)  
    print(plots_28death)
    print(plots_30CVD)
    dev.off()
  }
  #TABLES 
  suvival_list = c(cox_28death,cox_30CVD)
  name = "women_cox_yfullyADJ_incl_vaccination_bymonths_enrolment.csv"
  if (save_output == "YES"){
    path_filename = paste0(path_file,name)
    
    for (i in 1:length(suvival_list)){
      if (i == 1){write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
      else {write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    }
  }

  
# Running Survival analysis in men, adjusting for different confounders: ___________________________________________________________ ----
  ## Create a subset:
    mydata3 = mydata[mydata$sex == "1",]
  ## Merge some conditions into the same variable 
  ## NOTE: you might already have this merged in your dataset.
    mydata3$chronic_mental_health_disorders = ifelse((mydata3$schizophrenia==1)|(mydata3$bipolardisorder==1)|(mydata3$depression==1),1,0)
    mydata3$CVD_prevention_medication = ifelse((mydata3$anti_hypertensive_drugs==1)|(mydata3$anti_coagulant_drugs==1)|(mydata3$anti_platelet_drugs==1)|(mydata3$statins==1),1,0)    
    mydata3$LSOA_region = ifelse(is.na(mydata3$region_name),"Unknown",mydata3$region_name)    
  #Empty memory space:
  #rm(mydata2); gc()
  
  
  ## NOTE: To correct the different time when covid was diagnosed, we include the "months" variable but also we have into account the time from the start of the pandemic until the positive test, and the time at risk for the event.
  ##       To do so, 
  ##       we are including in the cox model the difference in days from the start of the pandemic (date_origin = start_date = 23 Jan 2020) to the date when the patient tested positive for covid (date_enrollment = covid_date) 
  ##       and the time difference between the end of follow up and the start of the pandemic.
  #starttime_diff: difference(date_enrollment - date_origin) #date_origin = start_date
  start_date = "2020-01-23"
  mydata3$starttime_diff = mydata3$covid_date-start_date
  #stoptime_diff: difference(date_stop - date_origin) #date_origin = start_date
  #Alternative to get the same: since we have time censored and will use more lines, we use days_to_EVENT and add the days from starttime_diff
  mydata3$stoptime_diff_28dDeath = mydata3$daysto_death28d + mydata3$starttime_diff +0.1
  mydata3$stoptime_diff_30dCVD   = mydata3$daysto_30dCVE + mydata3$starttime_diff +0.1
  
  
  ## Necessary variables for the cox models and plots
  ## Note: includes which models are we running, which are the variables we will need in the loop, the text in the plots, etc. 
  outcomes = c("death28d","post_covid_cvd_event_30d")
  days_variables = c("daysto_death28d","daysto_30dCVE")
  stoptime_diff_variables = c("stoptime_diff_28dDeath","stoptime_diff_30dCVD")
  outcomes_text = c("COVID-19 death at 28 days", "CVE at 30 days")
  ethnicity = c("ethnicity_5_group","ONS_9eth") #c("ethnicity_5_group","ONS_9eth","PrimaryCode_19cat")
  models = c('(female 6_cat)','(female 9_cat)') #c('(female 6_cat)','(female 9_cat)','(female 19_cat)')
  reference_group = c("White","British")        #c("White","British")
  
  ## Select which clinical variables  we want to include int he adjustment (this list does not include demographics): use the "reduced"
  ## Note: Please copy=paste the same lines modified for women reduced adjustment.
  ##       This list also include preg_flag. It doesn't really care as the men's adjustment doesn't include it later. (You can remove it from here or kept it).
  adj = "reduced" #(options "larger" or "reduced". "larger" for not merged variables; "reduced" for merged chronic mental disordes and CVD medication)
  if (adj == "larger")  {myVars = c("preg_flag","IMD_quintile","months","AF","CKD","diabetes","schizophrenia","bipolardisorder","depression","RA","anti_hypertensive_drugs","antipsychotic","cancer","copd","dementia","hypertension","alcoholic_liver_disease","anti_coagulant_drugs","anti_platelet_drugs","statins","obesity")}
  if (adj == "reduced") {myVars = c("preg_flag","IMD_quintile","months","AF","CKD","diabetes","RA","chronic_mental_health_disorders","CVD_prevention_medication","antipsychotic","cancer","copd","dementia","hypertension","alcoholic_liver_disease","obesity")}
  dim(mydata3[myVars])
  
  #Check the follow-up variables:
  followup = stoptime_diff_variables 
  #create empty lists to store results
  cox_mADJ      = vector("list",length(models)*length(outcomes))
  plots_fcoxADJ = vector("list",length(models)*length(outcomes))
  
  ###Loop full adjusted: ----
  ## Note: The ONS reported in their analysis that Age needed a second order interaction. I checked in our data and we need it as well for our England data.  
  adj = "reduced"
  j= 1;
  for (i in 1:length(outcomes)){
    print(outcomes_text[i])
    r=1
    for (eth in ethnicity){
      print(eth)
      #plot colour (includes the colours of the bars)
      if(eth == "ethnicity_5_group"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00"))} #reverse of (pink, blue, cian, green,  and yellow) red-salmon excluded  # See: library(scales); show_col(hue_pal()(6))
      if(eth == "ONS_9eth"){col2 = c("#F8766D","#00BFC4","#00BA38","#619CFF","#F564E3")} #order: red-salmon,green,cian,blue and pink # See: library(scales); show_col(hue_pal()(6))
      #if(eth == "PrimaryCode_19cat"){col2 = rev(c("#F564E3","#619CFF","#00BFC4","#00BA38","#F8766D","#B79F00"))} #reverse of (pink, blue, cian, green, red-salmon and yellow)  # See: library(scales); show_col(hue_pal()(6))
      ##prepare data
      df = data.frame(outcome = mydata3[[outcomes[i]]], origin_time=mydata3$starttime_diff, time = mydata3[[followup[i]]], vaccination_flag = mydata3$vaccination_flag,age = mydata3$age, ethnicity = mydata3[[eth]], LSOA_region=mydata3$LSOA_region, mydata3[myVars])
      df = df[df$age>= 30 & df$age <= 100,]
      if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
      if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
      #if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
      ##run survival model
      S <- Surv(df$origin_time,df$time, df$outcome)
      #if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + vaccination_flag + IMD_quintile +LSOA_region+ months +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
      if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + vaccination_flag + IMD_quintile +LSOA_region+ months + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
      data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
      row.names(data) = NULL
      cox_mADJ[[j]] = data
      #prepare data for the plot (include only ethnicity estimates)
      data = data.frame(ethnicity=factor(levels(df$ethnicity)[-1]), HR=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),1], low=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),3], high=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),4] )
      main_text = c(paste0("Risk of ", outcomes_text[i]," by ethnicity in men ranged 30 to 100 years\n(Reference group: White",reference_group[r],") [Adjusted]"))
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
      names(cox_mADJ)[j] = print(paste(outcomes_text[i],": men",eth, sep = ""))
      names(plots_fcoxADJ)[j] = print(paste(outcomes_text[i],": men",eth, sep = ""))
      #number for saving models and plots
      print(paste("loop number:",j, sep=" "))
      j = j+1 #number for saving models and plots
      r = r+1 #number for moving the ref group title
      gc()
    }}
  ##EXPORT _______________________________________________________________________-----
  #PLOTS: plots_fcoxADJ
  #TABLES:cox_mADJ  (no need for censoring as there are pure estimates)
  save_output = "YES"
  path_file = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/Survival analysis/"
  #PLOTS HR
  s_plot_list = c(plots_fcoxADJ)
  if(save_output == "YES"){
    pdf_name = "men_paper_HR_plots_incl_vaccination_Feb2023.pdf"
    cairo_pdf(paste0(path_file,pdf_name),width=8.67, height=4.68,onefile=T)  
    print(s_plot_list)
    dev.off()
  }
  #TABLES 
  suvival_list = c(cox_mADJ); 
  name = "men_cox_yfullyADJ_incl_vaccination.csv"
  if (save_output == "YES"){
    path_filename = paste0(path_file,name)
    
    for (i in 1:length(suvival_list)){
      if (i == 1){write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
      else {write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    }
  }
  
  
  #________________________
  #
  #Stratify by months
  #
  #_____________
  ###Loop full adjusted: ----
  list_months = levels(mydata$months)
  adj = "reduced"
  j= 1;
  for (i in 1:length(outcomes)){
    print(outcomes_text[i])
    for (m in list_months){
      r=1
      for (eth in ethnicity){
        print(eth)
        ##prepare data
        df = data.frame(outcome = mydata3[[outcomes[i]]], origin_time=mydata3$starttime_diff, time = mydata3[[followup[i]]], vaccination_flag = mydata3$vaccination_flag,age = mydata3$age, ethnicity = mydata3[[eth]], LSOA_region=mydata3$LSOA_region, mydata3[myVars])
        df = df %>% filter(months=m)
        df = df[df$age>= 30 & df$age <= 100,]
        if (eth == "ethnicity_5_group") {df$ethnicity = relevel(factor(df$ethnicity), ref = "White")} 
        if (eth == "ONS_9eth")          {df$ethnicity = relevel(factor(df$ethnicity), ref = "White British")}
        #if (eth == "PrimaryCode_19cat") {df$ethnicity = relevel(factor(df$ethnicity), ref = "British")}      
        ##run survival model
        S <- Surv(df$origin_time,df$time, df$outcome)
        #if (adj == "larger" ) {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region +AF + obesity + CKD + diabetes + schizophrenia + bipolardisorder + depression + RA + anti_hypertensive_drugs + antipsychotic + cancer + copd + dementia + hypertension + anti_coagulant_drugs + anti_platelet_drugs + statins , data = df)}
        if (adj == "reduced") {Ib = coxph(S ~ age + I(age^2) + ethnicity + preg_flag + vaccination_flag + IMD_quintile +LSOA_region + AF + obesity + CKD + diabetes + chronic_mental_health_disorders + CVD_prevention_medication + RA + antipsychotic + cancer + copd + dementia + hypertension , data = df)}
        data = data.frame(variables = rownames(summary(Ib)$conf.int), HR = summary(Ib)$conf.int[,1], low = summary(Ib)$conf.int[,3], high = summary(Ib)$conf.int[,4])
        row.names(data) = NULL
        cox_mADJ[[j]] = data
        #prepare data for the plot (include only ethnicity estimates)
        data = data.frame(ethnicity=factor(levels(df$ethnicity)[-1]), HR=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),1], low=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),3], high=summary(Ib)$conf.int[c(3:(1+length(levels(df$ethnicity)))),4] )
        #main_text = c(paste0("Risk of ", outcomes_text[i]," by ethnicity in men ranged 30 to 100 years\n(Reference group: White ",reference_group[r],") [Adjusted]"))
        main_text = c(paste("Enrolment period:",gsub("\n"," ",m)))
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
        names(cox_mADJ)[j] = print(paste(outcomes_text[i],": men",eth, sep = ""))
        names(plots_fcoxADJ)[j] = print(paste(outcomes_text[i],": men",eth, sep = ""))
        #number for saving models and plots
        print(paste("loop number:",j, sep=" "))
        j = j+1 #number for saving models and plots
        r = r+1 #number for moving the ref group title
        gc()
      }}}
  
  plots_28death = ggarrange(plots_fcoxADJ[[1]],plots_fcoxADJ[[2]],plots_fcoxADJ[[3]],plots_fcoxADJ[[4]],plots_fcoxADJ[[5]], ncol = 2, nrow = 3)
  cox_28death   = cox_mADJ[c(1:5)]
  plots_30CVD = ggarrange(plots_fcoxADJ[[6]],plots_fcoxADJ[[7]],plots_fcoxADJ[[8]],plots_fcoxADJ[[9]],plots_fcoxADJ[[10]], ncol = 2, nrow = 3)
  cox_30CVD   = cox_mADJ[c(6:10)]
  
  #TABLES:cox_mADJ  (no need for censoring as there are pure estimates)
  save_output = "YES"
  path_file = "~/dars_nic_391419_j3w9t_collab/CCU037/CCU037_02/Results/Survival analysis/"
  #PLOTS HR
  if(save_output == "YES"){
    pdf_name = "men_paper_HR_plots_incl_vaccination_bymonths_enrolment.pdf"
    cairo_pdf(paste0(path_file,pdf_name),width=8.67, height=4.68,onefile=T)  
    print(plots_28death)
    print(plots_30CVD)
    dev.off()
  }
  #TABLES 
  suvival_list = c(cox_28death,cox_30CVD)
  name = "men_cox_yfullyADJ_incl_vaccination_bymonths_enrolment.csv"
  if (save_output == "YES"){
    path_filename = paste0(path_file,name)
    
    for (i in 1:length(suvival_list)){
      if (i == 1){write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
      else {write.table(suvival_list[[i]],  path_filename, col.names = T, row.names=F, append= T, sep=',')}
    }
  }