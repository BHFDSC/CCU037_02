#==================================================================================================================================================================================================#
# 
# DATA LOAD & PACKAGES, Functions, etc.
#
#==================================================================================================================================================================================================#

#--------------------------------------------------------------------------------------------------#
# Load Packages                                                                                       ----
#--------------------------------------------------------------------------------------------------#
#Library for horizontal interactive/collapsible Tree [saved in htlm]
#NOTE: Better to produce in R studio:
#install.packages('collapsibleTree')
#library(collapsibleTree)

# Library for managing strings
library(stringr)

# Libraries for vertical tree (non interactive/collapsible) [saved in tiff]
#library(ggraph)
#library(igraph)
library(tidyverse)

# Library for sunburst plot
library(plotly)
#remotes::install_github("timelyportfolio/sunburstR")

#--------------------------------------------------------------------------------------------------#
# Functions                                                                                           ----
#--------------------------------------------------------------------------------------------------#
rm(list = ls())
'%!in%' <- function(x,y)!('%in%'(x,y))

#--------------------------------------------------------------------------------------------------#
# 1. Load Data                                                                                        ----
#--------------------------------------------------------------------------------------------------#
 setwd("C:") 	#Disc of data location
 folder_location = "C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/0.0 Womens paper/"

 #Upload w_gdppr aggregated data
 table_name = "ccu037_02_w_gdppr_aggregated_diversity_sunburn_plot.csv"
 w_gdppr <- read.csv(paste0(folder_location,table_name),fill = TRUE, header = TRUE)
 w_gdppr = rename(w_gdppr, cat6=ethnicity_5_group, cat19=PrimaryCode_ethnicity, SNOMED_id=SNOMED_conceptId_description) #rename(data_frame, new_name=old_name)
 w_gdppr$cat19[ w_gdppr$cat19 == "0"] = "C"
 w_gdppr$cat19[ w_gdppr$cat19 == "1"] = "M"
 w_gdppr$cat19[ w_gdppr$cat19 == "2"] = "N"
 w_gdppr$cat19[ w_gdppr$cat19 == "3"] = "P"
 w_gdppr$cat19[ w_gdppr$cat19 == "4"] = "H"
 w_gdppr$cat19[ w_gdppr$cat19 == "5"] = "J"
 w_gdppr$cat19[ w_gdppr$cat19 == "6"] = "K"
 w_gdppr$cat19[ w_gdppr$cat19 == "7"] = "R"
 w_gdppr$cat19[ w_gdppr$cat19 == "8"] = "S"
 w_gdppr$cat19[ w_gdppr$cat19 == "9" | w_gdppr$cat19 == "99" | w_gdppr$cat19 == "X"] = "Z"
 w_gdppr$cat6[w_gdppr$cat6 == "Other Ethnic Group" & w_gdppr$cat19=="R"] = "Asian or Asian British"
 #table(w_gdppr$cat19)
 
 w_gdppr$cat19[w_gdppr$cat19==""] = "Z"
 
 # total_w_gdppr = 31077280 + those with <10 counts (8 categories had less than 10 counts: we will include n=5 to all of them).
 # sum(w_gdppr$n_num[w_gdppr$n_cat != "<10"]) # = 31077280 
 w_gdppr$n_num[w_gdppr$n_cat == "<10"] = 5
 total_w_gdppr=sum( w_gdppr$n_num)
 
 
 #Upload w_covid aggregated data
 table_name = "ccu037_02_w_covid_aggregated_diversity_sunburn_plot.csv"
 w_covid <- read.csv(paste0(folder_location,table_name),fill = TRUE, header = TRUE)
 w_covid$cat19[ w_covid$cat19 == "0"] = "C"
 w_covid$cat19[ w_covid$cat19 == "1"] = "M"
 w_covid$cat19[ w_covid$cat19 == "2"] = "N"
 w_covid$cat19[ w_covid$cat19 == "3"] = "P"
 w_covid$cat19[ w_covid$cat19 == "4"] = "H"
 w_covid$cat19[ w_covid$cat19 == "5"] = "J"
 w_covid$cat19[ w_covid$cat19 == "6"] = "K"
 w_covid$cat19[ w_covid$cat19 == "7"] = "R"
 w_covid$cat19[ w_covid$cat19 == "8"] = "S"
 w_covid$cat19[ w_covid$cat19 == "9" | w_covid$cat19 == "99" | w_covid$cat19 == "X"] = "Z"
 w_covid$cat6[w_covid$cat6 == "Other Ethnic Group" & w_covid$cat19=="R"] = "Asian or Asian British" #not here, but happened in w_gdppr
 
 #table(w_covid$cat19)
 w_covid$cat19[w_covid$cat19==""] = "Z"
 # total_w_gdppr = 3625980 + those with <10 counts (22 categories had less than 10 counts: we will include n=5 to all of them).
 # sum(w_covid$n_num[w_covid$n_cat != "<10"]) # = 3625980 
 w_covid$n_num[w_covid$n_cat == "<10"] =  4.090909
 total_w_covid=sum( w_covid$n_num)
 
 
 
 #str(w_gdppr)
 #str(w_covid)
#--------------------------------------------------------------------------------------------------#
# 2. PREPARE COMMON LABELS: GET high level cat (i.e., six_cat), helper group variables and n(%)         ----
#--------------------------------------------------------------------------------------------------#
#Mapping 6cat to 19cat included already, and old Primary codes (19 includes old notation [ie.0,1 etc]) has been changes to new notation above.
 table(w_gdppr$cat19)
 table(w_covid$cat19)
#A-Z categories (mydata$PrimaryCode): "0","1","2","3","4","5","6","7","8","9","99","X" is not used (we have seen before, to fix the all cat plot, we modify from here).
 B_char = c('N','M','P')		     #c('1','2','3','N','M','P')
 W_char = c('A','B','C','T')		 #c('0','A','B','C','T')
 A_char = c('L','K','J','H','R') #c('4','5','6','L','K','J','H','R')
 O_char = c('W','S')	           #c('7','8','W','S')
 M_char = c('D','E','F','G')
 U_char = c('Z')		         	   #c('9','99','Z','X')

#--------------------------------------------------------------------------------------------------#
# 3. Plotly package -	sunburst plot for all ethnic groups ( https://plotly.com/r/sunburst-charts/ )     ----
#--------------------------------------------------------------------------------------------------#
 #library(plotly) #loaded above 

## 3.1 W_GDPPR:                                                                                         ----  
 #sum individuals for each 6cat
 sum_ind6cat = aggregate(x = w_gdppr$n_num,   	 # Specify data column
                       by = list(w_gdppr$cat6),  # Specify group indicator
                       FUN = sum)
 names(sum_ind6cat) <- c("cat6","n")
 #sum individuals for each 19cat
 sum_indAZ = aggregate(x = w_gdppr$n_num,   	        # Specify data column
                       by = list(w_gdppr$cat19),  # Specify group indicator
                       FUN = sum)
 names(sum_indAZ) <- c("AZ","n")
 #Mapping
 map_PC_6cat = unique(w_gdppr[c(2,3)])# %>% arrange(cat6) #cat6, cat19
 #Branches suburst plot - head
 head = data.frame(   n = c(sum(w_gdppr$n_num), sum_ind6cat$n, sum_indAZ$n    ), #total patients, patients in each 6cat, patients in each Primary Code (from A to Z)
                      IDs = c("GDPPR WOMEN", sum_ind6cat$cat6, sum_indAZ$AZ),
                      Parents = c("", rep("GDPPR WOMEN", length(sum_ind6cat$cat6)),  rep("", length(sum_indAZ$n)))
 )
 head [c(8:26),3] <- left_join(head[c(8:26),], map_PC_6cat, by=c("IDs"="cat19"))[4] #[4] to include only the joined column
 #Branches suburst plot - combine head and subbranches
 subdata <- w_gdppr[,c(4,6,3)]; names(subdata) = c("n","IDs","Parents")  #n_num, SNOMED_id, cat19
 subdata <- rbind(head, subdata)
  #when $ID == Na <- "No SNOMED"
 subdata$Labels <- str_replace_all(subdata$IDs, " ", "<br>")
 
 sum_feq_6cat <- aggregate(x = w_gdppr$freq,   	     # Specify data column
                           by = list(w_gdppr$cat6),  # Specify group indicator
                           FUN = sum)
 names(sum_feq_6cat) <- c("cat6","freq")
 
 add_percent = c(""," (9.5%)", " (3.8%)"," (1.7%)"," (1.6%)"," (7.3%)"," (74.5%)", rep("",length(c(8:length(subdata$n)))))
 add_percent = str_replace_all(add_percent," ", "<br>")
 subdata$Labels <- paste(subdata$Labels,add_percent, sep="")
 subdata_gdppr = subdata
 #Create suburst plot
 fig_gdppr =  plot_ly(subdata, ids = ~IDs, labels = ~Labels, parents = ~Parents,  type = 'sunburst',values = ~n, branchvalues = 'total')# ,  insidetextorientation='radial') #orientation text = radial
 #fig_gdppr
## 3.2 W_COVID:                                                                                         ----
 #sum individuals for each 6cat
 sum_ind6cat = aggregate(x = w_covid$n_num,   	 # Specify data column
                         by = list(w_covid$cat6),  # Specify group indicator
                         FUN = sum)
 names(sum_ind6cat) <- c("cat6","n")
 #sum individuals for each 19cat
 sum_indAZ = aggregate(x = w_covid$n_num,   	        # Specify data column
                       by = list(w_covid$cat19),  # Specify group indicator
                       FUN = sum)
 names(sum_indAZ) <- c("AZ","n")
 #Mapping
 map_PC_6cat = unique(w_covid[c(1,2)])# %>% arrange(cat6) #cat6, cat19
 #Branches suburst plot - head
 head = data.frame(   n = c(sum(w_covid$n_num), sum_ind6cat$n, sum_indAZ$n    ), #total patients-20 that are extra for the rounding, patients in each 6cat, patients in each Primary Code (from A to Z)
                      IDs = c("COVID-19 WOMEN (study cohort)", sum_ind6cat$cat6, sum_indAZ$AZ),
                      Parents = c("", rep("COVID-19 WOMEN (study cohort)", length(sum_ind6cat$cat6)),  rep("", length(sum_indAZ$n)))
 )
 head [c(8:26),3] <- left_join(head[c(8:26),], map_PC_6cat, by=c("IDs"="cat19"))[4] #[4] to include only the joined column
 #Branches suburst plot - combine head and subbranches
 subdata <- w_covid[,c(5,4,2)]; names(subdata) = c("n","IDs","Parents")  #n_num, SNOMED_id, cat19
 subdata <- rbind(head, subdata)
 #when $ID == Na <- "No SNOMED"
 subdata$Labels <- str_replace_all(subdata$IDs, " ", "<br>")
 
 sum_feq_6cat <- aggregate(x = w_covid$freq,   	     # Specify data column
                           by = list(w_covid$cat6),  # Specify group indicator
                           FUN = sum)
 names(sum_feq_6cat) <- c("cat6","freq")
 
 add_percent = c(""," (7.6%)", " (3.3%)"," (1.1%)"," (0.9%)"," (3.7%)"," (82.1%)", rep("",length(c(8:length(subdata$n)))))
 add_percent = str_replace_all(add_percent," ", "<br>")
 subdata$Labels <- paste(subdata$Labels,add_percent, sep="")
 #subdata$n[subdata$Labels=="COVID-19<br>WOMEN<br>(study<br>cohort)"] = 3626070
 subdata_covid = subdata
 subdata$n[subdata$n == 4.090909] = 0.10
 #Create suburst plot
 fig_covid =  plot_ly(subdata, ids = ~IDs, labels = ~Labels, parents = ~Parents,  type = 'sunburst',values = ~n, branchvalues = 'total')# ,  insidetextorientation='radial') #orientation text = radial
 #fig_covid 
 
##3.3 Plot both together ----
 d1=subdata_gdppr #data for plot on the left
 d2=subdata_covid #data for plot on the right
 
 d1$n[d1$n == 5] = 0.10
 d2$n[d2$n == 4.090909] = 0.10
 
 
 fig <- plot_ly() 
 fig <- fig %>%
   add_trace(
     ids = d1$IDs,
     labels = d1$Labels,
     parents = d1$Parents,
     values = d1$n,
    branchvalues = 'total',
     type = 'sunburst',
    # maxdepth = 4,
    name = "GDPPR\n women",     #name trace
     domain = list(column = 0)
   ) 
 fig <- fig %>%
   add_trace(
     ids = d2$IDs,
     labels = d2$Labels,
     parents = d2$Parents,
     values = d2$n,
     branchvalues = 'total',
     type = 'sunburst',
     name = "COVID-19\n women", #name trace
    # maxdepth = 4,
     domain = list(column = 1)
   ) 
 fig <-fig %>%
#   fig %>%
       layout(grid = list(columns =2, rows = 1),
              margin = list(l = 0, r = 0, b = 0, t = 0)#,
           #  sunburstcolorway = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BFC4","#00BA38"), extendsunburstcolors = TRUE),
           #  title = 'Diversity in women from GDPPR (left) and the study cohort (women diagnosed with COVID-19, right)'
       )
##3.4 Export ----
 #Individual
 htmlwidgets::saveWidget(as_widget(fig_gdppr), "Diversity in women from GDPPR.html") 
 htmlwidgets::saveWidget(as_widget(fig_covid), "Diversity in women from the study cohort - women diagnosed with COVID-19, right-.html") 
 #Merged
 htmlwidgets::saveWidget(as_widget(fig), "Diversity in women from GDPPR_left - and the study cohort_women diagnosed with COVID-19, right-.html") 
 
#--------------------------------------------------------------------------------------------------#
# Repeat the COVID-19 women (study cohort) sunburst but by every months of study enrolment period    ----
#--------------------------------------------------------------------------------------------------#
 
 #--------------------------------------------------------------------------------------------------#
 # S1. Load Data                                                                                        ----
 #--------------------------------------------------------------------------------------------------#
 setwd("C:") 	#Disc of data location
 folder_location = "C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/0.0 Womens paper/"
 #Upload w_covid aggregated data
 table_name = "ccu037_02_women_aggregated_diversity_sunburn_plot_permonths_numbers_woman_paper.csv"
 w_covid_months <- read.csv(paste0(folder_location,table_name),fill = TRUE, header = TRUE)
 w_covid_months$cat19[ w_covid_months$cat19 == "0"] = "C"
 w_covid_months$cat19[ w_covid_months$cat19 == "1"] = "M"
 w_covid_months$cat19[ w_covid_months$cat19 == "2"] = "N"
 w_covid_months$cat19[ w_covid_months$cat19 == "3"] = "P"
 w_covid_months$cat19[ w_covid_months$cat19 == "4"] = "H"
 w_covid_months$cat19[ w_covid_months$cat19 == "5"] = "J"
 w_covid_months$cat19[ w_covid_months$cat19 == "6"] = "K"
 w_covid_months$cat19[ w_covid_months$cat19 == "7"] = "R"
 w_covid_months$cat19[ w_covid_months$cat19 == "8"] = "S"
 w_covid_months$cat19[ w_covid_months$cat19 == "9" | w_covid_months$cat19 == "99" | w_covid_months$cat19 == "X"] = "Z"
 w_covid_months$cat6[w_covid_months$cat6 == "Other Ethnic Group" & w_covid_months$cat19=="R"] = "Asian or Asian British" #not here, but happened in w_gdppr
 #table(w_covid_months$cat19)
 w_covid_months$cat19[w_covid_months$cat19==""] = "Z"
 #Reduce text of 6cat for lables (like Other ethnic group to Other*)
 w_covid_months$cat6[w_covid_months$cat6 == "Asian or Asian British"] = "Asian*<br>"
 w_covid_months$cat6[w_covid_months$cat6 == "Black or Black British"] = "Black*"
 w_covid_months$cat6[w_covid_months$cat6 == "Other Ethnic Group"] = "Other*"
 
  
 #Include n_cat and n_num where ">10" == 4.035948 
 w_covid_months$n_cat = w_covid_months$n
 w_covid_months$n_num = ifelse(w_covid_months$n=="<10",4.035948,as.numeric(w_covid_months$n))
 # total_w_gdppr = 2921515 + those with <10 counts (306 categories had less than 10 counts: we will include n=5 to all of them).
 # sum(w_covid_months$n_num[w_covid_months$n_cat != "<10"]) # = 2921515 
 total_w_covid #n=3625970 
 total_w_covid_months = 2922750 #sum( w_covid_months$n_num)
 total_w_covid_months1 = 92460
 total_w_covid_months2 = 479205
 total_w_covid_months3 = 420890
 total_w_covid_months4 = 1361875
 total_w_covid_months5 = 568320
#discrepancy between total_w_covid and total_w_covid_months are due to the categories with <10 and the rounding... since we have more categories, we round more 

 frequency_6lvl_over_months  <- read_csv("C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/0.0 Womens paper/ccu037_02_frequency_6highlvl_over_months.csv", 
                                         col_types = cols(total_nperiod = col_number()),skip = 2)
 
 w_covid_months$time_ord = factor(w_covid_months$time, 
                              levels=c("23 Jan 2020 to\n30 Jun 2020","01 Jul 2020 to\n31 Dec 2020","01 Jan 2021 to\n30 Jun 2021","01 Jul 2021 to\n31 Dec 2021","01 Jan 2022 to\n01 Apr 2022"),
                              labels=c("23-Jan-2020 to 30-Jun-2020","01-Jul-2020 to 31-Dec-2020","01-Jan-2021 to 30-Jun-2021","01-Jul-2021 to 31-Dec-2021","01-Jan-2022 to 01-Apr-2022"))
 months =levels(w_covid_months$time_ord)
 
 #--------------------------------------------------------------------------------------------------#
 # 2. PREPARE COMMON LABELS: GET high level cat (i.e., six_cat), helper group variables and n(%)         ----
 #--------------------------------------------------------------------------------------------------#
 #Mapping 6cat to 19cat included already, and old Primary codes (19 includes old notation [ie.0,1 etc]) has been changes to new notation above.
 table(w_gdppr$cat19)
 table(w_covid$cat19)
 table(w_covid_months$cat19)
 #A-Z categories (mydata$PrimaryCode): "0","1","2","3","4","5","6","7","8","9","99","X" is not used (we have seen before, to fix the all cat plot, we modify from here).
 B_char = c('N','M','P')		     #c('1','2','3','N','M','P')
 W_char = c('A','B','C','T')		 #c('0','A','B','C','T')
 A_char = c('L','K','J','H','R') #c('4','5','6','L','K','J','H','R')
 O_char = c('W','S')	           #c('7','8','W','S')
 M_char = c('D','E','F','G')
 U_char = c('Z')		         	   #c('9','99','Z','X')
 
 #--------------------------------------------------------------------------------------------------#
 # S3. Plotly package -	sunburst plot for all ethnic groups ( https://plotly.com/r/sunburst-charts/ )     ----
 #--------------------------------------------------------------------------------------------------#
 #library(plotly) #loaded above 
 
 ## S3.1 Loop for individual months of enrollment:
 ##  Jan-Jun 2020 (W_COVID):        
 ##  Jul-Dec 2020 (W_COVID):      
 ##  Jan-Jun 2021 (W_COVID):        
 ##  Jul-Dec 2021 (W_COVID):      
 ##  Jan-Jun 2022 (W_COVID):      

 fig_covid_months = vector("list",length(months)); names(fig_covid_months) = months
 d15 = vector("list",length(months)); names(d15) = months #Keep subdata for plot toguether
 
 for (i in 1:length(months)){
   #Sudset data
   months_data = w_covid_months[w_covid_months$time_ord == months[i],]
   #sum individuals for each 6cat
   sum_ind6cat = aggregate(x = months_data$n_num,   	 # Specify data column
                           by = list(months_data$cat6),  # Specify group indicator
                           FUN = sum)
   names(sum_ind6cat) <- c("cat6","n")
   #sum individuals for each 19cat
   sum_indAZ = aggregate(x = months_data$n_num,   	        # Specify data column
                         by = list(months_data$cat19),  # Specify group indicator
                         FUN = sum)
   names(sum_indAZ) <- c("AZ","n")
   #Mapping
   map_PC_6cat = unique(months_data[c(2,3)])# %>% arrange(cat6) #cat6, cat19
   #Branches suburst plot - head
   first_ID = paste0("Enrollment: ",months[i])
   head = data.frame(   n = c(sum(months_data$n_num), sum_ind6cat$n, sum_indAZ$n    ), #total patients-20 that are extra for the rounding, patients in each 6cat, patients in each Primary Code (from A to Z)
                        IDs = c(first_ID, sum_ind6cat$cat6, sum_indAZ$AZ),
                        Parents = c("", rep(first_ID, length(sum_ind6cat$cat6)),  rep("", length(sum_indAZ$n)))
   )
   head [c(8:26),3] <- left_join(head[c(8:26),], map_PC_6cat, by=c("IDs"="cat19"))[4] #[4] to include only the joined column
   #Branches suburst plot - combine head and subbranches
   subdata <- months_data[,c(9,5,3)]; names(subdata) = c("n","IDs","Parents")  #n_num, SNOMED_id, cat19
   subdata <- rbind(head, subdata)
   #when $ID == Na <- "No SNOMED"
   subdata$Labels <- str_replace_all(subdata$IDs, " ", "<br>")
   
   months_data$freq = round(months_data$n_num/frequency_6lvl_over_months$total_nperiod[i],3)*100
   sum_feq_6cat <- aggregate(x = months_data$freq,   	     # Specify data column
                             by = list(months_data$cat6),  # Specify group indicator
                             FUN = sum)
   names(sum_feq_6cat) <- c("cat6","freq")
   
   #add_percent = c(""," (7.6%)", " (3.3%)"," (1.1%)"," (0.9%)"," (3.7%)"," (82.1%)", rep("",length(c(8:length(subdata$n)))))
   add_percent = c("", paste0(" (",sum_feq_6cat$freq,"%)"),rep("",length(c(8:length(subdata$n)))))
   # add_percent = str_replace_all(add_percent," ", "<br>")
   subdata$Labels <- paste(subdata$Labels,add_percent, sep="")
   #> subdata$n[subdata$Labels== (str_replace_all(first_ID, " ", "<br>"))]
   #Create suburst plot
   subdata$n[subdata$n == 4.035948] = 0.10
   fig_covid_months[[i]] =  plot_ly(subdata, ids = ~IDs, labels = ~Labels, parents = ~Parents,  type = 'sunburst',values = ~n, branchvalues = 'total')# ,  insidetextorientation='radial') #orientation text = radial
   #Save data to produce individual plots in the same html
   #subdata$n[subdata$n!= 0.1] =  round(subdata$n[subdata$n!= 0.1],0) # gives error later in some of the plots
   d15[[i]] = subdata
 }
 #fig_covid_months[[1]]
 #head(d15[[1]])
 
 ##S3.2 Plot both together ----
 rm(fig2); domain_columns = c(0:2,0:1); domain_rows = c(0,0,0,1,1)
 fig2 <- plot_ly() 
 for (i in 1:length(months)){
   fig2 <- fig2 %>%
     add_trace(
       ids = d15[[i]]$IDs,
       labels = d15[[i]]$Labels,
       parents = d15[[i]]$Parents,
       values = d15[[i]]$n,
       branchvalues = 'total',
       type = 'sunburst',
       # maxdepth = 4,
       name = months[i],     #name trace
       domain = list(column=domain_columns[i], row=domain_rows[i])
     ) 
 }
 #fig2
 
 fig3 <-fig2 %>%
 #fig2 %>%
   layout(grid = list(columns =3, rows = 2),
          margin = list(l = 0, r = 0, b = 0, t = 0)#,
          #  sunburstcolorway = c("#F8766D","#F564E3","#B79F00","#619CFF","#00BFC4","#00BA38"), extendsunburstcolors = TRUE),
          #  title = 'Diversity in women from GDPPR (left) and the study cohort (women diagnosed with COVID-19, right)'
   )
 
 ##S3.4 Export ----
 #Individual
 for(i in 1:length(months)){
 name_plot = paste0("Diversity in women from the study cohort (enrolment ",i,"): ",months[i]) 
 htmlwidgets::saveWidget(as_widget(fig_covid_months[[i]]),name_plot)
 }
 
 #Merged
 htmlwidgets::saveWidget(as_widget(fig3), "Diversity in women study cohort_women by every 6 months of enrollment, right-.html") 
 