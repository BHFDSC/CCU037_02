#Necessary to run ______________________________________________________________ ---- 
rm(list=ls())

#Include mapping
temp = c("Asian or Asian British","Bangladeshi","Indian","Pakistani","Chinese","Any other Asian background","Black or Black British","Black African","African","Black Caribbean","Caribbean","Any other Black background",
         "Mixed","White and Asian","White and Black African","White and Black Caribbean","Any other Mixed background",
         "Other Ethnic Group","Other","Arab","Any other ethnic group","Unknown","Unknown/Not stated","White", "White British","White other","British","Irish","Gypsy or Irish Traveller","Any other White background")

mapping = data.frame(map = c(rep("Asian",6), rep("Black",6), rep("Mixed",5), rep("Other",4), rep("Unknown",2), rep("White",7)), code = temp)

time_order = c("2020","2021","23 Jan 2020 to 31 Aug 2020","01 Sep 2020 to 31 Dec 2020","01 Jan 2021 to 31 Aug 2021","01 Sep 2021 to 31 Dec 2021")

#Load packages
library(ggplot2)
library(dplyr)
library(forcats) # fct_reorder()


#Ratio age-standardized IR plots eg., code _____________________________________ ---- 
#Load data 
data <- read.csv("C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/Results downoaded/2.1 IR female male ratios 6-10-19 cat.csv")
#Transform variables into factors (order them)
data$classification <- factor(data$classification, levels = c("6cat","9cat","19cat"), labels = c("High-level ethnic groups","Ten-level ethnic groups","NHS ethnicity codes") )
#Add mapping:
data = left_join(data, mapping)
#data$order_values = fct_reorder(data$code, data$IR_Ratio_FM, .desc = TRUE)

#death28d --> data[data$outcome=="death28d",]
data[data$outcome=="death28d",] %>% 
  mutate(order_values = fct_reorder(code, IR_Ratio_FM) ) %>%
  ggplot (aes(x=order_values, y=IR_Ratio_FM, colour = map)) +
    geom_point()+ scale_color_manual(values=c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00","#F8766D"))+
    facet_wrap(vars(classification),scales = "free_x")+
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))+
    ggtitle("Female/Male Ratio of Age-standardized incidence rates of 28-days mortality") +
    labs( colour = "Mapping to\nHigh-level\nethnic groups") + xlab("Ethnic groups")+ ylab("Female/Male Ratio") 
   # theme(plot.title = element_text(size=42)) +


# NOTES ________________________________________________________________________ ---- 
#We lost 1,106,946 excluding 2022 individuals.

#diferences ethnicities has different diferences in F/M ratio
#For 18, do the umbrella stucture 6-19

# Code for all outcomes using directly download csv ____________________________ ---- 
#Load data
IR_all <- read.csv("C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/Results downoaded/0. 2020-2021 subset/2020_2021_tables_IR_age_std_correct_CI/2020_2021_age_std_IR_correct_CI_censored.csv")
IR_years <- read.csv("C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/Results downoaded/0. 2020-2021 subset/2020_2021_tables_IR_age_std_correct_CI/2020_2021_age_std_IR_correct_CI_byyears_IR1_censored.csv")
IR_waives_6_9 <- read.csv("C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/Results downoaded/0. 2020-2021 subset/2020_2021_tables_IR_age_std_correct_CI/2020_2021_age_std_IR_correct_CI_every6months_IR2_censored_Part1_6&9cat.csv")
IR_waives_19 <- read.csv("C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/Results downoaded/0. 2020-2021 subset/2020_2021_tables_IR_age_std_correct_CI/2020_2021_age_std_IR_correct_CI_every6months_IR2_censored_Part2_19cat.csv")
#Merge IR_waives
IR_waives <- rbind(IR_waives_6_9,IR_waives_19)
#Create a list
IR_list = list(IR_all,IR_years,IR_waives)
#Separate female IR and calculate the Female/Male ratio of Age-std IR
for (i in 1:length(IR_list)){
  IR_list[[i]] <-data.frame(IR_list[[i]][IR_list[[i]]$SEX == 1,], IR_female = IR_list[[i]][IR_list[[i]]$SEX == 2,]$IR_std) #manual eg below 1.1
  IR_list[[i]]$IR_Ratio_FM <- IR_list[[i]]$IR_female/IR_list[[i]]$IR_std
  #For plots below:
    #Transform variables into factors (order them)
    IR_list[[i]]$classification <- factor(IR_list[[i]]$classification, levels = c("6cat","9cat","19cat"), labels = c("High-level ethnic groups","Ten-level ethnic groups","NHS ethnicity codes") )
    #Add mapping:
    IR_list[[i]] = left_join( IR_list[[i]], mapping)
  }

#Plots 
outcomes = names(table(IR_all$outcome))
ggtitle_outcomes = c("28-days mortality","90-days mortality","1-year CVE","30-days CVE")
#for IR_all:
plots_IR_all = vector("list", length(outcomes)); names(plots_IR_all) <- outcomes
j=1
for (i in 1:length(outcomes)){
 print(outcomes[i])
 title_text = paste("Female/Male Ratio of Age-standardized incidence rates of",ggtitle_outcomes[i], sep=" ")
 data = IR_list[[j]]
 plots_IR_all[[i]] = data[data$outcome==outcomes[i],] %>% 
                       mutate(order_values = fct_reorder(code, IR_Ratio_FM) ) %>%
                       ggplot (aes(x=order_values, y=IR_Ratio_FM, colour = map)) +
                       geom_point()+ scale_color_manual(values=c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00","#F8766D"))+
                       facet_wrap(vars(classification),scales = "free_x")+
                       theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))+
                       ggtitle(title_text) +
                       labs( colour = "Mapping to\nHigh-level\nethnic groups") + xlab("Ethnic groups")+ ylab("Female/Male Ratio") 
 }



#for IR_years and IR_waives
plots_IR_time = vector("list", length(outcomes)*2); names(plots_IR_time) <- paste(outcomes,rep(c("years","waives"), each = 4))
time_period=c("years","waives")

z=1
for (j in 2:length(IR_list)){
  print(j)
  print(time_period[j-1])
  for (i in 1:length(outcomes)){  
   title_text = paste("Female/Male Ratio of Age-standardized incidence rates of ",ggtitle_outcomes[i]," (",time_period[j-1],")", sep="")
   data = IR_list[[j]]
   data$period = fct_relevel(factor(data$time), time_order[time_order %in% data$time]) 
   
   plots_IR_time[[z]] = data[data$outcome==outcomes[i],] %>% 
     mutate(order_values = fct_reorder(code, IR_Ratio_FM)) %>%
     ggplot (aes(x=order_values, y=IR_Ratio_FM, colour = map)) +
     geom_point(aes(shape = period))+ #add different shapes for time period
     scale_color_manual(values=c("#F564E3","#619CFF","#00BFC4","#00BA38","#B79F00","#F8766D"))+
     facet_wrap(vars(classification),scales = "free_x")+
   #  facet_wrap(classification~time,scales = "free_x")+
     theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))+
     ggtitle(title_text) +
     labs( colour = "Mapping to\nHigh-level\nethnic groups") + xlab("Ethnic groups")+ ylab("Female/Male Ratio") 
   z = z+1
}}

#1.1 Separate female IR manually
#IR_all_ratio <-data.frame(IR_all[IR_all$SEX == 1,], IR_female = IR_all[IR_all$SEX == 2,]$IR_std)
#IR_years_ratio <-data.frame(IR_years[IR_years$SEX == 1,], IR_female = IR_years[IR_years$SEX == 2,]$IR_std)
#IR_waives_6_9_ratio <-data.frame(IR_waives_6_9[IR_waives_6_9$SEX == 1,], IR_female = IR_waives_6_9[IR_waives_6_9$SEX == 2,]$IR_std)
#IR_waives_19_ratio <-data.frame(IR_waives_19[IR_waives_19$SEX == 1,], IR_female = IR_waives_19[IR_waives_19$SEX == 2,]$IR_std)

#Save plots_IR_all and plots_IR_time
path = 
ggsave(path = path, width = width, height = height, device='tiff', dpi=700)




