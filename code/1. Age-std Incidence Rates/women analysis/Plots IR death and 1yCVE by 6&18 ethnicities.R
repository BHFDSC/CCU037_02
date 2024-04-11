#--------------------------------------------------------------------------------------------------#
# Load Data
#--------------------------------------------------------------------------------------------------#
 rm(list = ls())
 setwd("C:") 	#Disc of data location
 mydata <- read.csv('C:/Users/martapm/Desktop/Marta/CVD-COVID-UK [HDRUK]/ccu037_02/Results downoaded/IR_death&1yCVE_all_individuals_by_ethnicity.csv',fill = TRUE, header = TRUE)
 head(mydata)
#--------------------------------------------------------------------------------------------------#
library(ggplot2)
library(tidyverse)

##PLOT INCIDENCE RATE:

#''COVID DEATH (death within the 30 days after covid diagnosis)
data = mydata[mydata$classification == "6 groups" & mydata$outcome == "covid_death",]
data$eth = factor(data$code, levels = c('Asian or Asian British','Black or Black British','Mixed','Other Ethnic Group','White','Unknown'))
eth_order = c('Unknown','White','Other Ethnic Group','Mixed','Black or Black British','Asian or Asian British')

p6 <- data %>% 
	mutate(code = fct_relevel(code, eth_order)) %>%
	ggplot(aes( x=code, y=est, fill=code))+
		coord_flip()+ guides(fill = guide_legend(reverse = TRUE)) + 								
		geom_bar(stat="identity", alpha=.6, width=.4)+ 						#"#f68060" = nice salmon colour
		geom_errorbar(aes(ymin = lower, ymax = upper), width=.2, alpha=0.5) +		#width=.2,colour="black",position=position_dodge(width = .9))
		theme_bw() + labs(title="Incidence Rate of Covid Death", y="IR (cases per 1000 person-year)", x = "Ethnicity groups")+
		theme(legend.position = "none")

#'18 groups
data = mydata[mydata$classification == "18 groups" & mydata$outcome == "covid_death",]
data$cat6[data$code == 'Z' ] = 'Unknown'
data$cat6[data$code == 'W' | data$code == 'S' ] = 'Other Ethnic Groups'
data$cat6[data$code == 'N' | data$code == 'M' |data$code == 'P' ] = 'Black or Black British'
data$cat6[data$code == 'A' | data$code == 'B' |data$code == 'C' | data$code == 'T'] = 'White'
data$cat6[data$code == 'D' | data$code == 'E' |data$code == 'F' |data$code == 'G'] = 'Mixed'
data$cat6[data$code == 'L' | data$code == 'K' |data$code == 'J' | data$code == 'H' | data$code == 'R'] = 'Asian or Asian British'

data$colour[data$cat6 == "Unknown"] = "#00BA38"
data$colour[data$cat6 == "Other Ethnic Groups"] = "#619CFF" 
data$colour[data$cat6 == "Black or Black British"] = "#F564E3"
data$colour[data$cat6 == "White"] = "#00BFC4"
data$colour[data$cat6 == "Mixed"] = "#B79F00"
data$colour[data$cat6 == "Asian or Asian British"] = "#F8766D"



data$eth = factor(data$code, levels = c('A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','Z'),
				    labels = c('British','Irish','Any other White background',
						   'White and Black Caribbean','White and Black African','White and Asian','Any other Mixed background',
						   'Indian','Pakistani','Bangladeshi','Any other Asian background',
						   'Caribbean','African','Any other Black background','Chinese','Any other ethnic group','Gypsy or Irish Traveller','Arab','Unknown/Not stated'))
data$eth = as.character(data$eth)

eth_order = c('Indian','Pakistani','Bangladeshi','Chinese','Any other Asian background',					  #Asian or Asian Brithish
		  'African','Caribbean','Any other Black background',									  #Black or Black British
		  'White and Black Caribbean','White and Black African','White and Asian','Any other Mixed background', #Mixed
		  'Any other ethnic group','Arab',												  #Other Ethnic Groups
		  'British','Irish','Any other White background','Gypsy or Irish Traveller',					  #White
		  'Unknown/Not stated')


p18 <- data %>% 
	mutate(eth = fct_relevel(eth, rev(eth_order))) %>%
	ggplot(aes( x=eth, y=est, fill=colour))+
		coord_flip()+ guides(fill = guide_legend(reverse = TRUE)) + 								
		geom_bar(stat="identity", alpha=.6, width=.4)+ 						#"#f68060" = nice salmon colour
		geom_errorbar(aes(ymin = lower, ymax = upper), width=.2, alpha=0.5) +		#width=.2,colour="black",position=position_dodge(width = .9))
		theme_bw() + labs(title="Incidence Rate of Covid Death", y="IR (cases per 1000 person-year)", x = "Ethnicity groups") +
		theme(legend.position = "none")


#''1y CVE =======================================================================================================================================
data = mydata[mydata$classification == "6 groups" & mydata$outcome == "post_covid_cvd_event_1y",]
data$eth = factor(data$code, levels = c('Asian or Asian British','Black or Black British','Mixed','Other Ethnic Group','White','Unknown'))
eth_order = c('Unknown','White','Other Ethnic Group','Mixed','Black or Black British','Asian or Asian British')

p6 <- data %>% 
	mutate(code = fct_relevel(code, eth_order)) %>%
	ggplot(aes( x=code, y=est, fill=code))+
		coord_flip()+ guides(fill = guide_legend(reverse = TRUE)) + 								
		geom_bar(stat="identity", alpha=.6, width=.4)+ 						#"#f68060" = nice salmon colour
		geom_errorbar(aes(ymin = lower, ymax = upper), width=.2, alpha=0.5) +		#width=.2,colour="black",position=position_dodge(width = .9))
		theme_bw() + labs(title="Incidence Rate of 1-year CVE", y="IR (cases per 1000 person-year)", x = "Ethnicity groups")+
		theme(legend.position = "none")

#'18 groups
data = mydata[mydata$classification == "18 groups" & mydata$outcome == "post_covid_cvd_event_1y",]
data$cat6[data$code == 'Z' ] = 'Unknown'
data$cat6[data$code == 'W' | data$code == 'S' ] = 'Other Ethnic Groups'
data$cat6[data$code == 'N' | data$code == 'M' |data$code == 'P' ] = 'Black or Black British'
data$cat6[data$code == 'A' | data$code == 'B' |data$code == 'C' | data$code == 'T'] = 'White'
data$cat6[data$code == 'D' | data$code == 'E' |data$code == 'F' |data$code == 'G'] = 'Mixed'
data$cat6[data$code == 'L' | data$code == 'K' |data$code == 'J' | data$code == 'H' | data$code == 'R'] = 'Asian or Asian British'

data$colour[data$cat6 == "Unknown"] = "#00BA38"
data$colour[data$cat6 == "Other Ethnic Groups"] = "#619CFF" 
data$colour[data$cat6 == "Black or Black British"] = "#F564E3"
data$colour[data$cat6 == "White"] = "#00BFC4"
data$colour[data$cat6 == "Mixed"] = "#B79F00"
data$colour[data$cat6 == "Asian or Asian British"] = "#F8766D"



data$eth = factor(data$code, levels = c('A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','T','W','Z'),
				    labels = c('British','Irish','Any other White background',
						   'White and Black Caribbean','White and Black African','White and Asian','Any other Mixed background',
						   'Indian','Pakistani','Bangladeshi','Any other Asian background',
						   'Caribbean','African','Any other Black background','Chinese','Any other ethnic group','Gypsy or Irish Traveller','Arab','Unknown/Not stated'))
data$eth = as.character(data$eth)

eth_order = c('Indian','Pakistani','Bangladeshi','Chinese','Any other Asian background',					  #Asian or Asian Brithish
		  'African','Caribbean','Any other Black background',									  #Black or Black British
		  'White and Black Caribbean','White and Black African','White and Asian','Any other Mixed background', #Mixed
		  'Any other ethnic group','Arab',												  #Other Ethnic Groups
		  'British','Irish','Any other White background','Gypsy or Irish Traveller',					  #White
		  'Unknown/Not stated')


p18 <- data %>% 
	mutate(eth = fct_relevel(eth, rev(eth_order))) %>%
	ggplot(aes( x=eth, y=est, fill=colour))+
		coord_flip()+ guides(fill = guide_legend(reverse = TRUE)) + 								
		geom_bar(stat="identity", alpha=.6, width=.4)+ 						#"#f68060" = nice salmon colour
		geom_errorbar(aes(ymin = lower, ymax = upper), width=.2, alpha=0.5) +		#width=.2,colour="black",position=position_dodge(width = .9))
		theme_bw() + labs(title="Incidence Rate of 1-year CVE", y="IR (cases per 1000 person-year)", x = "Ethnicity groups") +
		theme(legend.position = "none")

