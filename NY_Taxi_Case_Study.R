############################################################################################
# NYC Parking Ticket case study  for exploratory data analysis to understand 	             #
# various statistics such as seasonality, most common cars for which tickets are issued,   #
# most common months in a year when tickets are issued across 2015 - 2017 data    		     #                                   												       
# 																						                                             #
# Graphs and charts are prepared for different 	analysis.								                   #
# Please open this file in RStudio for better code readability                             #
#                                                                                          #
# Author: Rakesh Pattanaik.                                                                #   
############################################################################################


#LOADING DATA INTO AWS(S3)

#Create account at kaggle to log-in 

#Go to the link where data is located.https://www.kaggle.com/new-york-city/nyc-parking-tickets/data

#Download chrome extension from https://chrome.google.com/webstore/detail/cookietxt-export/lopabhfecdfhgogdbojmaicoicjekelh

#From the command shell type the following command to transfer cookies file to the master node
#scp -i /C:/Users/Documents/Personel/UpGrad/Course 5_Big Data Analytics/Group Project/cookies.txt ec2-user@ec2-52-43-117-50.us-west-2.compute.amazonaws.com:~

#Login to the master node and type the following command
#wget -x -c --load-cookies cookies.txt -P data -nH --cut-dirs=5 https://www.kaggle.com/new-york-city/nyc-parking-tickets/downloads/nyc-parking-tickets.zip

#Use the followng command to unzip the above file
#sudo gzip -d nyc-parking-tickets.zip/2

#From the above unzipped files transfer files for year 2015-2017 to S3 bucket using following commands
#aws s3 cp Parking_Violations_Issued_-_Fiscal_Year_2015.csv s3://nyc-ticket
#aws s3 cp Parking_Violations_Issued_-_Fiscal_Year_2016.csv s3://nyc-ticket
#aws s3 cp Parking_Violations_Issued_-_Fiscal_Year_2017.csv s3://nyc-ticket

# The required files are now in the S3 bucket nyc-ticket



#SETTING UP SPARK

# load SparkR
install.packages("SparkR")
library(SparkR)

install.packages("ggplot2")
library(ggplot2)

install.packages("cowplot")
library(cowplot)


# initiating the spark session
#sparkR.session(master='local')
sparkR.session(spark.master = "yarn-client", spark.executor.memory = "1g")


#Read the data files 

nyc_parking_2015 <- read.df("s3://data-science-51/car_park/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", source = "csv", inferSchema = "true", header = "true")

nyc_parking_2016 <- read.df("s3://data-science-51/car_park/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", source = "csv", inferSchema = "true", header = "true")

nyc_parking_2017 <- read.df("s3://data-science-51/car_park/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", source = "csv", inferSchema = "true", header = "true")


#DATA CLEANING & PREPARATION

#checking the column names in the above 3 dataframes

names(nyc_parking_2015)

names(nyc_parking_2016)

names(nyc_parking_2017)

#Its observed that 2015 & 2016 have 51 columns, whereas 2017 has 43 columns. Following 8 columns are missing in 2017
#Latitude, Longitude, Community Board, Community Council, Census Tract, BIN, BBL, NTA

#To analyze the data further, we will merge the 3 files into one dataframe and to make the column names 
#consistent, we will delete the 8 extra columns from 2015 & 2016. These columns are not required for analysis

nyc_parking_2015 <- nyc_parking_2015[,-c(44:51)]
nyc_parking_2016 <- nyc_parking_2016[,-c(44:51)]

#We will now add a column at the end of each of the dataframe to inidicate the year for the observation

nyc_parking_2015$Year <- 2015

nyc_parking_2016$Year <- 2016

nyc_parking_2017$Year <- 2017

#Now we will rowbind the above 3 dataframes to create one master dataframe

nyc_parking_data <- rbind(nyc_parking_2015, nyc_parking_2016, nyc_parking_2017)

nrow(nyc_parking_data)

#Total row count of master dataframe is 33239160



#EDA

#Null function, regular expression and string functions were not working in SparkR and hence we used SQL commands for the same.

# Creating a temporary view for the year wise and consolidated dataframes for using SQL commands

createOrReplaceTempView(nyc_parking_2015, "nyc_parking_2015_tbl")

createOrReplaceTempView(nyc_parking_2016, "nyc_parking_2016_tbl")

createOrReplaceTempView(nyc_parking_2017, "nyc_parking_2017_tbl")

createOrReplaceTempView(nyc_parking_data, "nyc_parking_data_tbl")


#i. Checking if the parking ticket Issue Date corresponds to years 2015, 2016 & 2017 only

issue_date_year <- SparkR::sql("select distinct substring(`Issue Date`,7,4) as issue_date_year from nyc_parking_data_tbl order by issue_date_year desc")
head(issue_date_year,100)

#The output of the above query shows that there are 74 unique years in which the parking ticket was issued. The years range from 1970-2069
#Thus, we need to filter out the data for years 2015, 2016 & 2017 for further analysis

nyc_2015 <- SparkR::sql("select * FROM nyc_parking_2015_tbl where substring(`Issue Date`,7,4) ='2015'")

nyc_2016 <- SparkR::sql("select * FROM nyc_parking_2016_tbl where substring(`Issue Date`,7,4) ='2016'")

nyc_2017 <- SparkR::sql("select * FROM nyc_parking_2017_tbl where substring(`Issue Date`,7,4) ='2017'")

#Now we will rowbind the above 3 dataframes to create a new master dataframe

nyc_data <- rbind(nyc_2015, nyc_2016, nyc_2017)

nrow(nyc_data)

#Total row count of new master dataframe is 16291370


# Creating a temporary view for the year wise and consolidated dataframes for using SQL commands

createOrReplaceTempView(nyc_2015, "nyc_2015_tbl")

createOrReplaceTempView(nyc_2016, "nyc_2016_tbl")

createOrReplaceTempView(nyc_2017, "nyc_2017_tbl")

createOrReplaceTempView(nyc_data, "nyc_data_tbl")


#ii. Checking if there are Null entries present in key columns that will be used for analysis

null_sum_num <- SparkR::sql("select Year, count(*) from nyc_data_tbl where `Summons Number` is null group by Year")
head(null_sum_num)
#There are no null values in column Summons Number


null_reg_sta <- SparkR::sql("select Year, count(*) from nyc_data_tbl where `Registration State` is null group by Year")
head(null_reg_sta)
#There are no null values in column Registration State


null_iss_dat <- SparkR::sql("select Year, count(*) from nyc_data_tbl where `Issue Date` is null group by Year")
head(null_iss_dat)
#There are no null values in column Issue Date


null_vio_cod <- SparkR::sql("select Year, count(*) from nyc_data_tbl where `Violation Code` is null group by Year")
head(null_vio_cod)
#There are no null values in column Violation Code


null_vio_tim <- SparkR::sql("select Year, count(*) from nyc_data_tbl where `Violation Time` is null group by Year")
head(null_vio_tim)
#There are total 6058 null values present in column Violation Time as shown by the output of the above query
#   Year    count
#1  2017       16
#2  2015      720
#3  2016       74
#Further the null values are only 0.005% (810/16291370) and hence we will exclude them from the analysis


#iii. In US all state codes are formed by two alphabtes, lets analyze if we have any inconsisteny in the data

unique_states <- SparkR::sql("select distinct `Registration State` from nyc_data_tbl where (`Registration State` regexp '^[^A-Z]{2}')")

head(unique_states)

#The output of above query shows that there is a state code 99 present in the dataset, which seems to be an erroneous entry



# EDA Plots Start----

#For 2015 - We will create a subset of the data with key columns that we will be plotting

nyc_2015_filter <- SparkR::sql("SELECT `Violation Code` as ViolationCode, 
                               `Violation Precinct` as ViolationPrecinct, 
                               `Issuer Precinct` as IssuerPrecinct,
                               `Summons Number` as SummonsNumber 
                               FROM nyc_2015_tbl")

cache(nyc_2015_filter)

#histogram 1 - Plotting the Violation code frequency
hist <- histogram(nyc_2015_filter, nyc_2015_filter$ViolationCode, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#4646C9") +
  labs(title = "ViolationCode_Histogram_2015", x = "ViolationCode", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot


#histogram 2 - Plotting the Violation Precinct frequency
hist <- histogram(nyc_2015_filter, nyc_2015_filter$ViolationPrecinct, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#00994C") +
  labs(title = "ViolationPrecinct_Histogram_2015", x = "ViolationPrecinct", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot


#histogram 3 - Plotting the Violation Precinct frequency
hist <- histogram(nyc_2015_filter, nyc_2015_filter$IssuerPrecinct, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#4646C9") +
  labs(title = "IssuerPrecinct_Histogram_2015", x = "IssuerPrecinct", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot

unpersist(nyc_2015_filter)


#For 2016 - We will create a subset of the data with key columns that we will be plotting

nyc_2016_filter <- SparkR::sql("SELECT `Violation Code` as ViolationCode, 
                               `Violation Precinct` as ViolationPrecinct, 
                               `Issuer Precinct` as IssuerPrecinct,
                               `Summons Number` as SummonsNumber 
                               FROM nyc_2016_tbl")

cache(nyc_2016_filter)

#histogram 1 - Plotting the Violation code frequency
hist <- histogram(nyc_2016_filter, nyc_2016_filter$ViolationCode, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#4646C9") +
  labs(title = "ViolationCode_Histogram_2016", x = "ViolationCode", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot

#histogram 2 - Plotting the Violation Precinct frequency
hist <- histogram(nyc_2016_filter, nyc_2016_filter$ViolationPrecinct, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#00994C") +
  labs(title = "ViolationPrecinct_Histogram_2016", x = "ViolationPrecinct", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot


#histogram 3 - Plotting the Issuer Precinct frequency
hist <- histogram(nyc_2016_filter, nyc_2016_filter$IssuerPrecinct, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#4646C9") +
  labs(title = "IssuerPrecinct_Histogram_2016", x = "IssuerPrecinct", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot

unpersist(nyc_2016_filter)


#For 2017 - We will create a subset of the data with key columns that we will be plotting

nyc_2017_filter <- SparkR::sql("SELECT `Violation Code` as ViolationCode, 
                               `Violation Precinct` as ViolationPrecinct, 
                               `Issuer Precinct` as IssuerPrecinct,
                               `Summons Number` as SummonsNumber 
                               FROM nyc_2017_tbl")

cache(nyc_2017_filter)


#histogram 1 - Plotting the Violation code frequency
hist <- histogram(nyc_2017_filter, nyc_2017_filter$ViolationCode, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#4646C9") +
  labs(title = "ViolationCode_Histogram_2017", x = "ViolationCode", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot


#histogram 2 - Plotting the Violation Precinct frequency
hist <- histogram(nyc_2017_filter, nyc_2017_filter$ViolationPrecinct, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#00994C") +
  labs(title = "ViolationPrecinct_Histogram_2017", x = "ViolationPrecinct", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot


#histogram 3 - Plotting the Issuer Precinct frequency
hist <- histogram(nyc_2017_filter, nyc_2017_filter$IssuerPrecinct, nbins = 12)

plot <- ggplot(hist, aes(x = centroids, y = counts)) +
  geom_bar(stat = "identity",fill = "#4646C9") +
  labs(title = "IssuerPrecinct_Histogram_2017", x = "IssuerPrecinct", y = "Frequency")+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot

unpersist(nyc_2017_filter)

#EDA Plots End-----



##EXAMINE THE DATA

#Q1. Find total number of tickets for each year.

nyc_data_tkt_cnt <- summarize(groupBy(nyc_data, nyc_data$Year), count=n(nyc_data$`Summons Number`))

nyc_data_tkt_cnt_local <- head(nyc_data_tkt_cnt)
nyc_data_tkt_cnt_local

#Answer : Following is the output of the above query, we see in year 2015 maximum number of tickets were issued.
#	Year    count
#1 	2017 	5431918
#2 	2015 	5986831
#3 	2016 	4872621

plot <- ggplot(nyc_data_tkt_cnt_local, aes(x = Year, y = count)) +
  geom_bar(stat = "identity",fill = "#00994C") +
  labs(title = "Total Tickets year wise", x = "Year", y = "counts")+
  geom_text(aes(label=count, y=count), position = position_stack(vjust = 0.5), size = 4, angle=90)+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot




#Q2. Find out how many unique states the cars which got parking tickets came from.

#For 2015
unique_states_2015 <- summarize(groupBy(filter(nyc_2015, nyc_2015$`Registration State` != '99'), nyc_2015$`Registration State`), count=n(nyc_2015$`Registration State`))
nrow(unique_states_2015)
#The above output shows that in 2015 the cars which got parking tickets came from 67 unique states. Please note we have exculded the state code 99 in the above query.
#Additionally in US there are only 50 states, so lets examine this further by executing below query.

head(unique_states_2015, 67)
#By examining the output of the above query, we see that there are Mexico(MX), 
#Canada(NS, NT, BC, MB, ON, SK, AB, PE, YT, NB) and a few other country state codes present


#For 2016
unique_states_2016 <- summarize(groupBy(filter(nyc_2016, nyc_2016$`Registration State` != '99'), nyc_2016$`Registration State`), count=n(nyc_2016$`Registration State`))
nrow(unique_states_2016)
#The above output shows that in 2016 the cars which got parking tickets came from 66 unique states. Please note we have exculded the state code 99 in the above query.
#Additionally in US there are only 50 states, so lets examine this further by executing below query.

head(unique_states_2016, 66)
#By examining the output of the above query, we see that there are Mexico(MX), 
#Canada(NS, NT, BC, MB, ON, SK, AB, PE, YT, NB) and a few other country state codes present


#For 2017
unique_states_2017 <- summarize(groupBy(filter(nyc_2017, nyc_2017$`Registration State` != '99'), nyc_2017$`Registration State`), count=n(nyc_2017$`Registration State`))
nrow(unique_states_2017)
#The above output shows that in 2017 the cars which got parking tickets came from 64 unique states. Please note we have exculded the state code 99 in the above query.
#Additionally in US there are only 50 states, so lets examine this further by executing below query.

head(unique_states_2017, 64)
#By examining the output of the above query, we see that there are Mexico(MX), 
#Canada(NS, NT, BC, MB, ON, SK, AB, PE, YT, NB) and a few other country state codes present

#Answer : From the above queries, we observe following unique state codes for different year (state code 99 excluded)
# 2015 - 67
# 2016 - 66
# 2017 - 64
# We also observed that some other country codes (Mexico, Canada etc.) were also present, these countries share border with US and these cars came into US and got a parking ticket.



#Q3. Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are.

#In the dataset we are considering combination of House Number & Street Name as the address field. If any of these columns are missing then the address is incomplete.

tkt_no_addr <- SparkR::sql("select Year, count(*) as count from nyc_data_tbl where `House Number` is null or `Street Name` is null group by Year")

tkt_no_addr_local <- head(tkt_no_addr)
tkt_no_addr_local

#Answer : From the above queries, we observe that 2017 has the maximum number of tickets without address
#	Year	count
#1 	2017  	1029420
#2 	2015   	888283
#3 	2016   	895753
#Total number of tickets with missing address are 2813456

plot <- ggplot(tkt_no_addr_local, aes(x = Year, y = count)) +
  geom_bar(stat = "identity",fill = "#00994C") +
  labs(title = "Total tickets with missing address", x = "Year", y = "Counts")+
  geom_text(aes(label=count, y=count), position = position_stack(vjust = 0.5), size = 4, angle=90)+
  theme_bw()+
  theme(axis.text.x = element_text(angle = 90))

plot




##AGGREGATION TASKS

#Q1. How often does each violation code occur? (frequency of violation codes - find the top 5)

#Answer - We will run year wise queries as below to get the frequency of violation code

#For 2015
vio_cnt_2015 <- summarize(groupBy(nyc_2015, nyc_2015$Year, nyc_2015$`Violation Code`), count=n(nyc_2015$`Violation Code`))
vio_cnt_2015_loc <- head(arrange(vio_cnt_2015, desc(vio_cnt_2015$count)),5)
vio_cnt_2015_loc
#Following is the output of the above query, we see that the violation codes 21, 38, 14, 36 & 37 occur maximum number of times in 2015.
#	Year 	Violation Code   	count
#1 	2015             21 		809914
#2 	2015             38 		746562
#3 	2015             14 		517733
#4 	2015             36 		457313
#5 	2015             37 		416133


#For 2016
vio_cnt_2016 <- summarize(groupBy(nyc_2016, nyc_2016$Year, nyc_2016$`Violation Code`), count=n(nyc_2016$`Violation Code`))
vio_cnt_2016_loc <-head(arrange(vio_cnt_2016, desc(vio_cnt_2016$count)),5)
vio_cnt_2016_loc
#Following is the output of the above query, we see that the violation codes 21, 36, 38, 14 & 37 occur maximum number of times in 2016.
#	Year 	Violation Code   	count
#1 	2016             21 		664947
#2 	2016             36 		615242
#3 	2016             38 		547080
#4 	2016             14 		405885
#5 	2016             37 		330489


#For 2017
vio_cnt_2017 <- summarize(groupBy(nyc_2017, nyc_2017$Year, nyc_2017$`Violation Code`), count=n(nyc_2017$`Violation Code`))
vio_cnt_2017_loc <-head(arrange(vio_cnt_2017, desc(vio_cnt_2017$count)),5)
vio_cnt_2017_loc
#Following is the output of the above query, we see that the violation codes 21, 36, 38, 14 & 20 occur maximum number of times in 2017.
#	Year 	Violation Code   	count
#1 	2017             21 		768087
#2 	2017             36 		662765
#3 	2017             38 		542079
#4 	2017             14 		476664
#5 	2017             20 		319646

plot <- ggplot(rbind(vio_cnt_2015_loc, vio_cnt_2016_loc, vio_cnt_2017_loc) , aes(x = as.factor(`Violation Code`), y=count))
plot <- plot + geom_bar(stat="identity", position = "stack", fill ="#0683E5")
plot <- plot + geom_text(aes(label=count, y=count), position = position_stack(vjust = 0.5), size = 4, angle=90)
plot <- plot + labs(title = "Top 5 Violation code year wise", x = "VoilationCode", y = "Counts")
plot <- plot + facet_grid(~ Year)
plot <- plot + theme_bw()
plot <- plot + theme(axis.text.x = element_text(angle = 90))
plot



#Q2. How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)
#Answer - We will run separate year wise queries as below to get the count of parking tickets for vehicle body type and vehicle make

#For 2015 : vehicle body type
tkt_by_body_typ_2015 <- summarize(groupBy(nyc_2015, nyc_2015$Year, nyc_2015$`Vehicle Body Type`), count=n(nyc_2015$`Vehicle Body Type`))

tkt_by_body_typ_2015_loc <- head(arrange(tkt_by_body_typ_2015, desc(tkt_by_body_typ_2015$count)),5)
tkt_by_body_typ_2015_loc
#Following is the output of the above query, we see that the vehicle body types SUBN, 4DSD, VAN, DELV & SDN have maximum number of tickets in 2015.
#  Year 	Vehicle Body Type   count
#1 2015              SUBN 		1915458
#2 2015              4DSD 		1694252
#3 2015               VAN  		879764
#4 2015              DELV  		461300
#5 2015               SDN  		237304


#For 2016 : vehicle body type
tkt_by_body_typ_2016 <- summarize(groupBy(nyc_2016, nyc_2016$Year, nyc_2016$`Vehicle Body Type`), count=n(nyc_2016$`Vehicle Body Type`))

tkt_by_body_typ_2016_loc<- head(arrange(tkt_by_body_typ_2016, desc(tkt_by_body_typ_2016$count)),5)
tkt_by_body_typ_2016_loc
#Following is the output of the above query, we see that the vehicle body types SUBN, 4DSD, VAN, DELV & SDN have maximum number of tickets in 2016.
#  Year 	Vehicle Body Type   count
#1 2016              SUBN 		1596326
#2 2016              4DSD 		1354001
#3 2016               VAN  		722234
#4 2016              DELV  		354388
#5 2016               SDN  		178954


#For 2017 : vehicle body type
tkt_by_body_typ_2017 <- summarize(groupBy(nyc_2017, nyc_2017$Year, nyc_2017$`Vehicle Body Type`), count=n(nyc_2017$`Vehicle Body Type`))

tkt_by_body_typ_2017_loc <- head(arrange(tkt_by_body_typ_2017, desc(tkt_by_body_typ_2017$count)),5)
tkt_by_body_typ_2017_loc
#Following is the output of the above query, we see that the vehicle body types SUBN, 4DSD, VAN, DELV & SDN have maximum number of tickets in 2017.
#  Year 	Vehicle Body Type   count
#1 2017              SUBN 		1883954
#2 2017              4DSD 		1547312
#3 2017               VAN  		724029
#4 2017              DELV  		358984
#5 2017               SDN  		194197

#From the above queries its observed that for all the 3 years, vehicle body types SUBN, 4DSD, VAN, DELV & SDN have maximum number of tickets

plot <- ggplot(rbind(tkt_by_body_typ_2015_loc, tkt_by_body_typ_2016_loc, tkt_by_body_typ_2017_loc) , aes(x = `Vehicle Body Type`, y=count))
plot <- plot + geom_bar(stat="identity", position = "stack", fill ="#F11ED8")
plot <- plot + geom_text(aes(label=count, y=count), position = position_stack(vjust = 0.8), size = 3, angle=90)
plot <- plot + labs(title = "Top 5 Vehicle Body Type year wise", x = "Vehicle Body Type", y = "Counts")
plot <- plot + facet_grid(~ Year)
plot <- plot + theme_bw()
plot <- plot + theme(axis.text.x = element_text(angle = 90))
plot


#For 2015 : vehicle make
tkt_by_make_typ_2015 <- summarize(groupBy(nyc_2015, nyc_2015$Year, nyc_2015$`Vehicle Make`), count=n(nyc_2015$`Vehicle Make`))

tkt_by_make_typ_2015_loc <- head(arrange(tkt_by_make_typ_2015, desc(tkt_by_make_typ_2015$count)),5)
tkt_by_make_typ_2015_loc
#Following is the output of the above query, we see that the vehicel make FORD, TOYOT, HONDA, NISSA & CHEVR get maximum parking tickets in 2015
#	Year 	Vehicle Make   	count
#1 	2015         FORD 		763108
#2 	2015        TOYOT 		619164
#3 	2015        HONDA 		557315
#4 	2015        NISSA 		459982
#5 	2015        CHEVR 		450117


#For 2016 : vehicle make
tkt_by_make_typ_2016 <- summarize(groupBy(nyc_2016, nyc_2016$Year, nyc_2016$`Vehicle Make`), count=n(nyc_2016$`Vehicle Make`))

tkt_by_make_typ_2016_loc <-head(arrange(tkt_by_make_typ_2016, desc(tkt_by_make_typ_2016$count)),5)
tkt_by_make_typ_2016_loc

#Following is the output of the above query, we see that the vehicel make FORD, TOYOT, HONDA, NISSA & CHEVR get maximum parking tickets in 2016
#	Year 	Vehicle Make   	count
#1 	2016         FORD 		612276
#2 	2016        TOYOT 		529115
#3 	2016        HONDA 		459469
#4 	2016        NISSA 		382082
#5 	2016        CHEVR 		339466


#For 2017 : vehicle make
tkt_by_make_typ_2017 <- summarize(groupBy(nyc_2017, nyc_2017$Year, nyc_2017$`Vehicle Make`), count=n(nyc_2017$`Vehicle Make`))

tkt_by_make_typ_2017_loc <- head(arrange(tkt_by_make_typ_2017, desc(tkt_by_make_typ_2017$count)),5)
tkt_by_make_typ_2017_loc
#Following is the output of the above query, we see that the vehicel make FORD, TOYOT, HONDA, NISSA & CHEVR get maximum parking tickets in 2017
#	Year 	Vehicle Make   	count
#1 	2017         FORD 		636844
#2 	2017        TOYOT 		605291
#3 	2017        HONDA 		538884
#4 	2017        NISSA	 	462017
#5 	2017        CHEVR 		356032

#From the above queries its observed that for all the 3 years, vehicle make FORD, TOYOT, HONDA, NISSA & CHEVR get maximum number of tickets

plot <- ggplot(rbind(tkt_by_make_typ_2015_loc, tkt_by_make_typ_2016_loc, tkt_by_make_typ_2017_loc) , aes(x = `Vehicle Make`, y=count))
plot <- plot + geom_bar(stat="identity", position = "stack", fill ="#0ACDBC")
plot <- plot + geom_text(aes(label=count, y=count), position = position_stack(vjust = 0.5), size = 3, angle=90)
plot <- plot + labs(title = "Top 5 Vehicle Make year wise", x = "Vehicle Make", y = "Counts")
plot <- plot + facet_grid(~ Year)
plot <- plot + theme_bw()
plot <- plot + theme(axis.text.x = element_text(angle = 90))
plot


#Q3 A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:
#a) Violating Precincts (this is the precinct of the zone where the violation occurred)

#Answer - We will run year wise queries as below to get the frequency of violations for top 5 Violating Precincts

#For 2015 : Violating Precincts 
vio_pre_2015 <- summarize(groupBy(nyc_2015, nyc_2015$Year, nyc_2015$`Violation Precinct`), count=n(nyc_2015$`Violation Precinct`))

head(arrange(vio_pre_2015, desc(vio_pre_2015$count)),5)

#Following is the output of the above query, we see that the Violation Precinct 0, 19, 18, 14 & 1 have maximum violations in 2015
#	Year 	Violation Precinct  count
#1 	2015                  0 	801668
#2 	2015                 19 	320203
#3 	2015                 14 	217605
#4 	2015                 18 	215570
#5 	2015                  1 	169592


#For 2016 : Violating Precincts
vio_pre_2016 <- summarize(groupBy(nyc_2016, nyc_2016$Year, nyc_2016$`Violation Precinct`), count=n(nyc_2016$`Violation Precinct`))

head(arrange(vio_pre_2016, desc(vio_pre_2016$count)),5)

#Following is the output of the above query, we see that the Violation Precinct 0, 19, 13, 1 & 14 have maximum violations in 2016
#	Year 	Violation Precinct  count
#1 	2016                  0 		828348
#2 	2016                 19 		264299
#3 	2016                 13 		156144
#4 	2016                  1 		152231
#5 	2016                 14 		150637


#For 2017 : Violating Precincts
vio_pre_2017 <- summarize(groupBy(nyc_2017, nyc_2017$Year, nyc_2017$`Violation Precinct`), count=n(nyc_2017$`Violation Precinct`))

head(arrange(vio_pre_2017, desc(vio_pre_2017$count)),5)

#Following is the output of the above query, we see that the Violation Precinct 0, 19, 14, 1 & 18 have maximum violations in 2017
#	Year 	Violation Precinct  count
#1 2017                  0 		925596
#2 2017                 19 		274445
#3 2017                 14 		203553
#4 2017                  1 		174702
#5 2017                 18 		169131



#b) Issuing Precincts (this is the precinct that issued the ticket)

#Answer - We will run year wise queries as below to get the frequency of violations for top 5 Issuer Precincts

#For 2015 : Issuing Precincts
iss_pre_2015 <- summarize(groupBy(nyc_2015, nyc_2015$Year, nyc_2015$`Issuer Precinct`), count=n(nyc_2015$`Issuer Precinct`))

head(arrange(iss_pre_2015, desc(iss_pre_2015$count)),5)

#Following is the output of the above query, we see that the Issuer Precinct 0, 19, 18, 14 & 114 have issued maximum tickets in 2015
#	Year 	Issuer Precinct   	count
#1 	2015               0 		924018
#2 	2015              19 		310473
#3 	2015              18 		212130
#4 	2015              14 		210516
#5 	2015             114 		165345


#For 2016 : Issuing Precincts
iss_pre_2016 <- summarize(groupBy(nyc_2016, nyc_2016$Year, nyc_2016$`Issuer Precinct`), count=n(nyc_2016$`Issuer Precinct`))

head(arrange(iss_pre_2016, desc(iss_pre_2016$count)),5)

#Following is the output of the above query, we see that the Issuer Precinct 0, 19, 18, 14 & 1 have issued maximum tickets in 2016
#	Year 	Issuer Precinct   	count
#1 	2016               0 		948438
#2 	2016              19 		258049
#3 	2016              13 		153478
#4 	2016               1 		146987
#5 	2016              14 		146165


#For 2017 : Issuing Precincts
iss_pre_2017 <- summarize(groupBy(nyc_2017, nyc_2017$Year, nyc_2017$`Issuer Precinct`), count=n(nyc_2017$`Issuer Precinct`))

head(arrange(iss_pre_2017, desc(iss_pre_2017$count)),5)

#Following is the output of the above query, we see that the Issuer Precinct 0, 19, 14, 1 & 18 have issued maximum tickets in 2017
#	Year 	Issuer Precinct   	count
#1 	2017               0 		1078406
#2 	2017              19  		266961
#3 	2017              14  		200495
#4 	2017               1  		168740
#5 	2017              18  		162994



#Q4. Find the violation code frequency across 3 precincts which have issued the most number of tickets -
#do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?

#Answer - from query output in question 3, we know the top 3 Issuer Precincts for the three years. We will use them in the below queries  

#For 2015, from query output in question 3, we know that Issuer Precincts 0, 19 & 18 issued maximum tickets in 2015

vio_freq_2015 <- summarize(groupBy(filter(nyc_2015, nyc_2015$`Issuer Precinct` == '0' | nyc_2015$`Issuer Precinct` == '19' | nyc_2015$`Issuer Precinct` == '18'), nyc_2015$Year, nyc_2015$`Issuer Precinct`, nyc_2015$`Violation Code`), count=n(nyc_2015$`Violation Code`))

head(arrange(vio_freq_2015, desc(vio_freq_2015$count)),10)

#Following is the output of the above query, we see that the violation codes 36, 7 & 21 have exceptionally high frequency and are not common in all 3 precints
#	Year 	Issuer Precinct Violation Code  count
#1  2015               0             36 	457313
#2  2015               0              7 	276241
#3  2015               0             21 	108773
#4  2015              18             14  	66434
#5  2015               0              5  	61342
#6  2015              19             38  	51710
#7  2015              19             37  	45343
#8  2015              19             14  	34409
#9  2015              19             16  	32741
#10 2015              18             69  	31207


#For 2016, from query output in question 3, we know that Issuer Precincts 0, 19 & 13 issued maximum tickets in 2016

vio_freq_2016 <- summarize(groupBy(filter(nyc_2016, nyc_2016$`Issuer Precinct` == '0' | nyc_2016$`Issuer Precinct` == '19' | nyc_2016$`Issuer Precinct` == '13'), nyc_2016$Year, nyc_2016$`Issuer Precinct`, nyc_2016$`Violation Code`), count=n(nyc_2016$`Violation Code`))

head(arrange(vio_freq_2016, desc(vio_freq_2016$count)),10)

#Following is the output of the above query, we see that the violation codes 36, 7 & 21 have exceptionally high frequency and are not common in all 3 precints
#	Year 	Issuer Precinct Violation Code  count
#1  2016               0             36 	615242
#2  2016               0              7 	165111
#3  2016               0             21 	104351
#4  2016               0              5  	43467
#5  2016              19             37  	38052
#6  2016              19             38  	37855
#7  2016              19             46  	36442
#8  2016              19             14  	28772
#9  2016              19             21  	25588
#10 2016              19             16  	24647


#For 2017, from query output in question 3, we know that Issuer Precincts 0, 19 & 14 issued maximum tickets in 2017

vio_freq_2017 <- summarize(groupBy(filter(nyc_2017, nyc_2017$`Issuer Precinct` == '0' | nyc_2017$`Issuer Precinct` == '19' | nyc_2017$`Issuer Precinct` == '14'), nyc_2017$Year, nyc_2017$`Issuer Precinct`, nyc_2017$`Violation Code`), count=n(nyc_2017$`Violation Code`))

head(arrange(vio_freq_2017, desc(vio_freq_2017$count)),10)

#Following is the output of the above query, we see that the violation codes 36, 7 & 21 have exceptionally high frequency and are not common in all 3 precints
#	Year 	Issuer Precinct Violation Code  count
#1  2017               0             36 	662765
#2  2017               0              7 	210175
#3  2017               0             21 	126053
#4  2017              19             46 	 48445
#5  2017               0              5 	 48076
#6  2017              14             14 	 45036
#7  2017              19             38 	 36386
#8  2017              19             37 	 36056
#9  2017              14             69 	 30464
#10 2017              19             14 	 29797

#From the above queries its observed that for all the 3 years, violation codes 36 & 7 have exceptionally high frequency and are not common in all 3 precints



#5. You’d want to find out the properties of parking violations across different times of the day:

#a) The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.

#Answer - The Violation Time is present as a string. Its format is HHMMA or HHMMP
#The first two letters signify hours, the next two letters signify minutes and the last letter signify AM or PM
#We will treat it as a string and will subset it to get the hour, minute and AM/PM information


#b) Find a way to deal with missing values, if any.

#Answer - As seen in the EDA query above, Violation time does have NA values, which are 0.005% and can be excluded. 
#Further, the way we are subsetting the string, it will automatically exclude the NA values 


#c) Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. 
#For each of these groups, find the 3 most commonly occurring violations

#Answer - We will bin the time into the following buckets
#Early_Morning_AM : 00-03 hours
#Morning_AM : 04-07 hours
#Before_Noon_AM : 08-11 hours
#After_Noon_PM : 12-03 hours
#Evening_PM : 04-07 hours
#Night_PM : 08-11 hours
 
#For 2015

vio_tim_cod_2015 <- SparkR::sql("select AM_PM ,vio_cd, vio_cd_cnt, rank from (select AM_PM ,vio_cd ,count(vio_cd) as vio_cd_cnt, dense_rank() over(partition by AM_PM order by count(vio_cd) desc) as rank from (select
                                `Violation Code` as vio_cd
                                ,case when substring (`Violation Time`,0, 2) in ('00','01','02','03')  and substring(`Violation Time` ,5,1) = 'A' then 'Early_Morning_AM'
                                when substring(`Violation Time`,0,2) in ('04','05','06','07') and substring(`Violation Time` ,5,1) = 'A' then 'Morning_AM'
                                when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time` ,5,1) = 'A' then 'Before_Noon_AM'
                                when substring(`Violation Time` , 0,2) in ('12','01','02','03') and substring(`Violation Time`, 5,1) = 'P' then 'After_Noon_PM'
                                when substring(`Violation Time`, 0,2) in ('04','05','06','07') and substring(`Violation Time`, 5,1) = 'P' then 'Evening_PM'
                                when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time`, 5,1) = 'P' then 'Night_PM'
                                end as AM_PM from nyc_2015_tbl) temp group by vio_cd, AM_PM)temp1 where AM_PM is not null and rank<=3")
head(vio_tim_cod_2015,18)

#From the output of the above query, following are the 3 most commonly occurring violations for each time interval in 2015
#			AM_PM 		vio_cd 		vio_cd_cnt 	rank
#1    Before_Noon_AM     21     		643800    1
#2    Before_Noon_AM     38     		264213    2
#3    Before_Noon_AM     36     		213760    3
#4     After_Noon_PM     38     		323541    1
#5     After_Noon_PM     37     		237590    2
#6     After_Noon_PM     36     		200027    3
#7        Morning_AM     14     		 76126    1
#8        Morning_AM     21     		 55592    2
#9        Morning_AM     40     		 52012    3
#10 Early_Morning_AM     21     		 33152    1
#11 Early_Morning_AM     40     		 19982    2
#12 Early_Morning_AM     78     		 16851    3
#13       Evening_PM     38     		126304    1
#14       Evening_PM     37     		 93358    2
#15       Evening_PM     14     		 82683    3
#16         Night_PM      7     		 32608    1
#17         Night_PM     38     		 30667    2
#18         Night_PM     40     		 25516    3



#For 2016

vio_tim_cod_2016 <- SparkR::sql("select AM_PM ,vio_cd, vio_cd_cnt, rank from (select AM_PM ,vio_cd ,count(vio_cd) as vio_cd_cnt, dense_rank() over(partition by AM_PM order by count(vio_cd) desc) as rank from (select
                                `Violation Code` as vio_cd
                                ,case when substring (`Violation Time`,0, 2) in ('00','01','02','03')  and substring(`Violation Time` ,5,1) = 'A' then 'Early_Morning_AM'
                                when substring(`Violation Time`,0,2) in ('04','05','06','07') and substring(`Violation Time` ,5,1) = 'A' then 'Morning_AM'
                                when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time` ,5,1) = 'A' then 'Before_Noon_AM'
                                when substring(`Violation Time` , 0,2) in ('12','01','02','03') and substring(`Violation Time`, 5,1) = 'P' then 'After_Noon_PM'
                                when substring(`Violation Time`, 0,2) in ('04','05','06','07') and substring(`Violation Time`, 5,1) = 'P' then 'Evening_PM'
                                when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time`, 5,1) = 'P' then 'Night_PM'
                                end as AM_PM from nyc_2016_tbl) temp group by vio_cd, AM_PM)temp1 where AM_PM is not null and rank<=3")
head(vio_tim_cod_2016,18)

#From the output of the above query, following are the 3 most commonly occurring violations for each time interval in 2016
#			AM_PM 		vio_cd 			vio_cd_cnt 	rank
#1    Before_Noon_AM     21     			525280    1
#2    Before_Noon_AM     36     			284279    2
#3    Before_Noon_AM     38     			185395    3
#4     After_Noon_PM     36     			273581    1
#5     After_Noon_PM     38     			234220    2
#6     After_Noon_PM     37     			183853    3
#7        Morning_AM     14     			 65347    1
#8        Morning_AM     21     			 48240    2
#9        Morning_AM     40     			 42306    3
#10 Early_Morning_AM     21     			 29884    1
#11 Early_Morning_AM     40     			 17033    2
#12 Early_Morning_AM     78     			 13282    3
#13       Evening_PM     38     			105657    1
#14       Evening_PM     37     			 79991    2
#15       Evening_PM     14     			 63778    3
#16         Night_PM     38     			 20851    1
#17         Night_PM      7     			 20246    2
#18         Night_PM     40     			 20030    3



#For 2017

vio_tim_cod_2017 <- SparkR::sql("select AM_PM ,vio_cd, vio_cd_cnt, rank from (select AM_PM ,vio_cd ,count(vio_cd) as vio_cd_cnt, dense_rank() over(partition by AM_PM order by count(vio_cd) desc) as rank from (select
                                `Violation Code` as vio_cd
                                ,case when substring (`Violation Time`,0, 2) in ('00','01','02','03')  and substring(`Violation Time` ,5,1) = 'A' then 'Early_Morning_AM'
                                when substring(`Violation Time`,0,2) in ('04','05','06','07') and substring(`Violation Time` ,5,1) = 'A' then 'Morning_AM'
                                when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time` ,5,1) = 'A' then 'Before_Noon_AM'
                                when substring(`Violation Time` , 0,2) in ('12','01','02','03') and substring(`Violation Time`, 5,1) = 'P' then 'After_Noon_PM'
                                when substring(`Violation Time`, 0,2) in ('04','05','06','07') and substring(`Violation Time`, 5,1) = 'P' then 'Evening_PM'
                                when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time`, 5,1) = 'P' then 'Night_PM'
                                end as AM_PM from nyc_2017_tbl) temp group by vio_cd, AM_PM)temp1 where AM_PM is not null and rank<=3")
head(vio_tim_cod_2017,18)

#From the output of the above query, following are the 3 most commonly occurring violations for each time interval in 2017
#			AM_PM 		vio_cd 			vio_cd_cnt 	rank
#1    Before_Noon_AM     21     			598069    1
#2    Before_Noon_AM     36     			348165    2
#3    Before_Noon_AM     38     			176570    3
#4     After_Noon_PM     36     			286284    1
#5     After_Noon_PM     38     			240721    2
#6     After_Noon_PM     37     			167025    3
#7        Morning_AM     14     			 74114    1
#8        Morning_AM     40     			 60652    2
#9        Morning_AM     21     			 57897    3
#10 Early_Morning_AM     21     			 34703    1
#11 Early_Morning_AM     40     			 23628    2
#12 Early_Morning_AM     14     			 14168    3
#13       Evening_PM     38     			102855    1
#14       Evening_PM     14     			 75902    2
#15       Evening_PM     37     			 70345    3
#16         Night_PM      7     			 26293    1
#17         Night_PM     40     			 22337    2
#18         Night_PM     14     			 21045    3


#d) Now, try another direction. For the 3 most commonly occurring violation codes, 
#find the most common times of day (in terms of the bins from the previous part)

#Answer - We will first find the 3 most commonly occurring violation codes and then find the most common time of the day (bin)

#For 2015 : finding the 3 most commonly occurring violation codes
vio_cnt_2015 <- summarize(groupBy(nyc_2015, nyc_2015$Year, nyc_2015$`Violation Code`), count=n(nyc_2015$`Violation Code`))

head(arrange(vio_cnt_2015, desc(vio_cnt_2015$count)),3)

#Following is the output of the above query, we see that the violation codes 21, 38 & 14 are the three most common violation codes in 2015.
#	Year 	Violation Code   	count
#1 	2015             21 		809914
#2 	2015             38 		746562
#3 	2015             14 		517733


#For 2015 : finding the most common times of day based on the 3 most commonly occurring violation codes

time_bin_2015 <- SparkR::sql("select AM_PM , count(vio_cd) as vio_cd_cnt from (select
                      `Violation Code` as vio_cd
                      ,case when substring (`Violation Time`,0, 2) in ('00','01','02','03')  and substring(`Violation Time` ,5,1) = 'A' then 'Early_Morning_AM'
                       when substring(`Violation Time`,0,2) in ('04','05','06','07') and substring(`Violation Time` ,5,1) = 'A' then 'Morning_AM'
                       when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time` ,5,1) = 'A' then 'Before_Noon_AM'
                       when substring(`Violation Time` , 0,2) in ('12','01','02','03') and substring(`Violation Time`, 5,1) = 'P' then 'After_Noon_PM'
                       when substring(`Violation Time`, 0,2) in ('04','05','06','07') and substring(`Violation Time`, 5,1) = 'P' then 'Evening_PM'
                       when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time`, 5,1) = 'P' then 'Night_PM'
                       else '' end as AM_PM from nyc_2015_tbl where `Violation Code` = 21 or `Violation Code` = 38 or `Violation Code` =14) temp group by AM_PM order by vio_cd_cnt desc")
head(time_bin_2015)

#Following is the output of the above query, we see that Before_Noon_AM (08-11 hours) has the most violations in 2015.
#			AM_PM 		vio_cd_cnt
#1   Before_Noon_AM    	1075511
#2    After_Noon_PM     549402
#3       Evening_PM     209350
#4       Morning_AM     133190
#5         Night_PM      54565
#6 Early_Morning_AM      48389


#For 2016 : finding the 3 most commonly occurring violation codes
vio_cnt_2016 <- summarize(groupBy(nyc_2016, nyc_2016$Year, nyc_2016$`Violation Code`), count=n(nyc_2016$`Violation Code`))

head(arrange(vio_cnt_2016, desc(vio_cnt_2016$count)),3)

#Following is the output of the above query, we see that the violation codes 21, 36 & 38 are the three most common violation codes in 2016.
#	Year 	Violation Code   	count
#1 2016             21 			664947
#2 2016             36 			615242
#3 2016             38 			547080


#For 2016 : finding the most common times of day based on the 3 most commonly occurring violation codes

time_bin_2016 <- SparkR::sql("select AM_PM , count(vio_cd) as vio_cd_cnt from (select
                      `Violation Code` as vio_cd
                      ,case when substring (`Violation Time`,0, 2) in ('00','01','02','03')  and substring(`Violation Time` ,5,1) = 'A' then 'Early_Morning_AM'
                       when substring(`Violation Time`,0,2) in ('04','05','06','07') and substring(`Violation Time` ,5,1) = 'A' then 'Morning_AM'
                       when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time` ,5,1) = 'A' then 'Before_Noon_AM'
                       when substring(`Violation Time` , 0,2) in ('12','01','02','03') and substring(`Violation Time`, 5,1) = 'P' then 'After_Noon_PM'
                       when substring(`Violation Time`, 0,2) in ('04','05','06','07') and substring(`Violation Time`, 5,1) = 'P' then 'Evening_PM'
                       when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time`, 5,1) = 'P' then 'Night_PM'
                       else '' end as AM_PM from nyc_2016_tbl where `Violation Code` = 21 or `Violation Code` = 36 or `Violation Code` =38) temp group by AM_PM order by vio_cd_cnt desc")
head(time_bin_2016)

#Following is the output of the above query, we see that Before_Noon_AM (08-11 hours) has the most violations in 2016.
#			AM_PM 		vio_cd_cnt
#1   Before_Noon_AM     994954
#2    After_Noon_PM     566787
#3       Evening_PM     124170
#4       Morning_AM      88213
#5 Early_Morning_AM      29970
#6         Night_PM      21039


#For 2017 : finding the 3 most commonly occurring violation codes

vio_cnt_2017 <- summarize(groupBy(nyc_2017, nyc_2017$Year, nyc_2017$`Violation Code`), count=n(nyc_2017$`Violation Code`))

head(arrange(vio_cnt_2017, desc(vio_cnt_2017$count)),3)

#Following is the output of the above query, we see that the violation codes 21, 36 & 38 are the three most common violation codes in 2017.
#	Year 	Violation Code   	count
#1 	2017             21 		768087
#2 	2017             36 		662765
#3 	2017             38 		542079


#For 2017 : finding the most common times of day based on the 3 most commonly occurring violation codes

time_bin_2017 <- SparkR::sql("select AM_PM , count(vio_cd) as vio_cd_cnt from (select
                      `Violation Code` as vio_cd
                      ,case when substring (`Violation Time`,0, 2) in ('00','01','02','03')  and substring(`Violation Time` ,5,1) = 'A' then 'Early_Morning_AM'
                       when substring(`Violation Time`,0,2) in ('04','05','06','07') and substring(`Violation Time` ,5,1) = 'A' then 'Morning_AM'
                       when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time` ,5,1) = 'A' then 'Before_Noon_AM'
                       when substring(`Violation Time` , 0,2) in ('12','01','02','03') and substring(`Violation Time`, 5,1) = 'P' then 'After_Noon_PM'
                       when substring(`Violation Time`, 0,2) in ('04','05','06','07') and substring(`Violation Time`, 5,1) = 'P' then 'Evening_PM'
                       when substring(`Violation Time`,0,2) in ('08','09','10','11') and substring(`Violation Time`, 5,1) = 'P' then 'Night_PM'
                       else '' end as AM_PM from nyc_2017_tbl where `Violation Code` = 21 or `Violation Code` = 36 or `Violation Code` =38) temp group by AM_PM order by vio_cd_cnt desc")
head(time_bin_2017)

#Following is the output of the above query, we see that Before_Noon_AM (08-11 hours) has the most violations in 2017.
#			AM_PM 		vio_cd_cnt
#1   Before_Noon_AM    	1122804
#2    After_Noon_PM     601698
#3       Evening_PM     116648
#4       Morning_AM      73952
#5 Early_Morning_AM      34941
#6         Night_PM      20531

#From the above queries its observed that for all the 3 years, Before_Noon_AM (08-11 hours) has the most violations



#6. Let’s try and find some seasonality in this data
#First, divide the year into some number of seasons, and find frequencies of tickets for each season.
#Then, find the 3 most common violations for each of these season

#Answer - We will divide the year based on the ticket Issue Date, which is present as a string. Its format is mm/dd/yyyy
#We will treat it as a string and will subset it to get the month information and divide the year into following seasons
#Summer : 06-08 months (Jun-Aug)
#Fall : 09-11 months (Sep-Nov)
#Winter : 12-02 months (Dec-Feb)
#Spring : 03-05 months (Mar-May)


#For 2015 : finding frequencies of tickets for each season
seasons_2015 <- SparkR::sql("select season, count(`Summons Number`) as tkt_freq from (SELECT `Summons Number` 
                      ,case when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'  
                      when substring(`Issue Date`,0,2) in ('09','10','11') then 'Fall'
                      when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'
                      when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring' end as season
                      FROM nyc_2015_tbl) temp 
                      where `season` is not null group by season order by tkt_freq desc")
head(seasons_2015)

#Following is the output of the above query, we see that the Spring(Mar-May) season has maximum ticket requency for 2015
#	season 	tkt_freq
#1 	Spring  2860998
#2 	Winter  2121734
#3 	Summer  1004099


#For 2016 : finding frequencies of tickets for each season
seasons_2016 <- SparkR::sql("select season, count(`Summons Number`) as tkt_freq from (SELECT `Summons Number` 
                      ,case when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'  
                      when substring(`Issue Date`,0,2) in ('09','10','11') then 'Fall'
                      when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'
                      when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring' end as season
                      FROM nyc_2016_tbl) temp 
                      where `season` is not null group by season order by tkt_freq desc")
head(seasons_2016)

#Following is the output of the above query, we see that the Spring(Mar-May) season has maximum ticket requency for 2016
#	season 	tkt_freq
#1 	Spring  2789066
#2 	Winter  1654856
#3 	Summer   427796
#4   Fall      903


#For 2017 : finding frequencies of tickets for each season
seasons_2017 <- SparkR::sql("select season, count(`Summons Number`) as tkt_freq from (SELECT `Summons Number` 
                      ,case when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'  
                      when substring(`Issue Date`,0,2) in ('09','10','11') then 'Fall'
                      when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'
                      when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring' end as season
                      FROM nyc_2017_tbl) temp 
                      where `season` is not null group by season order by tkt_freq desc")
head(seasons_2017)

#Following is the output of the above query, we see that the Spring(Mar-May) season has maximum ticket requency for 2017
#	season 	tkt_freq
#1 	Spring  2873383
#2 	Winter  1704690
#3 	Summer   852866
#4   Fall      979



#For 2015 : finding the 3 most common violations for each of the seasons
season_vio_cod_2015 <- SparkR::sql("select season, vio_code, smn_cnt, rank from (select season, vio_code, count(`Summons Number`) as smn_cnt,
                       dense_rank() over(partition by season order by count(vio_code) desc) as rank from 
                      (SELECT `Summons Number` ,`Violation Code` as vio_code
                      ,case when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'  
                      when substring(`Issue Date`,0,2) in ('09','10','11') then 'Fall'
                      when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'
                      when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring' end as season
                      FROM nyc_2015_tbl) temp group by season, vio_code)temp1 where season is not null and rank<=3")
head(season_vio_cod_2015,12)

#Following is the output of the above query, we see that the 3 most common violations for 2015 are Spring-21,38,14 : Summer-21,38,36 : Winter-38,21,14
#	season 	vio_code smn_cnt rank
#1 Spring       21  425164    1
#2 Spring       38  327048    2
#3 Spring       14  243624    3
#4 Summer       21  169470    1
#5 Summer       38  107363    2
#6 Summer       36   86115    3
#7 Winter       38  312151    1
#8 Winter       21  215280    2
#9 Winter       14  189939    3


#For 2016 : finding the 3 most common violations for each of the seasons
season_vio_cod_2016 <- SparkR::sql("select season, vio_code, smn_cnt, rank from (select season, vio_code, count(`Summons Number`) as smn_cnt,
                       dense_rank() over(partition by season order by count(vio_code) desc) as rank from 
                      (SELECT `Summons Number` ,`Violation Code` as vio_code
                      ,case when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'  
                      when substring(`Issue Date`,0,2) in ('09','10','11') then 'Fall'
                      when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'
                      when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring' end as season
                      FROM nyc_2016_tbl) temp group by season, vio_code)temp1 where season is not null and rank<=3")
head(season_vio_cod_2016,12)

#Following is the output of the above query, we see that the 3 most common violations for 2015 are Spring-21,36,38 : Summer-21,38,14 : Fall-46,21,40 : Winter-21,36,38
#	season 	vio_code smn_cnt rank
#1  Spring       21  383448    1
#2  Spring       36  374362    2
#3  Spring       38  299439    3
#4  Summer       21   60007    1
#5  Summer       38   51077    2
#6  Summer       14   45020    3
#7    Fall       46     195    1
#8    Fall       21     192    2
#9    Fall       40      79    3
#10 Winter       21  221300    1
#11 Winter       36  200971    2
#12 Winter       38  196559    3


#For 2017 : finding the 3 most common violations for each of the seasons
season_vio_cod_2017 <- SparkR::sql("select season, vio_code, smn_cnt, rank from (select season, vio_code, count(`Summons Number`) as smn_cnt,
                       dense_rank() over(partition by season order by count(vio_code) desc) as rank from 
                      (SELECT `Summons Number` ,`Violation Code` as vio_code
                      ,case when substring(`Issue Date`,0,2) in ('06','07','08') then 'Summer'  
                      when substring(`Issue Date`,0,2) in ('09','10','11') then 'Fall'
                      when substring(`Issue Date`,0,2) in ('12','01','02') then 'Winter'
                      when substring(`Issue Date`,0,2) in ('03','04','05') then 'Spring' end as season
                      FROM nyc_2017_tbl) temp group by season, vio_code)temp1 where season is not null and rank<=3")
head(season_vio_cod_2017,12)

#Following is the output of the above query, we see that the 3 most common violations for 2017 are Spring-21,36,38 : Summer-21,36,38 : Fall-46,21,40 : Winter-21,36,38
#	season 	vio_code smn_cnt rank
#1  Spring       21  402424    1
#2  Spring       36  344834    2
#3  Spring       38  271167    3
#4  Summer       21  127352    1
#5  Summer       36   96663    2
#6  Summer       38   83518    3
#7    Fall       46     231    1
#8    Fall       21     128    2
#9    Fall       40     116    3
#10 Winter       21  238183    1
#11 Winter       36  221268    2
#12 Winter       38  187386    3



# 7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. 
#Let’s take an example of estimating that for the 3 most commonly occurring codes.

#a) Find total occurrences of the 3 most common violation codes

#Answer - we will use below queries to find total occurrences of the 3 most common violation codes for the 3 years

#For 2015
vio_cnt_2015 <- summarize(groupBy(nyc_2015, nyc_2015$Year, nyc_2015$`Violation Code`), count=n(nyc_2015$`Violation Code`))

head(arrange(vio_cnt_2015, desc(vio_cnt_2015$count)),3)

#Following is the output of the above query, we see that the violation codes 21, 38 & 14 are the three most common violation codes in 2015.
#	Year 	Violation Code   	count
#1 	2015             21 		809914
#2 	2015             38 		746562
#3 	2015             14 		517733


#For 2016
vio_cnt_2016 <- summarize(groupBy(nyc_2016, nyc_2016$Year, nyc_2016$`Violation Code`), count=n(nyc_2016$`Violation Code`))

head(arrange(vio_cnt_2016, desc(vio_cnt_2016$count)),3)

#Following is the output of the above query, we see that the violation codes 21, 36 & 38 are the three most common violation codes in 2016.
#	Year 	Violation Code   	count
#1 	2016             21 		664947
#2 	2016             36 		615242
#3 	2016             38 		547080


#For 2017
vio_cnt_2017 <- summarize(groupBy(nyc_2017, nyc_2017$Year, nyc_2017$`Violation Code`), count=n(nyc_2017$`Violation Code`))

head(arrange(vio_cnt_2017, desc(vio_cnt_2017$count)),3)

#Following is the output of the above query, we see that the violation codes 21, 36 & 38 are the three most common violation codes in 2017.
#	Year 	Violation Code   	count
#1 2017             21 			768087
#2 2017             36 			662765
#3 2017             38 			542079


#b) Then, search the internet for NYC parking violation code fines. You will find a website (on the nyc.gov URL) that lists these fines. 
#They’re divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. 
#For simplicity, take an average of the two.

#Answer - we extracted the fine amount for each violation code from http://www1.nyc.gov 
# We used the below average fine amount for mostly occuring violation codes for futher analysis
#	Violation Code   	average fine amount
#        21 				55
#        36 				50
#        38 				50
#		 14					115


#c) Using this information, find the total amount collected for all of the fines. State the code which has the highest total collection.

#Answer - we will use below queries to find total amount collected for all of the fines for the 3 years

#For 2015
coll_amnt_2015 <- SparkR::sql("select yr, vio_cd,cnt, case when vio_cd = '21' then (cnt*55) 
                               when vio_cd = '36' then (cnt*50)
                               when vio_cd = '38' then (cnt*50)
                               when vio_cd = '14' then (cnt*115) end as coll_amnt from
                              (SELECT yr, vio_cd, count(vio_cd) as cnt from (select
                              case when (substring(`Issue Date`,7,4) = '2015') then '2015' end as yr
                              ,`Violation Code` as vio_cd
                              FROM nyc_2015_tbl where `Violation Code` is not null 
                              and substring(`Issue Date`,7,4) = '2015' and `Issue Date` is not null)temp
                              group by yr, vio_cd order by cnt desc limit 3) temp1 order by coll_amnt desc ")
head(coll_amnt_2015)

#Following is the output of the above query, we see that violation codes 14 has highest collection in 2015.
#	yr 		vio_cd    cnt 		coll_amnt
#1 2015     14 		517733  	59539295
#2 2015     21 		809914  	44545270
#3 2015     38 		746562  	37328100


#For 2016
coll_amnt_2016 <- SparkR::sql("select yr, vio_cd,cnt, case when vio_cd = '21' then (cnt*55) 
                               when vio_cd = '36' then (cnt*50)
                               when vio_cd = '38' then (cnt*50)
                               when vio_cd = '14' then (cnt*115) end as coll_amnt from
                              (SELECT yr, vio_cd, count(vio_cd) as cnt from (select
                              case when (substring(`Issue Date`,7,4) = '2016') then '2016' end as yr
                              ,`Violation Code` as vio_cd
                              FROM nyc_2016_tbl where `Violation Code` is not null 
                              and substring(`Issue Date`,7,4) = '2016' and `Issue Date` is not null)temp
                              group by yr, vio_cd order by cnt desc limit 3) temp1 order by coll_amnt desc ")
head(coll_amnt_2016)

#Following is the output of the above query, we see that violation codes 21 has highest collection in 2016.
#	yr 		vio_cd    cnt 		coll_amnt
#1 2016     21 		664947  	36572085
#2 2016     36 		615242  	30762100
#3 2016     38 		547080  	27354000


#For 2017
coll_amnt_2017 <- SparkR::sql("select yr, vio_cd,cnt, case when vio_cd = '21' then (cnt*55) 
                               when vio_cd = '36' then (cnt*50)
                               when vio_cd = '38' then (cnt*50)
                               when vio_cd = '14' then (cnt*115) end as coll_amnt from
                              (SELECT yr, vio_cd, count(vio_cd) as cnt from (select
                              case when (substring(`Issue Date`,7,4) = '2017') then '2017' end as yr
                              ,`Violation Code` as vio_cd
                              FROM nyc_2017_tbl where `Violation Code` is not null 
                              and substring(`Issue Date`,7,4) = '2017' and `Issue Date` is not null)temp
                              group by yr, vio_cd order by cnt desc limit 3) temp1 order by coll_amnt desc ")
head(coll_amnt_2017)

#Following is the output of the above query, we see that violation codes 21 has highest collection in 2017.
#	yr 		vio_cd    cnt 		coll_amnt
#1 2017     21 		768087  	42244785
#2 2017     36 		662765  	33138250
#3 2017     38 		542079  	27103950


#d) What can you intuitively infer from these findings?

#Answer - Following are the inferences based on the above queries
#i. Violation codes 21,36,38 and 14 occur maximum number of times 
#ii. Year wise profitable violation codes
# 		2017 - 21
# 		2016 - 21
# 		2015 - 14
#iii. For 2015, though violation code 14 has the least count of violations, however due to the high fine amount associated with code 14, 
#it has generated the highest revenue.