library(tidyr)
library(readxl)
library(dplyr)
library(RJDBC)
library(stringr)
library(httr)
library(aws.s3)
library(aws.ec2metadata)


#Get Data from Redshift Table ----

######### Get Redshift creds #########
get_redshift_connection <- function() {
  driver <-
    JDBC(
      driverClass = "com.amazon.redshift.jdbc.Driver",
      # classPath = list.files('/usr/lib/drivers/', pattern = '*.jar', full.names = TRUE), #to run in MAP locally
      classPath = "/usr/lib/drivers/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar",
      identifier.quote = "`"
    )
  url <-
    str_glue(
      "jdbc:redshift://live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com:5439/redshiftdb?user={Sys.getenv('REDSHIFT_USERNAME')}&password={Sys.getenv('REDSHIFT_PASSWORD')}"
    )
  conn <- dbConnect(driver, url)
  return(conn)
}

# Variable to hold the connection info
conn <- get_redshift_connection()
dbGetQuery(conn, "SELECT * FROM prez.id_profile LIMIT 10;") #check connection works

### Create Relative Dates ### ---
args = commandArgs(trailingOnly=TRUE)
date_1 <- args[1]

run_date <- as.Date(date_1, format='%Y-%m-%d') #for  {{ds}}

date_end <- run_date
# date_end <- as.Date('2025-08-11') #use when testing dates, otherwise revert to line 45
date_start <- date_end - 98 #15 weeks

date_end_char <- as.character(date_end)
date_start_char <- as.character(date_start)

# Obtain Content Data for the last 15 weeks ----
data = dbGetQuery(conn, paste0("SELECT * FROM marketing_insights.in_content_media
                                        WHERE wc_date between '",
                               date_start_char,
                               "' and '",
                               date_end_char,
                               "';")) 

processed = data %>% arrange(desc(wc_date),desc(percentile_average)) %>% 
  mutate(digital_spend = formatC(digital_spend,format = "f", big.mark = ",",digits =0),
         owned_impressions = formatC(impressions,format = "f", big.mark = ",",digits =0),
         owned_tvrs = round(tvrs,0)) %>% 
  select(-c(impressions,tvrs)) %>% 
  relocate(-c(percentile_average,media_label))

### Set S3 Bucket and Write Data ###

s3_b <- "s3://map-input-output/ccog-content-media-report/" 

s3_obj <- paste0(s3_b,paste0("ccog_content_media_",date_end_char),".csv")
s3_obj_latest <- paste0(s3_b,"latest.csv")


s3write_using(processed, FUN = write.csv, row.names = FALSE,
              object = s3_obj)
s3write_using(processed, FUN = write.csv, row.names = FALSE,
              object = s3_obj_latest)

