#' produces a weekly report comprising media spends by title for the content team

# Write functions only and document them with roxygen-styled comments.
# Example below taken from http://r-pkgs.had.co.nz/man.html

install.packages("tidyverse")
library(tidyr)
library(readxl)
library(dplyr)
library(RJDBC)
library(stringr)
library(magrittr) 
library(data.table)
library(lubridate)
library(jsonlite)
library(httr)
library(tibble)
library(readr)
library(aws.s3)
library(aws.ec2metadata)

#Get Data from Redshift Table ----

######### Get Redshift creds #########
get_redshift_connection <- function() {
  driver <-
    JDBC(
      driverClass = "com.amazon.redshift.jdbc.Driver",
      classPath = "/usr/lib/drivers/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar",
      # classPath = list.files('/usr/lib/drivers/', pattern = '*.jar', full.names = TRUE), #to run in MAP locally
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