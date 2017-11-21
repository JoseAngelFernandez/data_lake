#install.packages("devtools")
#devtools::install_github("rstudio/sparklyr")
# library(SparkR, lib.loc="/opt/spark/R/lib")
# library(SparkR, lib.loc = "/home/joseangel/Downloads/spark-2.1.0-bin-hadoop2.7/R/lib/")
setwd("/home/joseangel/Documents/OFFSHORE/PreProcess")
source("/home/joseangel/Documents/OFFSHORE/PreProcess/Functions/pixel_preprocess.R")
source("/home/joseangel/Documents/OFFSHORE/PreProcess/Functions/signup_preprocess.R")
source("/home/joseangel/Documents/OFFSHORE/PreProcess/Functions/signoff_preprocess.R")
source("/home/joseangel/Documents/OFFSHORE/PreProcess/Functions/messagesent_preprocess.R")
source("/home/joseangel/Documents/OFFSHORE/PreProcess/Functions/reminder_preprocess.R")
source("/home/joseangel/Documents/OFFSHORE/PreProcess/Functions/receipt_preprocess.R")

library(sparklyr)
library(dplyr)
library(data.table)
library(aws.s3)

Sys.setenv(SPARK_HOME="/home/joseangel/Downloads/spark-2.1.0-bin-hadoop2.7")
Sys.setenv(AWS_ACCESS_KEY_ID="AKIAIHGN643VGRUGCDOQ")
Sys.setenv(AWS_SECRET_ACCESS_KEY="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")
Sys.setenv(AWS_REGION="eu-west-1")

config <- spark_config()
config$sparklyr.defaultPackages[[1]] <- "org.apache.hadoop:hadoop-aws:2.7.3"
sc <- sparklyr::spark_connect(
  master = "local",
  sparkHome = "/home/joseangel/Downloads/spark-2.1.0-bin-hadoop2.7",
  version = "2.1.0",
  appName = "spark_test",
  config = config
)

# Get file names from bucket
s3_path <- "s3://basebone.datalake/raw_data/"
l_filename <- system(
  paste(
    "aws s3 ls",
    s3_path,
    "| awk {'print $4'}"
  ), 
  intern = TRUE
)
# For testing purposes
l_filename <- l_filename[-1]

# ETL for Signups log
s3a_path <- 's3a://basebone.datalake/raw_data/'
s3a_path_output <- 's3a://basebone.datalake/processed_data/'

for ( file_name in l_filename) # Think if it is possible to do parallel
{
  # Interpret log name:
  # file_name <- "message_sent_2017.10.16_0_00_.log"
  name_parts <- unlist( strsplit(file_name,"_") )
  log_type <- name_parts[1]
  if (log_type=="message")
  {
    log_type <- "messagesent"
    log_date <- name_parts[3]
    name_parts_hour <- unlist( strsplit(name_parts[4],":") )
    log_hour <- name_parts_hour[1]
    log_min <- name_parts_hour[2]
  } else {
    log_date <- name_parts[2]
    name_parts_hour <- unlist( strsplit(name_parts[3],":") )
    log_hour <- name_parts_hour[1]
    log_min <- name_parts_hour[2]
  }
  print(file_name)
  
  if (log_type == "messagesent"  )#&& as.numeric(log_hour) == 0)
  {
    # Read log
    file_path <- paste0(s3a_path, file_name)
    print(file_path)
    if (log_type=="pixel")
    {
      new_rdd <- spark_read_csv(sc, "new_rdd", file_path, header = FALSE, delimiter = '"')
    } else {
      new_rdd <- spark_read_csv(sc, "new_rdd", file_path, header = FALSE, delimiter =" ")
    }
    new_rdd_tbl <- tbl(sc, "new_rdd")
    FULL_rdd_tbl <- NULL
    
    if (log_type == "messagesent")
    {
      FULL_rdd_tbl <- messagesent_preprocess(new_rdd_tbl)
    } else if (log_type == "pixel")
    {
      FULL_rdd_tbl <- pixel_preprocess(new_rdd_tbl)
    } else if (log_type == "receipt")
    {
      FULL_rdd_tbl <- receipt_preprocess(new_rdd_tbl)
    } else if (log_type == "reminder")
    {
      FULL_rdd_tbl <- reminder_preprocess(new_rdd_tbl)
    } else if (log_type == "signoff")
    {
      FULL_rdd_tbl <- signoff_preprocess(new_rdd_tbl)
    } else if (log_type == "signup")
    {
      FULL_rdd_tbl <- signup_preprocess(new_rdd_tbl)
    }
    
    # New path
    output_path <- paste0(s3a_path_output,log_date,"/",log_hour,"/")
    # Common Preprocessing
    if (!is.null(FULL_rdd_tbl))
    {
      spark_write_parquet(FULL_rdd_tbl, paste0(output_path,file_name),mode='overwrite')
    }
    
  }
}