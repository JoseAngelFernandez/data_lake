library(dplyr)
library(sparklyr)
library(data.table)
library(aws.s3)
library(shinythemes)
library(ggplot2)

Sys.setenv(SPARK_HOME="./home/ubuntu/spark")
Sys.setenv(AWS_ACCESS_KEY_ID="AKIAIHGN643VGRUGCDOQ")
Sys.setenv(AWS_SECRET_ACCESS_KEY="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")
Sys.setenv(AWS_REGION="eu-west-1")

config <- spark_config()
config$sparklyr.defaultPackages[[1]] <- "org.apache.hadoop:hadoop-aws:2.7.3"
sc <- sparklyr::spark_connect(
  master = "local",
  sparkHome = "./home/ubuntu/spark",
  version = "2.2.0",
  appName = "spark_test",
  config = config
)

# Get file names from bucket
s3_path <- 's3://basebone.datalake/processed_data/'
s3a_path <- 's3a://basebone.datalake/processed_data/'
l_filename <- system(
  paste(
    "aws s3 ls",
    s3_path,
    "| awk {'print $2'}"
  ), 
  intern = TRUE
)
# For testing purposes
Available_days <- l_filename
Hours <- 0:23
Minutes <- c("00","15","30","45")