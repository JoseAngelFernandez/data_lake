write_in_s3 = function(path, logDate, file_name, FULL_rdd_tbl)
{
  file_name_parts <- unlist( strsplit(file_name,"_") )
  
  for (logHour in 0:23)
  {
    # FULL_rdd_tbl_tmp <- FULL_rdd_tbl %>% filter(Hour==logHour) 
    
    quarter <- c(15,30,45,60)
    FULL_rdd_tbl_tmp_15 <- FULL_rdd_tbl %>% filter(Hour==logHour, Minute < quarter[1]) 
    FULL_rdd_tbl_tmp_30 <- FULL_rdd_tbl %>% filter(Hour==logHour, Minute >= quarter[1] & Minute < quarter[2]) 
    FULL_rdd_tbl_tmp_45 <- FULL_rdd_tbl %>% filter(Hour==logHour, Minute >= quarter[2] & Minute < quarter[3]) 
    FULL_rdd_tbl_tmp_60 <- FULL_rdd_tbl %>% filter(Hour==logHour, Minute >= quarter[3]) 
    
    # New path
    output_path <- paste0(path,logDate,"/",logHour,"/")
    
    # Write 15minutes files
    if (!is.null(FULL_rdd_tbl_tmp_15))
    {
      spark_write_parquet(FULL_rdd_tbl_tmp_15, paste0(output_path,file_name_parts[1],"_",file_name_parts[2],"_",logHour,"_",quarter[1],"_",file_name_parts[3]),mode='overwrite')
    }
    if (!is.null(FULL_rdd_tbl_tmp_30))
    {
      spark_write_parquet(FULL_rdd_tbl_tmp_30, paste0(output_path,file_name_parts[1],"_",file_name_parts[2],"_",logHour,"_",quarter[2],"_",file_name_parts[3]),mode='overwrite')
    }
    if (!is.null(FULL_rdd_tbl_tmp_45))
    {
      spark_write_parquet(FULL_rdd_tbl_tmp_45, paste0(output_path,file_name_parts[1],"_",file_name_parts[2],"_",logHour,"_",quarter[3],"_",file_name_parts[3]),mode='overwrite')
    }
    if (!is.null(FULL_rdd_tbl_tmp_60))
    {
      spark_write_parquet(FULL_rdd_tbl_tmp_60, paste0(output_path,file_name_parts[1],"_",file_name_parts[2],"_",logHour,"_",quarter[4],"_",file_name_parts[3]),mode='overwrite')
    }
  }
}
