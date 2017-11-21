reminder_preprocess = function(new_rdd_tbl)
{
  # Common Preprocessing
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V1","V1new",pattern=":") %>% sdf_separate_column("V1new",c("Hour","Minute","Second_plus"))
  
  # Bind both dataframe and arrange by date-hour
  FULL_rdd_tbl <- new_rdd_tbl %>% select(Hour,Minute,Type=V2,Reminder_for=V6,Company=V10,Product=V14)
  FULL_rdd_tbl
}