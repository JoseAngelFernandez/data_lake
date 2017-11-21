receipt_preprocess = function(new_rdd_tbl)
{
  # Common Preprocessing
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V1","V1new",pattern=":") %>% sdf_separate_column("V1new",c("Hour","Minute","Second_plus"))
  new_rdd_tbl <- new_rdd_tbl %>% filter(V3 != "TESTING")
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V19","V19new",pattern=":") %>% sdf_separate_column("V19new",c("V19new2","TID"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V20","V20new",pattern=":") %>% sdf_separate_column("V20new",c("V20new2","R"))

  # Bind both dataframe and arrange by date-hour
  FULL_rdd_tbl <- new_rdd_tbl %>% select(Hour,Minute,Type=V2,MSISDN=V3,N=V4,State=V6,Type_msg=V8,MT=V10,P=V12,E=V14,C=V18,TID,R)
  FULL_rdd_tbl
}