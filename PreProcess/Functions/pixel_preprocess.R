pixel_preprocess = function(new_rdd_tbl)
{
  # Common Preprocessing
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V1","V1new",pattern=":") %>% sdf_separate_column("V1new",c("Hour","Minute","Second_plus")) 
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V25","V25new",pattern=":") %>% sdf_separate_column("V25new",c("Code_pre"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("Code_pre","Code_pre2",pattern=",") %>% sdf_separate_column("Code_pre2",c("Code"))
  
  #Select Relevant information
  FULL_rdd_tbl <- new_rdd_tbl %>% select(Hour,Minute,Identifier=V4,Pixel_id=V8,URL=V14,Response=V18,Status=V22,Code,Body=V28,
                                         affiliate=V32,Advertising_campaign=V36,Monthstamp=V40,MSISDN=V44,Session=V48,Campaign=V52,N=V56)
  FULL_rdd_tbl
}
