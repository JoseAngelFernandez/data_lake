signoff_preprocess = function(new_rdd_tbl)
{
  # Common Preprocessing
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V1","V1new",pattern=":") %>% sdf_separate_column("V1new",c("Hour","Minute","Second_plus"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V7","V7new",pattern=":") %>% sdf_separate_column("V7new",c("V7new1","M"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V8","V8new",pattern=":") %>% sdf_separate_column("V8new",c("V8new1","N"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V9","V9new",pattern=":") %>% sdf_separate_column("V9new",c("V9new1","E"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V10","V10new",pattern=":") %>% sdf_separate_column("V10new",c("V10new1","C"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V11","V11new",pattern=":") %>% sdf_separate_column("V11new",c("V11new1","SD"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V12","V12new",pattern=":") %>% sdf_separate_column("V12new",c("V12new1","CSD"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V13","V13new",pattern=":") %>% sdf_separate_column("V13new",c("V13new1","Track_id"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V14","V14new",pattern=":") %>% sdf_separate_column("V14new",c("V14new1","Action"))
  
  #Select Relevant information
  FULL_rdd_tbl <- new_rdd_tbl %>% select(Hour,Minute,Type=V2,unsub_match=V5,M,N,E,C,SD,CSD,Track_id,Action)
  FULL_rdd_tbl
}