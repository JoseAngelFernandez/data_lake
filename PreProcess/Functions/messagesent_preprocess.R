messagesent_preprocess = function(new_rdd_tbl,log_date)
{
  # Common Preprocessing
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V1","V1new",pattern=":") %>% sdf_separate_column("V1new",c("Hour","Minute","Second_plus"))
  new_rdd_tbl <- new_rdd_tbl %>% filter( !is.na(V5) )
  new_rdd_tbl <- new_rdd_tbl %>% filter( !is.na(V6) )
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V5","V5new",pattern=":") %>% sdf_separate_column("V5new",c("V5new1","R"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V6","V6new",pattern=":") %>% sdf_separate_column("V6new",c("V6new1","E"))
  new_rdd_tbl <- new_rdd_tbl %>% filter(V3 == ">>")
  # new_rdd_tbl <- new_rdd_tbl %>% mutate(Hour = as.numeric(Hour_tmp))
  # new_rdd_tbl <- new_rdd_tbl %>% mutate(Minute = as.numeric(Minute_tmp))
  # new_rdd_tbl <- new_rdd_tbl %>% mutate(phone = as.character(V7))
  
  new_rdd_tbl <- new_rdd_tbl %>% mutate(phone = as.character(V7))
  new_rdd_tbl <- new_rdd_tbl %>% mutate(Pub = as.double(V8))
  # 
  # FULL_rdd_tbl <- new_rdd_tbl %>% select(Hour,Minute,Type=V2,R,E,phone,Pub, N=V9,C=V11,TransmissionID = V13, TextID = V15)
  FULL_rdd_tbl <- new_rdd_tbl %>% select(Hour,Minute,Type=V2,R,E,phone=V7,Pub=V8, N=V9,C=V11,TransmissionID = V13, TextID = V15)
  # FULL_rdd_tbl <- new_rdd_tbl %>% select(Hour,Minute,Type=V2,R,E,phone,Pub=V8, N=V9,C=V11,TransmissionID = V13, TextID = V15)
  
  FULL_rdd_tbl
}