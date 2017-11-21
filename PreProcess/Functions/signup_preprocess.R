signup_preprocess = function(new_rdd_tbl)
{
  # Common Preprocessing
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V1","V1new",pattern=":") %>% sdf_separate_column("V1new",c("Hour","Minute","Second_plus"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V3","V3new",pattern=":") %>% sdf_separate_column("V3new",c("V3new2","Success"))
  new_rdd_tbl <- new_rdd_tbl %>% ft_regex_tokenizer("V4","V4new",pattern=":") %>% sdf_separate_column("V4new",c("V4new2","CreateDate"))
  
  # Filtering WAP_MO / OTHER
  new_rdd_tbl_WAP_MO <- new_rdd_tbl %>% dplyr::filter(V8 == "WAP_MO")
  new_rdd_tbl_OTHER <- new_rdd_tbl %>% dplyr::filter(V8 != "WAP_MO")
  
  # WAP_MO_Process
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V11","V11new",pattern=":") %>% sdf_separate_column("V11new",c("V11new2","E"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V12","V12new",pattern=":") %>% sdf_separate_column("V12new",c("V12new2","TF"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V13","V13new",pattern=":") %>% sdf_separate_column("V13new",c("V13new2","N"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V14","V14new",pattern=":") %>% sdf_separate_column("V14new",c("V14new2","Mtn"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V15","V15new",pattern=":") %>% sdf_separate_column("V15new",c("V15new2","CT"))
  
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V16","V16new",pattern=":") %>% sdf_separate_column("V16new",c("V16new2","CO_pre"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("CO_pre","CO_pre_new",pattern="\\[") %>% sdf_separate_column("CO_pre_new",c("CO","CO_pre2"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("CO_pre2","CO_pre2_new",pattern="]") %>% sdf_separate_column("CO_pre2_new",c("CO_name"))
  
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V17","V17new",pattern=":") %>% sdf_separate_column("V17new",c("V17new2","PR_pre"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("PR_pre","PR_pre_new",pattern="\\[") %>% sdf_separate_column("PR_pre_new",c("PR","PR_pre2"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("PR_pre2","PR_pre2_new",pattern="]") %>% sdf_separate_column("PR_pre2_new",c("PR_name"))
  
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V18","V18new",pattern=":") %>% sdf_separate_column("V18new",c("V18new2","A"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V19","V19new",pattern=":") %>% sdf_separate_column("V19new",c("V19new2","P"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V20","V20new",pattern=":") %>% sdf_separate_column("V20new",c("V20new2","C"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V21","V21new",pattern=":") %>% sdf_separate_column("V21new",c("V21new2","BS"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V22","V22new",pattern=":") %>% sdf_separate_column("V22new",c("V22new2","S"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V23","V23new",pattern=":") %>% sdf_separate_column("V23new",c("V23new2","AF"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V24","V24new",pattern=":") %>% sdf_separate_column("V24new",c("V24new2","PU"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V25","V25new",pattern=":") %>% sdf_separate_column("V25new",c("V25new2","KW"))
  new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% ft_regex_tokenizer("V28","V28new",pattern=":") %>% sdf_separate_column("V28new",c("V28new2","App"))
  
  # OTHER_Process
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V10","V10new",pattern=":") %>% sdf_separate_column("V10new",c("V10new2","E"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V11","V11new",pattern=":") %>% sdf_separate_column("V11new",c("V11new2","TF"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V12","V12new",pattern=":") %>% sdf_separate_column("V12new",c("V12new2","N"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V13","V13new",pattern=":") %>% sdf_separate_column("V13new",c("V13new2","Mtn"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V14","V14new",pattern=":") %>% sdf_separate_column("V14new",c("V14new2","CT"))
  
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V15","V15new",pattern=":") %>% sdf_separate_column("V15new",c("V15new2","CO_pre"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("CO_pre","CO_pre_new",pattern="\\[") %>% sdf_separate_column("CO_pre_new",c("CO","CO_pre2"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("CO_pre2","CO_pre2_new",pattern="]") %>% sdf_separate_column("CO_pre2_new",c("CO_name"))
  
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V16","V16new",pattern=":") %>% sdf_separate_column("V16new",c("V16new2","PR_pre"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("PR_pre","PR_pre_new",pattern="\\[") %>% sdf_separate_column("PR_pre_new",c("PR","PR_pre2"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("PR_pre2","PR_pre2_new",pattern="]") %>% sdf_separate_column("PR_pre2_new",c("PR_name"))
  
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V17","V17new",pattern=":") %>% sdf_separate_column("V17new",c("V17new2","A"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V18","V18new",pattern=":") %>% sdf_separate_column("V18new",c("V18new2","P"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V19","V19new",pattern=":") %>% sdf_separate_column("V19new",c("V19new2","C"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V20","V20new",pattern=":") %>% sdf_separate_column("V20new",c("V20new2","BS"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V21","V21new",pattern=":") %>% sdf_separate_column("V21new",c("V21new2","S"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V22","V22new",pattern=":") %>% sdf_separate_column("V22new",c("V22new2","AF"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V23","V23new",pattern=":") %>% sdf_separate_column("V23new",c("V23new2","PU"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V24","V24new",pattern=":") %>% sdf_separate_column("V24new",c("V24new2","KW"))
  new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% ft_regex_tokenizer("V27","V27new",pattern=":") %>% sdf_separate_column("V27new",c("V27new2","App"))
  
  # Bind both dataframe and arrange by date-hour
  if (sum("V29" == tbl_vars(new_rdd_tbl_WAP_MO) ) > 0) 
  {
    new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% dplyr::select(Hour,Minute,Type=V2,Success,CreateDate,CreateTime=V5,Type_msg=V8,MSISDN=V10,
                                                               E,TF,N,Mtn,CT,CO,CO_name,PR,PR_name,A,P,C,BS,S,AF,PU,KW,App,App2=V29)
    new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% dplyr::select(Hour,Minute,Type=V2,Success,CreateDate,CreateTime=V5,Type_msg=V8,MSISDN=V9,
                                                             E,TF,N,Mtn,CT,CO,CO_name,PR,PR_name,A,P,C,BS,S,AF,PU,KW,App,App2=V28)
  } else{
    new_rdd_tbl_WAP_MO <- new_rdd_tbl_WAP_MO %>% dplyr::select(Hour,Minute,Type=V2,Success,CreateDate,CreateTime=V5,Type_msg=V8,MSISDN=V10,
                                                               E,TF,N,Mtn,CT,CO,CO_name,PR,PR_name,A,P,C,BS,S,AF,PU,KW,App,App2 = App)
    new_rdd_tbl_OTHER <- new_rdd_tbl_OTHER %>% dplyr::select(Hour,Minute,Type=V2,Success,CreateDate,CreateTime=V5,Type_msg=V8,MSISDN=V9,
                                                             E,TF,N,Mtn,CT,CO,CO_name,PR,PR_name,A,P,C,BS,S,AF,PU,KW,App,App2 = App)
  }
  FULL_rdd_tbl <- sdf_bind_rows(new_rdd_tbl_WAP_MO, new_rdd_tbl_OTHER) %>% dplyr::arrange(CreateTime)
  FULL_rdd_tbl
}