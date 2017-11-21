library(shiny)

# Define server logic required to draw a histogram
shinyServer(function(input, output) {
  
  message_sent_name_reactive_00 <- function(){paste0("message_sent_",gsub("/","",input$InputDate),"_",input$InputHour,"_00_.log")
  }
  message_sent_path_reactive_00 <- function(){
    paste0(s3a_path,input$InputDate,input$InputHour,"/",message_sent_name_reactive_00())
  }
  message_sent_name_reactive_15 <- function(){
    paste0("message_sent_",gsub("/","",input$InputDate),"_",input$InputHour,"_15_.log")
  }
  message_sent_path_reactive_15 <- function(){
    paste0(s3a_path,input$InputDate,input$InputHour,"/",message_sent_name_reactive_15())
  }
  message_sent_name_reactive_30 <- function(){
    paste0("message_sent_",gsub("/","",input$InputDate),"_",input$InputHour,"_30_.log")
  }
  message_sent_path_reactive_30 <- function(){
    paste0(s3a_path,input$InputDate,input$InputHour,"/",message_sent_name_reactive_30())
  }
  message_sent_name_reactive_45 <- function(){
    paste0("message_sent_",gsub("/","",input$InputDate),"_",input$InputHour,"_45_.log")
  }
  message_sent_path_reactive_45 <- function(){
    paste0(s3a_path,input$InputDate,input$InputHour,"/",message_sent_name_reactive_45())
  }
  
  path_builder <- function(){
    paste0("s3a://basebone.datalake/processed_data/2017.10.16/",input$InputHour,"/mess*")
  }
  
  retrieve_data_tbl <- function(){
    file_path_00 <- message_sent_path_reactive_00()
    file_path_15 <- message_sent_path_reactive_15()
    file_path_30 <- message_sent_path_reactive_30()
    file_path_45 <- message_sent_path_reactive_45()
    
    rdd_new <- spark_read_parquet(sc, name="rdd_new", path = path_builder() )
    rdd_new
  }
  
  output$testText <- renderText({ 
    path_builder()
    })
  
  output$distPlot <- renderPlot({
    message_count <- retrieve_data_tbl() %>% select(C) %>% group_by(C) %>% count(C) %>% collect()

    aes_Network1 <- aes(as.factor(message_count$C),message_count$n,fill=as.factor(message_count$C))
    ggplot() + geom_col(aes_Network1) + ggtitle("Message Sent per Country")
  })

  output$distPlot2 <- renderPlot({
    message_count <- retrieve_data_tbl() %>% select(E) %>% group_by(E) %>% count(E) %>% collect()

    aes_Network1 <- aes(as.factor(message_count$E),message_count$n,fill=as.factor(message_count$E))
    ggplot() + geom_col(aes_Network1) + ggtitle("Message Sent per Endpoint")
  })
  
})
