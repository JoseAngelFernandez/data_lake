library(shiny)

# Define UI for application that draws a histogram
shinyUI(fluidPage( #theme=shinytheme("flatly"),
  
  # Application title
  titlePanel("Log based stats - Exploratory Data Analysis Example"),
  
  # Sidebar with a slider input for number of bins 
  sidebarLayout(
    sidebarPanel(
      selectInput("InputDate",
                  "Date:",
                  Available_days),
      selectInput("InputHour",
                  "Hour:",
                  Hours,
                  selected = 0),
      # selectInput("InputMinute",
      #             "Minutes:",
      #             Minutes),
      submitButton("Apply")
      ),
      mainPanel(
        textOutput("testText"),
        plotOutput("distPlot"),
        plotOutput("distPlot2")
      )
    )
  )
  
)
