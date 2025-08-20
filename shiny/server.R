source('../R/ccog-content-media-report.R')

server <- function(input, output) {

  # Basic Example of shiny app using out library
  output$addNumbers <- renderText({

    x    <- input$x
    y    <- input$y

    add(x,y)

  })


}