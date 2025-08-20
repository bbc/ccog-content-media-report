 # Define UI for app that draws a histogram ----
ui <- fluidPage(

  # App title ----
  titlePanel("ccog-content-media-report"),

  # Sidebar layout with input and output definitions ----
  sidebarLayout(

    # Sidebar panel for inputs ----
    sidebarPanel(

      # Input: Slider for the x ----
      sliderInput(inputId = "x",
                  label = "x:",
                  min = 1,
                  max = 50,
                  value = 30),

      # Input: Slider for y ----
      sliderInput(inputId = "y",
                  label = "y:",
                  min = 1,
                  max = 50,
                  value = 30)


    ),

    # Main panel for displaying outputs ----
    mainPanel(

      # Output: Histogram ----
      h2(textOutput(outputId = "addNumbers"))

    )
  )
)