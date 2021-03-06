library(dplyr)

#1. HERE Maps Function

### https://developer.here.com/  
###create a developer account to use 250K free transcation per month for api ##

# use an internal environment to cache DB information
HERE_rest_queries <- new.env(parent = emptyenv())

heremaps.restquery <- function(HEREtype, key = NULL) {
  # use cached information first
  if (HEREtype %in% names(HERE_rest_queries)) {
    result <- HERE_rest_queries[[HEREtype]]
  } else {
    if (is.null(key)) {
    ########  API KEY ###########
      key <- "Here Maps API KEY"
    }
    urlstring <-
      paste0("https://geocode.search.hereapi.com/v1/geocode?q=",
             HEREtype,
             "&apiKey=",
             key)
    connect <- curl::curl(urlstring)
    lines <- try(readLines(connect, warn = FALSE), silent = TRUE)
    close(connect)
    
    if (class(lines) == "try-error")
      stop("  HERE REST query failed for type: ", HEREtype)
    
    # convert to a list
    result <- rjson::fromJSON(paste(lines, collapse = ""))
    # cache the result
    HERE_rest_queries[[HEREtype]] <- result
  }
  
  # return only the relevant node
  result$items[[1]]
  
}

#2. HERE Maps Reverse location Function
# use an internal environment to cache DB information
HERE_rest_reversegeo_queries <- new.env(parent = emptyenv())

heremaps.rev_geo_restquery <- function(HEREtype, key = NULL) {
  # use cached information first
  if (HEREtype %in% names(HERE_rest_reversegeo_queries)) {
    result <- HERE_rest_reversegeo_queries[[HEREtype]]
  } else {
    if (is.null(key)) {
    ########  API KEY ###########
      key <- "Here Maps API KEY"
    }
    urlstring <-
      paste0(
        "https://geocode.search.hereapi.com/v1/revgeocode?at=",
        HEREtype,
        "&apiKey=",
        key
      )
    connect <- curl::curl(urlstring)
    lines <- try(readLines(connect, warn = FALSE), silent = TRUE)
    close(connect)
    
    if (class(lines) == "try-error")
      stop("  HERE REST query failed for type: ", HEREtype)
    
    # convert to a list
    result <- rjson::fromJSON(paste(lines, collapse = ""))
    # cache the result
    HERE_rest_reversegeo_queries[[HEREtype]] <- result
  }
  
  # return only the relevant node
  result$items[[1]]
  
}
#### Example ###
### Avoid spaces in function parameters or replace spaces with %20 for UTF-8 convertion 

r <- heremaps.restquery("Tirupati")
##round of to 3 digits after point
lat <- signif(as.numeric(r$position$lat), digits = 5)
lng <- signif(as.numeric(r$position$lng), digits = 5)
city <- r$address$city

l <- heremaps.rev_geo_restquery ("13.622523,79.417959")
city_l <- l$address$city
