# Load necessary libraries
library(DBI)
library(odbc)
library(jsonlite)
library(httr)
library(dplyr)
library(dbx)
library(curl)

# Optionally load for Java-specific functions
options(java.parameters = "- Xmx1024m")
gc()

#################################################################################################################################################
# Date
#################################################################################################################################################
DateCreated <- format(Sys.time(), "%Y-%m-%d")

#################################################################################################################################################
# Functions
#################################################################################################################################################

# 1. Convert to tibble
convert_to_tibble <- function(df) {
  tibble::as_tibble(df)
}

# 2. Convert to character
convert_to_char <- function(fct) {
  as.character(fct)
}

# 3. mgsub - Multiple substitution in a string
mgsub <- function(pattern, replacement, x, ...) {
  if (length(pattern) != length(replacement)) {
    stop("Pattern and replacement do not have the same length.")
  }
  result <- x
  for (i in 1:length(pattern)) {
    result <- gsub(pattern[i], replacement[i], result, ...)
  }
  result
}

#################################################################################################################################################
# Database Connections
#################################################################################################################################################

# Create a connection to the database using odbc package
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver   = "ODBC Driver 17 for SQL Server",  # Updated to ODBC Driver 17
  Server   = "Hostname",
  Database = "DBName",
  UID      = "user ID",
  PWD      = "password",
  Port     = port
)

#################################################################################################################################################
# Declarations
#################################################################################################################################################

Table <- "SQL Query to get data"

dup_query <- "SQL Query to get data"

city_cache_query <- "SQL Query to get data"

Data_loc_query <- "SQL Query to get data"

Data_city_query <- "SQL Query to get data"

#################################################################################################################################################
# Lat and Long Update Cache
#################################################################################################################################################

# Get data and convert it to tibble
city_cache <- dbGetQuery(con, city_cache_query)
city_cache <- convert_to_tibble(city_cache)

#################################################################################################################################################
# HERE Maps Functions
#################################################################################################################################################
###proxy 
#Sys.setenv(https_proxy = "http://user:passowrd@server:8080")

# Use internal environment for caching API results
HERE_rest_queries <- new.env(parent = emptyenv())

# Function to query HERE Maps API
heremaps.restquery <- function(HEREtype, key = NULL) {
  if (HEREtype %in% names(HERE_rest_queries)) {
    result <- HERE_rest_queries[[HEREtype]]
  } else {
    if (is.null(key)) {
      #########API KEY #######
      key <- "Here Maps Key"
    }
    urlstring <- paste0("https://geocode.search.hereapi.com/v1/geocode?q=", HEREtype, "&apiKey=", key)
    connect <- curl::curl(urlstring)
    lines <- try(readLines(connect, warn = FALSE), silent = TRUE)
    close(connect)
    
    if (class(lines) == "try-error")
      stop("HERE REST query failed for type: ", HEREtype)
    
    result <- jsonlite::fromJSON(paste(lines, collapse = ""))
    HERE_rest_queries[[HEREtype]] <- result
  }
  result$items[[1]]
}

# Reverse geocode function
HERE_rest_reversegeo_queries <- new.env(parent = emptyenv())

heremaps.rev_geo_restquery <- function(HEREtype, key = NULL) {
  if (HEREtype %in% names(HERE_rest_reversegeo_queries)) {
    result <- HERE_rest_reversegeo_queries[[HEREtype]]
  } else {
    if (is.null(key)) {
      key <- "Here Maps Key"
    }
    urlstring <- paste0("https://geocode.search.hereapi.com/v1/revgeocode?at=", HEREtype, "&apiKey=", key)
    connect <- curl::curl(urlstring)
    lines <- try(readLines(connect, warn = FALSE), silent = TRUE)
    close(connect)
    
    if (class(lines) == "try-error")
      stop("HERE REST query failed for type: ", HEREtype)
    
    result <- jsonlite::fromJSON(paste(lines, collapse = ""))
    HERE_rest_reversegeo_queries[[HEREtype]] <- result
  }
  result$items[[1]]
}

# Cache function for city and location
city_cache_fn_queries <- city_cache
city_cache_fn <- function(HEREtype1, HEREtype2, key = NULL) {
  if (HEREtype1 %in% city_cache_fn_queries$latitude) {
    result <- subset(city_cache_fn_queries, latitude == HEREtype1 & longitude == HEREtype2)
    return(result$city[1])
  } else {
    return(NA)
  }
}

# Cache Location Function
city_cache_fn_loc <- function(HEREtype, key = NULL) {
  if (HEREtype %in% city_cache_fn_queries$Location) {
    result <- subset(city_cache_fn_queries, Location == HEREtype)
    return(paste0(result$latitude[1], ",", result$longitude[1], ",", result$city[1]))
  } else {
    return(NA)
  }
}

#################################################################################################################################################
# Lat_Long Update Cache
#################################################################################################################################################
Data_loc <- dbGetQuery(con, Data_loc_query)
Data_loc <- convert_to_tibble(Data_loc)
Data_loc <- select(Data_loc, -c("latitude", "longitude"))

df_1 <- list()
Location_u <- Data_loc$Location
for (i in seq_along(Location_u)) {
  r <- city_cache_fn_loc(Location_u[i])
  lat <- as.numeric(strsplit(r, ",")[[1]][1])
  lng <- as.numeric(strsplit(r, ",")[[1]][2])
  city <- strsplit(r, ",")[[1]][3]
  
  df_2 <- data.frame(
    latitude = signif(lat, digits = 5),
    longitude = signif(lng, digits = 5),
    Location = Data_loc$Location[i],
    DateCreated = Data_loc$DateCreated[i],
    city = city
  )
  df_1[[i]] <- df_2
}

df_3 <- do.call(rbind, df_1)
df_3 <- convert_to_tibble(df_3)
masterDf_loc_cache <- unique(df_3)

#################################################################################################################################################
# Lat_Long Update
#################################################################################################################################################
Data_loc <- subset(masterDf_loc_cache, is.na(masterDf_loc_cache$city))
Data_loc <- select(Data_loc, -c("latitude", "longitude"))

if (nrow(Data_loc) != 0) {
  Location <- gsub(" ", "", paste0(Data_loc$Location, ",India"))
  Location_u <- Location
  df_1 <- list()
  
  for (i in seq_along(Location_u)) {
    loc <- mgsub(c(":", "0:9", "-", "&"), c(",", "", "", ""), Location_u[i])
    location_maps <- try(heremaps.restquery(loc))
    
    if (inherits(location_maps, "try-error")) {
      city <- "India"
      lat <- 22.351
      lng <- 79.362
    } else {
      lat <- signif(as.numeric(location_maps$position$lat), digits = 5)
      lng <- signif(as.numeric(location_maps$position$lng), digits = 5)
      city <- location_maps$address$city
    }
    
    df_2 <- data.frame(
      latitude = lat,
      longitude = lng,
      Location = Data_loc$Location[i],
      DateCreated = Data_loc$DateCreated[i],
      city = city
    )
    df_1[[i]] <- df_2
  }
  
  df_3 <- do.call(rbind, df_1)
  df_3 <- convert_to_tibble(df_3)
  masterDf_loc <- unique(df_3)
} else {
  print("Location column is Empty")
  masterDf_loc <- NULL
}

masterDf_loc_cache <- subset(masterDf_loc_cache, !is.na(masterDf_loc_cache$city))
masterDf_loc <- rbind(masterDf_loc, masterDf_loc_cache)

# dbx Update
loc_update <- try(dbxUpdate(con, Table, masterDf_loc, where_cols = c("eventId", "Location", "DateCreated")))
paste(Sys.time(), loc_update)

#################################################################################################################################################
# City Update Cache
#################################################################################################################################################
Data_city <- dbGetQuery(con, Data_city_query)
Data_city <- convert_to_tibble(Data_city)
Data_city <- select(Data_city, -c("city"))

lat_long <- paste0(Data_city$latitude, ",", Data_city$longitude)
lat <- signif(Data_city$latitude, digits = 5)
lng <- signif(Data_city$longitude, digits = 5)

df_4 <- list()

for (i in seq_along(lat_long)) {
  city <- city_cache_fn(lat[i], lng[i])
  
  df_5 <- data.frame(
    Location = Data_city$Location[i],
    latitude = lat[i],
    longitude = lng[i],
    DateCreated = Data_city$DateCreated[i],
    city = city
  )
  df_4[[i]] <- df_5
}

df_6 <- do.call(rbind, df_4)
df_6 <- convert_to_tibble(df_6)
masterDf_city_cache <- unique(df_6)

#################################################################################################################################################
# City Update
#################################################################################################################################################
Data_city <- subset(masterDf_city_cache, is.na(masterDf_city_cache$city))
Data_city <- select(Data_city, -c("city"))

lat_long <- paste0(Data_city$latitude, ",", Data_city$longitude)
df_4 <- list()

for (i in seq_along(lat_long)) {
  loc <- try(heremaps.rev_geo_restquery(lat_long[i]))
  
  if (inherits(loc, "try-error") || is.null(loc$address$city)) {
    city <- "India"
  } else {
    city <- loc$address$city
  }
  
  df_5 <- data.frame(
    Location = Data_city$Location[i],
    latitude = Data_city$latitude[i],
    longitude = Data_city$longitude[i],
    DateCreated = Data_city$DateCreated[i],
    city = city
  )
  df_4[[i]] <- df_5
}

df_6 <- do.call(rbind, df_4)
df_6 <- convert_to_tibble(df_6)
masterDf_city <- unique(df_6)

masterDf_city_cache <- subset(masterDf_city_cache, !is.na(masterDf_city_cache$city))
masterDf_city <- rbind(masterDf_city, masterDf_city_cache)

#################################################################################################################################################
# Duplicate Records Update with City
#################################################################################################################################################
duplicate_rec <- dbGetQuery(con, dup_query)
duplicate_rec <- convert_to_tibble(duplicate_rec)
duplicate_rec_u <- distinct(duplicate_rec, Location, latitude, longitude, .keep_all = TRUE)

dup <- full_join(duplicate_rec_u, masterDf_city)
dup_null_lat <- dup[is.na(dup$latitude), ]
dup_notnull_city <- dup[!is.na(dup$city), ]
duplicate_rec_f <- full_join(dup_null_lat, dup_notnull_city)
duplicate_rec_f <- distinct(duplicate_rec_f, Location, latitude, longitude, .keep_all = TRUE)

# Backup of last updated Event 02 table
write.csv(duplicate_rec_f, file = "C:\\data.csv")

# Update duplicate records in the database
dbBegin(con)
city_update_dup <- try({
  dbWriteTable(con, Table, duplicate_rec_f, append = FALSE, overwrite = TRUE, row.names = FALSE)
})
if (!inherits(city_update_dup, "try-error")) {
  dbCommit(con)
} else {
  dbRollback(con)
}

paste(Sys.time(), city_update_dup)

#################################################################################################################################################
# Reset Environment
#################################################################################################################################################
rm(list = ls())

