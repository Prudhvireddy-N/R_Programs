library(RODBC)
library(RJDBC)
library(DBI)
library(jsonlite)
require(jsonlite)
library(httr)
require(httr)
library(dplyr)
library(dbx)
#library(stringi)

options(java.parameters = "- Xmx1024m")
gc()

#################################################################################################################################################
# Date
#################################################################################################################################################

DateCreated <- format(Sys.time(), "%Y-%m-%d")

#################################################################################################################################################
# functions
#################################################################################################################################################


#1.  convert to tibble

convert_to_tibble <- function(df)
{
  tbf <- as.tbl(df)
  
  return(tbf)
}

#2.  convert to character

convert_to_char <- function(fct)
{
  char <- as.character(fct)
  
  return(char)
}

# 3 MGSUB
mgsub <- function(pattern, replacement, x, ...) {
  if (length(pattern) != length(replacement)) {
    stop("pattern and replacement do not have the same length.")
  }
  result <- x
  for (i in 1:length(pattern)) {
    result <- gsub(pattern[i], replacement[i], result, ...)
  }
  result
}
#################################################################################################################################################
# Connections
#################################################################################################################################################
con <- DBI::dbConnect(
  odbc::odbc(),
  Driver   = "ODBC Driver 13 for SQL Server",
  Server   = "Hostname",
  Database = "DBName",
  UID      = "user ID",
  PWD      = "password",
  Port     = port
)


#################################################################################################################################################
# Declarations
#################################################################################################################################################


Table =  "SQL Query to get data"


dup_query <- paste0("SQL Query to get data")

city_cache_query <-
  paste0("SQL Query to get data")


Data_loc_query <-
  paste0(
    "SQL Query to get data"
  )

Data_city_query <-
  paste0(
    "SQL Query to get data"
  )


#################################################################################################################################################
# Lat and Long
#################################################################################################################################################


city_cache = dbGetQuery(con, city_cache_query)
city_cache <- convert_to_tibble(city_cache)

#################################################################################################################################################
# function
#################################################################################################################################################

###proxy 
#Sys.setenv(https_proxy = "http://user:passowrd@server:8080")

#3. HERE Maps Function

# use an internal environment to cache DB information
HERE_rest_queries <- new.env(parent = emptyenv())

heremaps.restquery <- function(HEREtype, key = NULL) {
  # use cached information first
  if (HEREtype %in% names(HERE_rest_queries)) {
    result <- HERE_rest_queries[[HEREtype]]
  } else {
    if (is.null(key)) {
      key <- "Here "
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

#4. HERE Maps Reverse location Function
# use an internal environment to cache DB information
HERE_rest_reversegeo_queries <- new.env(parent = emptyenv())

heremaps.rev_geo_restquery <- function(HEREtype, key = NULL) {
  # use cached information first
  if (HEREtype %in% names(HERE_rest_reversegeo_queries)) {
    result <- HERE_rest_reversegeo_queries[[HEREtype]]
  } else {
    if (is.null(key)) {
      key <- "DzmSa0KO8IqhIrN-anOUDM6Jn7juu-9tmWDpCPBdyso"
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

#5 cache Function
# use an internal environment to cache DB data


city_cache_fn_queries <- city_cache
city_cache_fn <- function(HEREtype1, HEREtype2, key = NULL) {
  # use cached information first
  if (HEREtype1 %in% (city_cache_fn_queries$latitude)) {
    result <-
      subset(
        city_cache_fn_queries,
        city_cache_fn_queries$latitude == HEREtype1 &
          city_cache_fn_queries$longitude == HEREtype2
      )
    result_city <- result$city[1]
  } else {
    result_city <- NA
  }
}


#6 cache Location Function
city_cache_fn_loc <- function(HEREtype, key = NULL) {
  # use cached information first
  if (HEREtype %in% (city_cache_fn_queries$Location)) {
    result <-
      subset(city_cache_fn_queries,
             city_cache_fn_queries$Location == HEREtype)
    result_lat <- result$latitude[1]
    result_lng <- result$longitude[1]
    result_city <- result$city[1]
    paste0(result_lat, ",", result_lng, ",", result_city)
  } else {
    result_lat <- NULL
    result_lng <- NULL
    result_city <- NULL
    paste0(result_lat, ",", result_lng, ",", result_city)
  }
}


#################################################################################################################################################
# lat_long Update_cache
#################################################################################################################################################
paste0(Sys.time(), " lat_long Update_cache Started")
Data_loc = dbGetQuery(con, Data_loc_query)
Data_loc <- convert_to_tibble(Data_loc)
Data_loc <- select(Data_loc, -c("latitude"))
Data_loc <- select(Data_loc, -c("longitude"))
df_1 = list()
Location_u <- Data_loc$Location
print(nrow(Data_loc))
for (i in 1:length(Location_u))
{
  r <- city_cache_fn_loc(Location_u[i])
  lat <-  strsplit(r, ",")[[1]][1]
  lng <- strsplit(r, ",")[[1]][2]
  city <- strsplit(r, ",")[[1]][3]
  df_2 <-
    data.frame(
      latitude  = signif(as.numeric(lat), digits = 5),
      longitude  = signif(as.numeric(lng), digits = 5),
      Location = Data_loc$Location[i],
      DateCreated = Data_loc$DateCreated[i],
      city = city
    )
  df_1[[i]] <- df_2
}
df_3 = do.call(rbind, df_1)
df_3 <- convert_to_tibble(df_3)
masterDf_loc_cache <- unique(df_3)


#################################################################################################################################################
# lat_long Update
#################################################################################################################################################
paste0(Sys.time(), " lat_long Update Started")
Data_loc <-
  subset(masterDf_loc_cache, is.na(masterDf_loc_cache$city))
Data_loc <- select(Data_loc,-c("latitude"))
Data_loc <- select(Data_loc,-c("longitude"))
print(nrow(Data_loc))
if (nrow(Data_loc) != 0) {
  loadError_loc = F
} else {
  loadError_loc = T
}
if (loadError_loc == F) {
  Location <- paste0(Data_loc$Location, ",", "India")
  Location = gsub(" ", "", Location)
  Location_u <- Location
  Location_f <- Data_loc$Location
  Location_u = gsub(" ", "", Location_u)
  df_1 = list()
  #Loop to get find Lat and Long from given Location with Error Handling
  for (i in 1:length(Location_u))
  {
    loadError_maps = F
    v1 <- sample(Location_u[i], replace = T)
    r1 <-
      c(":",
        0:9,
        "-",
        "&")
    r2 <-
      c(",", "", "", "", "", "", "", "", "", "", "", "", "")
    loc <- mgsub(r1, r2, v1)
    location_maps = try({
      heremaps.restquery(loc)
    })
    loadError_maps <-
      (is(location_maps, 'try-error') | is(location_maps, 'error'))
    if (loadError_maps == F) {
      r <- heremaps.restquery(loc)
      lat <- signif(as.numeric(r$position$lat), digits = 5)
      lng <- signif(as.numeric(r$position$lng), digits = 5)
    } else {
      loc <- gsub('.* ', "", unlist(strsplit(Location_u[i], ':')))
      loadError_maps_1 = F
      location_maps_1 = try({
        heremaps.restquery(loc[2])
      })
      loadError_maps_1 <-
        (is(location_maps_1, 'try-error') |
           is(location_maps_1, 'error'))
      if (loadError_maps_1 == F) {
        r <- heremaps.restquery(loc[2])
        lat <- signif(as.numeric(r$position$lat), digits = 5)
        lng <- signif(as.numeric(r$position$lng), digits = 5)
      } else {
        lat <- 22.351
        lng <- 79.362
      }
    }
    df_2 <-
      data.frame(
        latitude  = lat,
        longitude  = lng,
        Location = Data_loc$Location[i],
        DateCreated = Data_loc$DateCreated[i],
        city = Data_loc$city[i]
      )
    df_1[[i]] <- df_2
  }
  df_3 = do.call(rbind, df_1)
  df_3 <- convert_to_tibble(df_3)
  masterDf_loc <- unique(df_3)
} else {
  print("Location column is Empty")
  masterDf_loc <- NULL
}
masterDf_loc_cache <-
  subset(masterDf_loc_cache, !is.na(masterDf_loc_cache$city))
masterDf_loc <- rbind(masterDf_loc, masterDf_loc_cache)

#dbx Update

loc_update = try({
  dbxUpdate(
    con,
    Table,
    masterDf_loc,
    where_cols = c("eventId", "Location", "DateCreated")
  )
})
paste(Sys.time(), loc_update)
paste0(Sys.time(), " lat_long Update Ended")

#################################################################################################################################################
# city Update_cache
#################################################################################################################################################
paste0(Sys.time(), " City Update_cache Started")
Data_city = dbGetQuery(con, Data_city_query)
Data_city <- convert_to_tibble(Data_city)
Data_city <- select(Data_city, -c("city"))
lat_long <- paste0(Data_city$latitude, ",", Data_city$longitude)
lat <- signif(Data_city$latitude, digits = 5)
lng <- signif(Data_city$longitude, digits = 5)
df_4 = list()
print(length(lat_long))
for (i in 1:length(lat_long))
{
  n1 <- lat[i]
  n2 <- lng[i]
  loadError_a = F
  a = try({
    city_cache_fn(n1, n2)
  })
  city <- a
  #print(city)
  df_5 <-
    data.frame(
      Location = Data_city$Location[i],
      latitude = signif(Data_city$latitude[i], digits = 5),
      longitude = signif(Data_city$longitude[i], digits = 5) ,
      DateCreated = Data_city$DateCreated[i],
      city = city
    )
  df_4[[i]] <- df_5
}
df_6 = do.call(rbind, df_4)
df_6 <- convert_to_tibble(df_6)
masterDf_city_cache <- unique(df_6)
masterDf_city_cache <- unique(masterDf_city_cache)
paste0(Sys.time(), " City Update_cache Ended")
#################################################################################################################################################
# City update
#################################################################################################################################################
paste0(Sys.time(), " City update Started")
Data_city <-
  subset(masterDf_city_cache, is.na(masterDf_city_cache$city))
Data_city <- select(Data_city,-c("city"))
lat_long <- paste0(Data_city$latitude, ",", Data_city$longitude)
lat <- Data_city$latitude
lng <- Data_city$longitude
df_4 = list()
if (lat_long != ",") {
  print(length(lat_long))
  for (i in 1:length(lat_long))
  {
    n <- lat_long[i]
    loadError = F
    a = try({
      heremaps.rev_geo_restquery(n)
    })
    loadError_a <- (is(a, 'try-error') | is(a, 'error'))
    b = try({
      exists(a$address$city)
    })
    loadError_b <- (is(b, 'try-error') | is(b, 'error'))
    if (loadError_a == F & loadError_b == F) {
      city <- a$address$city
    } else if (loadError_b == T & loadError_a == F) {
      city <- a$address$countryName
    } else{
      city <- "India"
    }
    #print(city)
    df_5 <-
      data.frame(
        Location = Data_city$Location[i],
        latitude = signif(Data_city$latitude[i], digits = 5),
        longitude = signif(Data_city$longitude[i], digits = 5) ,
        DateCreated = Data_city$DateCreated[i],
        city = city
      )
    df_4[[i]] <- df_5
  }
  df_6 = do.call(rbind, df_4)
  df_6 <- convert_to_tibble(df_6)
  masterDf_city <- unique(df_6)
  masterDf_city <- unique(masterDf_city)
} else {
  print("Latitude and Longitude are NULL")
  masterDf_city <- NULL
}

masterDf_city_cache <-
  subset(masterDf_city_cache, !is.na(masterDf_city_cache$city))
masterDf_city <- rbind(masterDf_city, masterDf_city_cache)
paste0(Sys.time(), " City update Ended")
#################################################################################################################################################
# Duplicate records update with CITY
#################################################################################################################################################

paste0(Sys.time(), " Duplicate records update Started")

duplicate_rec = dbGetQuery(con, dup_query)
duplicate_rec <- convert_to_tibble(duplicate_rec)
paste0(Sys.time(), " Duplicate records fetched")
duplicate_rec_u <-
  distinct(
    duplicate_rec,
    Location,
    latitude,
    longitude,
    .keep_all = TRUE
  )

dup <- full_join(duplicate_rec_u, masterDf_city)
dup_null_lat <- dup[is.na(dup$latitude),]
dup_notnull_city <- dup[!is.na(dup$city),]
duplicate_rec_f <- full_join(dup_null_lat, dup_notnull_city)
duplicate_rec_f <-
  distinct(
    duplicate_rec_f,
    Location,
    latitude,
    longitude,
    .keep_all = TRUE
  )

#backup of last updated Event 02 table
write.csv(duplicate_rec_f, file = "C:\\data.csv")

paste0(Sys.time(), " Duplicate db")
dbBegin(con)
city_update_dup = try({
  dbWriteTable(
    con,
    Table,
    duplicate_rec_f,
    append = F,
    overwrite = T,
    row.names = FALSE
  )
})
loadError_dup <-
  (is(city_update_dup, 'try-error') | is(city_update_dup, 'error'))
if (loadError_dup == F) {
  dbCommit(con)
} else {
  print("Rollback")
  dbRollback(con)
}
paste0(Sys.time(), city_update_dup)
paste0(Sys.time(), "Duplicate records Done")

#################################################################################################################################################
#Reset Environment
#################################################################################################################################################
#rm(list=ls())
