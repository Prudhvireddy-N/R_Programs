# R Script: Data Processing and Location Updates

## Overview

This R script connects to a database and performs a series of operations related to location data. It retrieves, updates, and caches geographical information based on latitude, longitude, and city information. The script uses external APIs like **Here Maps** for geolocation, reverse geolocation, and caching data from the database.

## Features

- **Database Connections**: The script connects to a database (SQL Server) using ODBC to fetch and update data.
- **Data Processing**: The script processes and updates latitude, longitude, and city information.
- **Here Maps API Integration**: Utilizes the **Here Maps** API for geocoding and reverse geocoding (latitude/longitude to city and vice versa).
- **Caching**: Caches city information and geographical data to avoid redundant API calls.
- **Duplicate Records Handling**: Ensures that duplicate records are processed, updated, and saved correctly.

## Prerequisites

1. **R Version**: This script is compatible with R version 3.6 and higher.
2. **Required R Packages**: The following R packages are needed:

   - `RODBC`
   - `RJDBC`
   - `DBI`
   - `jsonlite`
   - `httr`
   - `dplyr`
   - `dbx`

   You can install them using the following command:

   ```R
   install.packages(c("RODBC", "RJDBC", "DBI", "jsonlite", "httr", "dplyr"))
   # dbx may need to be installed manually
   ```

3. **Here Maps API Key**: You must have a valid Here Maps API key. Replace "Here Maps Key" in the script with your actual API key.

4. **Database Connection**: The script connects to a SQL Server database. Ensure the odbc connection details (hostname, database name, user ID, password, port) are updated correctly.
