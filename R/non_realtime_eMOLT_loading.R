## ---------------------------
## Script name: non_realtime_eMOLT_loading.R
##
## Purpose of script: This script reads in .dat files generated from downloading
##  loggers used in the non-realtime eMOLT program and pushes the data over to
##  the emoltdbs schema on the NEFSC servers where it can be served up via
##  ERDDAP. This script also generates a log for each loading session.
##
## Date Created: 2023-01-12
##
## Software code created by U.S. Government employees is 
## not subject to copyright in the United States 
## (17 U.S.C. §105).
##
## Email: george.maynard@noaa.gov
##
## ---------------------------
## Notes: You must be connected to the NEFSC network in order for this script to
##  run. The database connection relies on reading in an encrypted connection
##  configuration file. If you don't have this file or the script to generate
##  it, you can hard code the Oracle connection. 
## ---------------------------
## 
library(DBI)
library(lubridate)
library(measurements)
library(rjson)
library(ROracle)
## Set the environmental variables. These data have historically been handled in
##  local time. System time needs to be handled differently depending on the 
##  time change. 
setwd("C:/Users/george.maynard/Documents/GitHubRepos/eMOLT_non_realtime")
Sys.setenv(TZ = "America/New_York")
Sys.setenv(ORA_SDTZ = "America/New_York")
year_processed=2022
## Source the database connection script
source("R/NEFSC_db_Connector.R")
## Connect to the database
conn=NEFSC_db_Connector()
## For DST ONLY
dbGetQuery(conn=conn,statement="alter session set time_zone='-5:00'")
## Select the working directory on the NEFSC file share 
## (/EMOLT/data/emoltdbs/non_realtime/)
setwd("E:/data/emoltdbs/non_realtime/")
## Create a new log
logfile=paste0(
  "change_logs/",
  gsub(
    " ","",
       gsub(
         "-","",
            gsub(
              ":","",Sys.time()
                 )
            )
       ),"_non_realtime_loading.log"
  )
logging=list()
logging$HEADER=list(
  timestamp=Sys.time(),
  file_list=dir(path="/data/emoltdbs/non_realtime/to_do/")
)
logging$PROCESSING=list()
logging$ERRORS=list()
logging$SUCCESS=data.frame(
  FILENAME=character(),
  TIMESTAMP=character()
)
logging$FAILURE=data.frame(
  FILENAME=character(),
  TIMESTAMP=character(),
  REASON=character()
)
## Loop over the files, reading them in, checking their format, loading them, 
##  and moving them into the "loaded" folder.
pb=txtProgressBar(min=0,max=1,style=3)
for(i in 1:length(logging$HEADER$file_list)){
  ## Read in the file and log summary statistics
  file=logging$HEADER$file_list[i]
  data=read.csv(
    file=paste0("/data/emoltdbs/non_realtime/to_do/",file),
    header=FALSE
    )
  setTxtProgressBar(pb,(i-0.9)/length(logging$HEADER$file_list))
  ## Convert timestamp characters to POSIX
  data$V4=ymd_hms(data$V4,tz='America/New_York')
  ## Drop records that are impacted by the time change (should only be one, max)
  if(sum(is.na(data$V4))==1){
    data=subset(data,is.na(data$V4)==FALSE)
  }
  ## Record summary statistics
  logging$PROCESSING$file=list(
    rows=nrow(data),
    columns=ncol(data),
    site=data$V1[1],
    logger_serial=data$V2[1],
    start_date_local=min(data$V4,na.rm=TRUE),
    end_date_local=max(data$V4,na.rm=TRUE),
    min_temp_F=min(data$V6,na.rm=TRUE),
    max_temp_F=max(data$V6,na.rm=TRUE),
    depth_fathoms=data$V8[1]
  )
  setTxtProgressBar(pb,(i-0.8)/length(logging$HEADER$file_list))
  ## Reformat the dataframe to match the table structure
  emolt_sensor=data.frame(
    SITE=data$V1,
    SERIAL_NUM=data$V2,
    PROBE_SETTING=data$V3,
    TIME_LOCAL=data$V4,
    YRDAY0_LOCAL=data$V5,
    TEMP=data$V6,
    SALT=ifelse(data$V7==99.999,NA,data$V7),
    DEPTH_I=data$V8,
    U=NA,
    V=NA
  )
  setTxtProgressBar(pb,(i-0.7)/length(logging$HEADER$file_list))
  ## Check to see if the site already has data for this year
  previous=dbGetQuery(
    conn=conn,
    statement=paste0(
      "SELECT * FROM EMOLT_SENSOR WHERE YRDAY0_LOCAL BETWEEN ",
      yday(logging$PROCESSING$file$start_date_local),
      " AND ",
      yday(logging$PROCESSING$file$end_date_local),
      " AND SITE = '",
      emolt_sensor$SITE[1],
      "' AND DEPTH_I = ",
      emolt_sensor$DEPTH_I[1],
      " AND extract(YEAR from TIME_LOCAL) = ",
      year_processed
    )
  )
  setTxtProgressBar(pb,(i-0.6)/length(logging$HEADER$file_list))
  if(nrow(previous)!=0){
    failures=data.frame(
      filename=file,
      timestamp=Sys.time(),
      reason="FILE PREVIOUSLY LOADED"
    )
    logging$FAILURE=rbind(logging$FAILURE,failures)
    ## Copy the file over into the loaded directory
    file.copy(
      from=paste0("/data/emoltdbs/non_realtime/to_do/",file),
      to=paste0("/data/emoltdbs/non_realtime/loaded/duplicate_load_",file)
    )
    ## Remove the file from the to do directory
    file.remove(paste0("/data/emoltdbs/non_realtime/to_do/",file))
    setTxtProgressBar(pb,(i-0.5)/length(logging$HEADER$file_list))
  } else {
    ## Attempt to load the data into the "load" table
    loaddata=try(
      expr=dbWriteTable(
        conn=conn,
        name="load_emolt_sensor",
        value=emolt_sensor,
        append=TRUE
      ),
      silent=TRUE
    )
    ## If the load fails, note it in the log and move on
    if(loaddata!=TRUE){
      logging$ERRORS$file=loaddata[[1]]
      failures=data.frame(
        filename=file,
        timestamp=Sys.time(),
        reason="ERROR: SEE ERROR LOG FOR MORE DETAILS"
      )
      logging$FAILURE=rbind(logging$FAILURE,failures)
      setTxtProgressBar(pb,(i-0.4)/length(logging$HEADER$file_list))
    } else {
      ## Otherwise, load the data into the production table and clear the load table
      dbWriteTable(
        conn=conn,
        name="EMOLT_SENSOR",
        value=emolt_sensor,
        append=TRUE
      )
      ## Truncate the load table
      dbGetQuery(
        conn=conn,
        statement='truncate table "EMOLTDBS"."load_emolt_sensor" drop storage'
      )
      setTxtProgressBar(pb,(i-0.3)/length(logging$HEADER$file_list))
      # ## Create a converted version of the table with metric units
      # emolt_sensor_metric=data.frame(
      #   TIME_LOCAL=floor_date(emolt_sensor$TIME_LOCAL,unit="days"),
      #   SITE=emolt_sensor$SITE,
      #   DEPTH=conv_unit(
      #     x=emolt_sensor$DEPTH_I,
      #     from='fathom',
      #     to='m'
      #   ),
      #   TEMP=conv_unit(
      #     x=emolt_sensor$TEMP,
      #     from='F',
      #     to='C'
      #   ),
      #   TIME=emolt_sensor$TIME_LOCAL
      # )
      setTxtProgressBar(pb,(i-0.2)/length(logging$HEADER$file_list))
      ## Add a success record
      success=data.frame(
        filename=file,
        timestamp=Sys.time()
      )
      logging$SUCCESS=rbind(logging$SUCCESS,success)
      ## Copy the file over into the loaded directory
      file.copy(
        from=paste0("/data/emoltdbs/non_realtime/to_do/",file),
        to=paste0("/data/emoltdbs/non_realtime/loaded/",file)
      )
      ## Remove the file from the to do directory
      file.remove(paste0("/data/emoltdbs/non_realtime/to_do/",file))
      setTxtProgressBar(pb,(i-0.1)/length(logging$HEADER$file_list))
    }
  }
  ## Rename log to reflect filename
  names(logging$PROCESSING)=ifelse(
    names(logging$PROCESSING)=="file",
    file,
    names(logging$PROCESSING)
  )
  names(logging$ERRORS)=ifelse(
    names(logging$ERRORS)=="file",
    file,
    names(logging$ERRORS)
  )
  setTxtProgressBar(pb,(i-0.05)/length(logging$HEADER$file_list))
  ## Write out the log
  jsonlite::write_json(logging,logfile)
  setTxtProgressBar(pb,(i-0.01)/length(logging$HEADER$file_list))
  ## Clear everything that isn't the log or the database
  ##  connection
  rm(list=subset(
    ls(),
    ls()%in%c(
      'conn','logging','NEFSC_db_Connector','i','logfile','pb','year_processed'
      )==FALSE
    )
  )
  setTxtProgressBar(pb,i/length(logging$HEADER$file_list))
}