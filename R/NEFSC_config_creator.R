## ---------------------------
## Script name: NEFSC_Config_Creator.R
##
## Purpose of script: Creates an encrypted configuration file that can be used 
##    with NEFSC_db_Connector to securely connect to an NEFSC Oracle database
##    from R. 
##
## Date Created: 2023-01-11
##
## Software code created by U.S. Government employees is 
## not subject to copyright in the United States 
## (17 U.S.C. §105).
##
## Email: george.maynard@noaa.gov
##
## ---------------------------
## Notes:
##   
##
## ---------------------------
NEFSC_Config_Creator=function(config_name,host,port,service_name,username){
  ## Create a list
  x=list(
    "default"=list(
      "config"=list(
      )
    )
  )
  ## Store the inputs in the list
  x$default$config$host=host
  x$default$config$port=port
  x$default$config$service_name=service_name
  x$default$config$username=username
  x$default$config$password=.rs.askForPassword("Please Enter Your Oracle Password")
  ## Write the list out to a yaml file
  filename=paste0(config_name,".yml")
  yaml::write_yaml(
    x=x,
    file=filename
  )
  ## Check to see if the encryption keys exist. If not, prompt the user for a 
  ## master password to create them
  if("id_rsa.pub"%in%dir()*"id_rsa"%in%dir()==0){
    encryptr::genkeys()
  }
  encryptr::encrypt_file(filename)
  file.remove(filename)
}