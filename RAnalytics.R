# load packages
if (!require("RGA")) install.packages("RGA")
stopifnot(library(RGA, logical.return = TRUE))
if (!require("stringr")) install.packages("stringr")
stopifnot(library(stringr, logical.return = TRUE))
if (!require("plyr")) install.packages("plyr")
stopifnot(library(plyr, logical.return = TRUE))
# if (!require("data.table")) install.packages("data.table")
# stopifnot(library(data.table, logical.return = TRUE))
if (!require("RODBC")) install.packages("RODBC")
stopifnot(library(RODBC, logical.return = TRUE))

config <- data.frame(token_cache = c("O:/Lulu DWH/Scripts/R Scripts/Authorisation Tokens/ga.lulu.token", 
                                     "O:/Lulu DWH/Scripts/R Scripts/Authorisation Tokens/ga.pdfs.token"), 
                     merchant = c("LULU Software", 
                                  "Interactive Brands"), 
                     username = c("", 
                                  ""), 
                     client.id = c("", 
                                   ""), 
                     client.secret = c("", 
                                       ""), 
                     dimensions = c("ga:date, ga:experimentId, ga:experimentVariant, ga:campaign, ga:countryIsoCode"), 
                     connection = c("Driver={SQL Server}; Server=192.168.10.64; Database=Lulu_DWH;"), 
                     stringsAsFactors = FALSE)

# establish connection to SQL Server
conn <- odbcDriverConnect(config$connection[1])

# truncate Temp_AUX_GAnalytics
sqlClear(channel = conn, 
         sqtable = "dbo.Temp_AUX_GAnalytics")

# close connection to SQL Server 
odbcCloseAll()

first.date <- as.Date(Sys.Date() - 3)
last.date <- as.Date(Sys.Date())

# loop through merchants
for(i in 1:length(config$merchant)) {
  
  ga_total <- NULL
  
  print(paste0("Merchant", config$merchant[i]))
  
  # get access token
  ga_token <- authorize(client.id = config$client.id[i], 
                        client.secret = config$client.secret[i], 
                        cache = config$token_cache[i])
  
  # get a GA profiles
  ga_profiles <- list_profiles()
  
  # create a list of id - name pairs
  idNames <- subset(ga_profiles, select = c("id", "name"))
  
  # create a list of the GA profiles you need
  pdfList <- subset(idNames, grepl(".*", name))
  
  # create a list of "redundant" GA profies
  # " " adds all exclude params profiles
  # "architect" adds all PDF Architect profiles
  # "phd"; adds all Touche PHD profiles
  pdfExcludeList <- subset(pdfList,grepl(" ", name))
  
  # exclude the redundant GA profiles
  sodaList <- subset(pdfList, ! id %in% pdfExcludeList$id)
  
  # get the list of goals 
  ga_goals_all <- list_goals()
  
  # for each view, fetch the sessions and the required goals
  for(j in 1:nrow(sodaList)) {
    # get the profile ID by i
    profile.id <- sodaList[j, "id"]
    
    # get the account ID by profile ID
    account.id <- ga_profiles[ga_profiles$id == profile.id, "accountId"]
    
    # get the web property ID by profile ID
    webproperty.id <- ga_profiles[ga_profiles$id == profile.id, "webPropertyId"]
    
    # get the profile name by profile ID
    profile.name <- ga_profiles[ga_profiles$id == profile.id, "name"]
    
    print(paste0("Profile ", profile.name))
    
    # list of goals to fetch
    goal_names <- c("Downloads", "Installs", "Checkouts")
    
    # list goals in selected view
    ga_goals <- ga_goals_all[ga_goals_all$profileId == profile.id,]
    
    # loop through goals
    for(k in 1:length(goal_names)) {
      
      print(paste0("Goal ", goal_names[k]))
      
      # get the downloads goal ID
      metrics <- paste0("ga:goal", 
                              ga_goals[ga_goals$name == goal_names[k], "id"], 
                              "Completions")
      
      # configure dimensions
      dimensions <- "ga:date,ga:hostname,ga:landingPagePath,ga:country"
      
      # get GA report data only if required metrics exist in this GA profile
      if (metrics != "ga:goalCompletions") {
        
        # get GA report data
        ga_data <- get_ga(profile.id, 
                          start.date = first.date, 
                          end.date = last.date, 
                          metrics, 
                          dimensions, 
                          filters = paste0(metrics, ">0"),
                          start.index = NULL, 
                          max.results = NULL)
        
        if (!is.null(ga_data)) {
          # rename the metrics column
          names(ga_data)[names(ga_data) == sub("ga:(.*)", "\\1", metrics, ignore.case = TRUE)] <- tolower(goal_names[k])
          
          # add column with the profile name; added after the last column
          ga_data$name <- profile.name
          
          # join data frames
          ga_total <- rbind.fill(ga_total, ga_data)
        } 
      }
    }
    
    # get Sessions (aka Visits)
    ga_data <- get_ga(profile.id, 
                      start.date = first.date, 
                      end.date = last.date, 
                      metrics = "ga:sessions", 
                      dimensions = "ga:date,ga:hostname,ga:landingPagePath,ga:country", 
                      filters = "ga:sessions>0",
                      start.index = NULL, 
                      max.results = NULL)
    if (!is.null(ga_data)) {
      
      # add column with the profile name; added after the last column
      ga_data$name <- profile.name
      
      # join data frames
      ga_total <- rbind.fill(ga_total, ga_data)
    } 
  }
  
  if(!is.null(ga_total)) {
    # add column with lp
    # for earlier versions of RGA
    # ga_total$lp <- str_match(ga_total$landing.page.path , "[^\\?]*")
    # for recent versions of RGA
    ga_total$lp <- str_match(ga_total$landingPagePath , "[^\\?]*")
    ga_total$lp <- sub("default\\.aspx", "", ga_total$lp, ignore.case = TRUE)
    ga_total$lp_url <- paste0("http://", ga_total$hostname, ga_total$lp)
    
    # add column with uc_cmp
    # for earlier versions of RGA
    # ga_total$uc_cmp <- str_match(ga_total$landing.page.path , "[\\?&]cmp=([^&]*)")[, 2]
    # for recent versions of RGA
    ga_total$uc_cmp <- str_match(ga_total$landingPagePath , "[\\?&]cmp=([^&]*)")[, 2]
    
    # add column with uc_mkey1
    # for earlier versions of RGA
    # ga_total$uc_mkey1 <- str_match(ga_total$landing.page.path , "[\\?&]mkey1=([^&]*)")[, 2]
    # for recent versions of RGA
    ga_total$uc_mkey1 <- str_match(ga_total$landingPagePath , "[\\?&]mkey1=([^&]*)")[, 2]
    
    ga_total$date <- as.Date(ga_total$date)
    
    # export to CSV
    # fwrite(ga_total, 
    #        file = paste0("O:/Lulu DWH/External Data Integration/GA_", config$merchant[i], ".csv"),
    #        row.names = F)
    
    # normalize columns
    ga_total$date <- as.Date(ga_total$date)
    ga_total$hostname <- NULL
    ga_total$landingPagePath <- NULL
    ga_total$lp <- NULL
    names(ga_total)[names(ga_total) == "date"] <- "Date"
    names(ga_total)[names(ga_total) == "name"] <- "Profile_Name"
    names(ga_total)[names(ga_total) == "country"] <- "Country"
    names(ga_total)[names(ga_total) == "sessions"] <- "Sessions"
    names(ga_total)[names(ga_total) == "downloads"] <- "Downloads"
    names(ga_total)[names(ga_total) == "installs"] <- "Installs"
    ga_total$uc_style <- ""
    
    # establish connection to SQL Server
    conn <- odbcDriverConnect(config$connection[1])
    
    # Commit to Temp_AUX_GAnalytics
    sqlSave(channel = conn, 
            dat = ga_total, 
            tablename = "dbo.Temp_AUX_GAnalytics",
            varTypes = c(date = "datetime"), 
            append = TRUE,
            rownames = FALSE)
    
    # close connection to SQL Server
    odbcCloseAll()
  }
}


# establish connection to SQL Server
conn <- odbcDriverConnect(config$connection[1])

# delete old records from AUX_GAnalytics
sqlQuery(channel = conn, 
         query = "DELETE
         FROM   AUX_GAnalytics
         WHERE  [date] IN (SELECT DISTINCT tag.[Date]
         FROM   Temp_AUX_GAnalytics AS tag)")

# insert new records to AUX_GAnalytics
sqlQuery(channel = conn, 
         query = "INSERT INTO AUX_GAnalytics
         SELECT * FROM Temp_AUX_GAnalytics AS tag")

# close connection to SQL Server
odbcCloseAll()