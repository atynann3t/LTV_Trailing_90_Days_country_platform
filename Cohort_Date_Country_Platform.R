
require(bigrquery)

project <- '610510520745'
args <- commandArgs(trailingOnly = TRUE)
set_service_token(args[1])

require(binhf)
require(plyr)
require(dplyr)
require(scales)
require(flexdashboard)
require(drc)
require(nlme)
library(ggplot2)
library(forecast)
library(xlsx)
library(tibble)
library(formattable)
require(googlesheets)
library(broom)

# updating to c("joindate, country, platform")
# next combine this with the original country platform 
  # meaning: https://github.com/N3TWORK/analytics_tools/blob/master/marketing/Leg_R_Analysis/Leg_Partner_ROI_pred_calcs.R
  #to make:  c("joindate, country, platform, channel")


project <- '610510520745'

# date_range <- "SELECT DATEDIFF( current_date(),'2017-01-01')"
# # new way to get date_range 
# date_range <- "Select DATEDIFF(current_date(), DATE_ADD(CURRENT_DATE(), -97, 'DAY'))"
# date_range_query_execute <- query_exec(date_range, project = project)
# cohort_days <- date_range_query_execute[1,1]
cohort_days <- 97

# is there country level spend? 
# might not be able to use this 
  # if there is, would also need to add platform in the outer level 
# spend_sql <- 
# "SELECT
#       date,
#       ad_network,
#       IFNULL(SUM(spend),0) AS spend
#     FROM (SELECT
#               date,
#               campaign_name,
#               CASE  WHEN ad_network = 'apple_search' THEN 'ios search ads'
#                     WHEN ad_network = 'applifier' THEN 'unity ads'
#                     WHEN ad_network = 'supersonic' THEN 'ironsource'
#                     ELSE ad_network
#               END AS ad_network,
#               platform,
#               spend
#             FROM [n3twork-marketing-analytics:INSTALL_ATTRIBUTION.tenjin_summary]
#           WHERE date >= '2017-01-01'AND bundle_id = 'com.n3twork.legendary'
#         GROUP BY 1,2,3,4,5
#         ORDER BY 1
#     )
# GROUP BY 1,2
# ORDER BY 1"
# spend_query_execute <- query_exec(spend_sql, project = project)
# spend_data <- spend_query_execute

# channel_choose <- spend_data %>%
#   group_by(ad_network) %>%
#   summarize(channel_names = sort(unique(ad_network))) %>%
#   select(channel_names)
# channel_choose <- as.data.frame(channel_choose)

# top revenue countries (used for filtering)
top_rev_countries <-  c("United States", "Germany", #"United Kingdom",		 
                        "France", "Australia", "Canada", "Italy",		 
                        "Sweden",	"Switzerland", "Netherlands")
# list of platforms
platforms <- c('Android', 'iOS')

# country <- top_rev_countries[1]  #hard coded for testing
# platform <- platforms[1]


for(os in seq_along(platforms)) {
  platform <- platforms[os]
  for (nation in seq_along(top_rev_countries)) { 
    country <- top_rev_countries[nation]
    print(paste(country," ",platform,sep=''))  

  #### ua retention curve
  select_ua_retention_curve <- "SELECT
  joindate AS date,
  concat(string(country), string('_'), string(platform)) as source,
  EXACT_COUNT_DISTINCT(CASE
  WHEN playerage = 0 THEN gamerid
  ELSE NULL END) AS installs,"
  body_ua_retention_curve <- character()
  for(i in 1:cohort_days) {
    body_ua_retention_curve[i] <- paste("EXACT_COUNT_DISTINCT(CASE WHEN playerage = ",i," THEN gamerid ELSE NULL END) AS d",i,",",sep='')
  }
  body_ua_retention_curve <- paste(body_ua_retention_curve,collapse = " ")
  source("query_body.R")
  end_ua_retention_curve <- query_body # from source("query_body.R")
  sql <- paste(select_ua_retention_curve,body_ua_retention_curve,end_ua_retention_curve,sep = ' ')
  sql <- gsub("[\r\n]", " ", sql) 
  ###### executes sql
  data <- query_exec(sql, project = project)
  
  # data %>% head()
  # View(data)
  
  #### dau sql
  # dau_sql <-"SELECT date, EXACT_COUNT_DISTINCT(gamerid) AS DAU FROM [n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary] WHERE date >= '2017-01-01'GROUP BY 1 ORDER BY 1"
  dau_sql <- paste(
  "SELECT date
        ,EXACT_COUNT_DISTINCT(gamerid) AS DAU 
  FROM [n3twork-legendary-analytics:DAILY_GAMERIDS.gamerids_poole_summary] a 
  LEFT JOIN (
  SELECT
  STRING(UPPER(country_code)) AS country_code,
  country as country_name 
  FROM [n3twork-marketing-analytics:TEMP_ANALYSIS.country_code_lookup] 
  ) b
  ON a.country_code = b.country_code      
  WHERE DATEDIFF(current_date(), date) <= 97
  AND b.country_name = (","'",country,"'",")
  AND a.os_system = (","'",platform,"'",")
  GROUP BY 1 
  ORDER BY 1"
  ,sep='')
  
  dau_query_execute <- query_exec(dau_sql, project = project)
  dau_data <- dau_query_execute
  
  
  ## organic install data 
  source('org_install_query.R')  
  org_install_query <-  gsub("[\r\n]", " ", org_install_query) 
  org_install_query_execute <- query_exec(org_install_query, project = project)
  org_install_data <- org_install_query_execute
  
  
  ############################### organic revenue calculator
  select_org_rev <- "SELECT
  joindate AS date,
  SUM(CASE
  WHEN playerage <= 0 AND channel = 'organic' AND DATEDIFF(max_date,joindate) >= 0 THEN net_revenue
  ELSE NULL END) AS d0_ARPU,"
  body_org_rev <- character()
  for(i in 1:cohort_days) {
    body_org_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND channel = 'organic' AND DATEDIFF(max_date,joindate) >= ",i," THEN net_revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
  }
  body_org_rev <- paste(body_org_rev,collapse = " ")
  source('query_body_org_rev.R')
  end_org_rev <- query_body_org_rev  # get from source('quer_body.R')
  org_rev_sql <- paste(select_org_rev,body_org_rev,end_org_rev,sep = ' ')
  org_rev_query_execute <- query_exec(org_rev_sql, project = project)
  org_rev_data <- org_rev_query_execute
  
  
  #### query exectute for ua revenue
  select_ua_rev <- "SELECT
  joindate AS date,
  concat(string(country), string('_'), string(platform)) as source,
  SUM(CASE
  WHEN playerage <= 0 AND channel != 'organic' AND DATEDIFF(max_date,joindate) >= 0 THEN net_revenue
  ELSE NULL END) AS d0_ARPU,"
  body_ua_rev <- character()
  for(i in 1:cohort_days) {
    body_ua_rev[i] <- paste("SUM(CASE WHEN playerage <= ",i," AND channel != 'organic'  AND DATEDIFF(max_date,joindate) >= ",i," THEN net_revenue ELSE NULL END) AS d",i,"_ARPU,",sep='')
  }
  body_ua_rev <- paste(body_ua_rev,collapse = " ")
  end_ua_rev <- query_body  # get from source('quer_body.R')
  ua_rev_sql <- paste(select_ua_rev,body_ua_rev,end_ua_rev,sep = ' ')  
  ua_rev_sql <- gsub("[\r\n]", " ", ua_rev_sql) 
  ua_rev_query_execute <- query_exec(ua_rev_sql, project = project)
  ua_rev_data <- ua_rev_query_execute
  
  # View(ua_rev_data)
  
  # revenue_cleaner_header <- "SELECT date, campaign_source, CASE WHEN d0_ARPU IS NULL AND DATEDIFF(b.max_date,date) >= 0 THEN 0 ELSE d0_ARPU END AS d0_ARPU,"
  # 
  # revenue_cleaner_body <- character()
  # 
  # for(i in 1:(ncol(ua_rev_data[,3:ncol(ua_rev_data)])-1)) {
  #   revenue_cleaner_body[i] <- paste("CASE WHEN d",i,"_ARPU IS NULL AND DATEDIFF(b.max_date,date) >= ",i," THEN 0 ELSE d",i,"_ARPU END AS d",i,"_ARPU,",sep='')
  # }
  # 
  # revenue_cleaner_body <- paste(revenue_cleaner_body,collapse = " ")
  # 
  # revenue_cleaner_bridge <- "FROM ("
  # 
  # revenue_cleaner_a <- as.character(ua_rev_sql)
  # 
  # revenue_cleaner_b <- ") a CROSS JOIN (SELECT DATE(MAX(date)) AS max_date FROM [n3twork-legendary-analytics:DAILY_IAPS.iaps_summary]) b"
  # 
  # cleansed_ua_rev_sql <- paste(revenue_cleaner_header,revenue_cleaner_body,revenue_cleaner_bridge,revenue_cleaner_a,revenue_cleaner_b, sep = ' ') 
  # 
  # cleansed_ua_rev_query_execute <- query_exec(cleansed_ua_rev_sql, project = project)
  # cleansed_ua_rev_data <- cleansed_ua_rev_query_execute
  
  # data[c(1:5),c(1:5)]
  # View(missing_dates)
  
  ##################################################################
  ##################################################################
  ##################################################################
  #### this piece needs to be de-aggregated and raw form
  # then it needs to take an input to filter data on
  # group by date and create sums of all the retention values
  # then aggregate
  
  # for (channel_choice_for_this_run in 1:nrow(channel_choose)) {
  #   data_ua_filter <- data %>%
  #     filter(campaign_source == channel_choose[channel_choice_for_this_run,1])
  #   
  #   missing_dates <- data %>%
  #     group_by(date) %>%
  #     summarize(all_dates = sort(unique(date))) %>%
  #     select(date)
  #   
  #   data_ua_filter <- left_join(missing_dates, data_ua_filter, by = 'date')
  #   data_ua_filter[is.na(data_ua_filter)] <- 0
  #   data_ua_filter <- as.data.frame(data_ua_filter)
  #   
  #   ##### captures data only
  #   x <- data_ua_filter[,c(3:ncol(data_ua_filter))]
  
  x <- data[,c(3:ncol(data))]
  
  #### overwrites unneeded installs
  x[1:nrow(x),1] <- 0
  
  ##### pivots for retention curve
  mat <- as.matrix(x)
  for (i in 3:nrow(mat)) {
    mat[i,] <- shift(mat[i,],i-2)
  }
  
  ##### pivoted matrix
  matrix_size <- nrow(mat) - 1
  
  retention_curve <- mat[c(1:matrix_size),c(1:matrix_size)]
  
  ##### captures date range
  # dates <- c(data_ua_filter[c(1:(nrow(data_ua_filter))-1),1])
  dates <- c(data[c(1:(nrow(data))-1),1])
  
  #### reassigns retention curve to dates
  colnames(retention_curve) <- dates
  
  ##### finds retention curve sums
  retention_curve_sums <- colSums(retention_curve)
  
  ### assigns as a dataframe
  retention_curve_sums <- as.data.frame(retention_curve_sums)
  
  # retention_curve_sums %>%  dim()
  
  retention_curve_sums$ua_installs <- data[1:nrow(retention_curve_sums),3]
  
  
  ###### orgdau calculation
  orgdau <- org_install_data$installs/ dau_data$DAU
  orgdau <- orgdau[1:(length(orgdau)-1)]
  
  ###### merging into ua retention curve sums
  retention_curve_sums$orgdau <- orgdau
  retention_curve_sums$org_installs_from_ua <- round(retention_curve_sums$orgdau*retention_curve_sums$retention_curve_sums,digits = 0)
  
  ##### calculations for ua percentage of organic revenue
  retention_curve_sums$org_installs <- org_install_data[,2][1:(nrow(retention_curve_sums))]
  retention_curve_sums$ua_perc_for_rev <- retention_curve_sums$org_installs_from_ua / retention_curve_sums$org_installs
  
  
  ###### spend filter for the chosen channel
  
  # spend_filter <- spend_data %>%
  #   filter(ad_network == channel_choose[channel_choice_for_this_run,1])
  # 
  # 
  # spend_filter <- left_join(missing_dates, spend_filter, by = 'date')
  # spend_filter <- as.data.frame(spend_filter)
  # spend_filter[is.na(spend_filter)] <- 0
  # 
  # #### pairs down to match size and only cohorts that have a posted d7
  # retention_curve_sums <- retention_curve_sums[1:(nrow(retention_curve_sums)-6),]
  
  organic_revenue_date_match <- org_rev_data[1:nrow(retention_curve_sums),]
  
  # 
  # spend_data_date_match <- spend_filter[1:nrow(retention_curve_sums),]
  # retention_curve_sums$spend <- spend_data_date_match$spend
  # retention_curve_sums$aCPI <- retention_curve_sums$spend / (retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua)


  ##### calculates revenue from organic revenue from ua_contribution
  percent_ua_contribution <- retention_curve_sums$ua_perc_for_rev
  organic_revenue_ua_contribution <- organic_revenue_date_match[,2:ncol(organic_revenue_date_match)]

  #### for aCPI
  organic_revenue_ua_contribution <- organic_revenue_ua_contribution*percent_ua_contribution

  dates <- organic_revenue_date_match[,1]
  organic_revenue_ua_contribution <- cbind(dates,organic_revenue_ua_contribution)
  
  ####### UA revenue cleaner data and manipulation
  # 
  # ua_rev_data_filter <- cleansed_ua_rev_data %>%
  #   filter(campaign_source == channel_choose[channel_choice_for_this_run,1])
  # 
  # 
  # ua_rev_data_filter <- left_join(missing_dates, ua_rev_data_filter, by = 'date')
  # ua_rev_data_filter <- ua_rev_data_filter %>%
  #   arrange(desc(date))
  # 
  # ua_rev_data_filter <- as.matrix(ua_rev_data_filter)
  # 
  # for(i in 3:ncol(ua_rev_data_filter)) {
  #   for(j in 1:nrow(ua_rev_data_filter)) {
  #     if(lower.tri(ua_rev_data_filter[j,i],diag = T) && is.na(ua_rev_data_filter[j,i])) {
  #       ua_rev_data_filter[j,i] <- 0
  #     }
  #   }
  # }
  # 
  # ua_rev_data_filter[,3:ncol(ua_rev_data_filter)] <- as.numeric(as.character(ua_rev_data_filter[,3:ncol(ua_rev_data_filter)]))
  # 
  # ua_rev_data_filter[,3:ncol(ua_rev_data_filter)][upper.tri(ua_rev_data_filter[,3:ncol(ua_rev_data_filter)])] <- NA
  # 
  # ua_rev_data_filter <- as.data.frame(ua_rev_data_filter)
  # ua_rev_data_filter <- ua_rev_data_filter %>%
  #   arrange(date)
  
  
  #### matches date range for cohorts with 7 days baked in
  # ua_revenue_date_match <- ua_rev_data_filter[1:nrow(retention_curve_sums),]
  
  ua_revenue_date_match <- ua_rev_data[1:nrow(retention_curve_sums),]  
  
  # ua_revenue_date_match <- ua_revenue_date_match %>%
  #   arrange(desc(date))
  # 
  # ua_revenue_date_match <- as.matrix(ua_revenue_date_match, stringsAsFactors = F)
  # ua_revenue_date_match[,3:ncol(ua_revenue_date_match)][is.na(ua_revenue_date_match[,3:ncol(ua_revenue_date_match)])] <- 0
  # ua_revenue_date_match[,3:ncol(ua_revenue_date_match)] <- as.numeric(as.character(ua_revenue_date_match[,3:ncol(ua_revenue_date_match)]))
  # ua_revenue_date_match[,10:ncol(ua_revenue_date_match)][upper.tri(ua_revenue_date_match[,10:ncol(ua_revenue_date_match)])] <- NA
  # ua_revenue_date_match <- as.data.frame(ua_revenue_date_match)
  # ua_revenue_date_match <- ua_revenue_date_match %>%
  #   arrange(date)
  # ua_revenue_date_match <- as.data.frame(ua_revenue_date_match)
  
  #### blends ua revenue and organic revenue contribution
  # total_ua_revenue <- as.data.frame(lapply(organic_revenue_ua_contribution[,2:ncol(organic_revenue_ua_contribution)], function(x) as.numeric(as.character(x)))
  #                                   ) + as.data.frame(lapply(ua_revenue_date_match[,3:ncol(ua_revenue_date_match)], function(x) as.numeric(as.character(x))))
  
  total_ua_revenue <- organic_revenue_ua_contribution[,2:ncol(organic_revenue_ua_contribution)] + ua_revenue_date_match[,3:ncol(ua_revenue_date_match)] 
  
  total_ua_revenue <- cbind(dates,total_ua_revenue)
  total_ua_revenue <- as.data.frame(total_ua_revenue)
  
  #### retrieves total ua installs with k-factor for aCPI
  total_ua_installs <- retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua
  #### calculates arpi
  
  total_arpi <- as.matrix(as.data.frame(lapply(total_ua_revenue[,2:ncol(total_ua_revenue)], as.numeric))) / (total_ua_installs+.01)
  
  total_arpi_AJT <- total_arpi[,c(2:ncol(total_arpi))]
  total_arpi_AJT_means <- colMeans(total_arpi_AJT, na.rm = TRUE)
  total_arpi_AJT_means <- total_arpi_AJT_means[1:97] 

  # AJT 
  # a %>%  length()
  # b %>% length()
  a <- sort(total_arpi_AJT_means) 
  b <- c(0:(length(a)-1))
  prediction_set <- cbind(b,a)  
  prediction_set <- as.data.frame(prediction_set)
  prediction_set %>% head()
  
  # model 
  model_AJT <- 
    nls(a ~ SSgompertz(log(b+1), A, B, C) ,
        data = prediction_set,
        start = list(A=40000,B=11.8,C=.92),
        upper = list(A=40000,B=13,C=.93),
        lower = list(A=40000,B=11.0,C=.90),
        algorithm = "port",
        control = nls.control(maxiter = 500000,tol = 1e-09,warnOnly=T))
  
  # get each coef
  A <- tidy(model_AJT)[1,2]
  B <- tidy(model_AJT)[2,2]
  C <- tidy(model_AJT)[3,2]  
  
  # insert the coef into the formula 
  equation <- paste(as.character(A),"*exp(-",as.character(B),"*",as.character(C),"^log(x))",sep='')
  
  # rm(ltv_output)
  ltv_output <- cbind(as.character(country)
                      ,equation
                      ,as.character("No Channel") # channel 
                      ,as.character(platform))
  ltv_output <- cbind(as.data.frame(as.character(Sys.time())),ltv_output)
  colnames(ltv_output) <- c("created_timestamp", "county", "equation", "channel", "platform")
  
  ltv_output <- data.frame(lapply(ltv_output, as.character), stringsAsFactors=FALSE)
  
  insert_upload_job(project = '167707601509', 
                    dataset = "TEST_AGAMEMNON", 
                    table = "test_ltv_profiles",  
                    values = ltv_output, 
                    write_disposition = "WRITE_APPEND")   
  }
}  
  
  
  
#   
#   
#   ##### creates extended forecasting range
#   df <- data.frame(matrix(NA, ncol = 366 - ncol(total_arpi), nrow = nrow(total_arpi)))
#   
#   df_column_names <- character()
#   for(i in ncol(total_arpi):365) {
#     df_column_names[i] <- paste("d",i,"_ARPU",sep='')
#   }
#   df_column_names <- df_column_names[!is.na(df_column_names)]
#   colnames(df) <- df_column_names
#   
#   
#   total_arpi <- cbind(dates,total_arpi,df)
#   
#   
#   
#   ##### transposes cohorts to dataframe in order to support prediction formats
#   total_arpi_transposed <- t(total_arpi)
#   cohort_days <- c(0:365)
#   colnames(total_arpi_transposed) <- total_arpi_transposed[1, ]
#   total_arpi_transposed <- as.data.frame(total_arpi_transposed[-1,]) 
#   
#   
#   
#   
#   total_arpi_transposed <- cbind(cohort_days,total_arpi_transposed)
#   
#   #### models the data for each cohort
#   for (i in 2:ncol(total_arpi_transposed)) {
#     a <- as.numeric(levels(total_arpi_transposed[,i]))
#     a <- unique(a)
#     a <- sort(a)
#     b <- total_arpi_transposed[1:length(a),1]
#     prediction_set <- cbind(b,a)
#     prediction_set <- as.data.frame(prediction_set)
#     if (length(a) <=2) {
#       A <- 0
#       B <- 0
#       C <- 0
#       assign(paste('too_small',i,sep = ''), c('A' = A,'B' = B,'C' = C))
#     } else if (max(prediction_set$a) <= .4) {
#       assign(paste("fit",i,sep=''),
#              nls(a ~ SSgompertz(log(b+1), A, B, C) ,
#                  data = prediction_set,
#                  start = list(A=20000,B=14.0,C=.92),
#                  lower = list(A=20000,B=14.5,C=.94),
#                  upper = list(A=20000,B=13.5,C=.90),
#                  algorithm = "port",
#                  control = nls.control(maxiter = 500000,tol = 1e-09,warnOnly=T))
#       ) 
#     } else {
#       assign(paste("fit",i,sep=''),
#              nls(a ~ SSgompertz(log(b+1), A, B, C) ,
#                  data = prediction_set,
#                  start = list(A=45000,B=11.8,C=.92),
#                  lower = list(A=40000,B=11.0,C=.90),
#                  upper = list(A=50000,B=13,C=.93),
#                  algorithm = "port",
#                  control = nls.control(maxiter = 500000,tol = 1e-09,warnOnly=T))
#       )  
#     }
#   }
#   
#   
#   finished_matrix <- as.matrix(total_arpi_transposed,stringsAsFactors= F)
#   
#   
#   #### pulls model coefficients and stores each cohort to a dataframe
#   small.list <- mget(grep("too_small[0-9]+$", ls(),value=T))
#   flattened_zeros <- as.data.frame(unlist(small.list))
#   
#   if (length(flattened_zeros) > 0) {
#     colnames(flattened_zeros) <- c('coefs')
#     flattened_zeros$index <- row.names(flattened_zeros)
#     flattened_zeros$index <- as.numeric(gsub('[a-zA-Z._]','',flattened_zeros$index))
#     flattened_zeros$model_component <- gsub('[a-z0-9._]','',row.names(flattened_zeros))
#   } 
#   
#   
#   
#   model.list<-mget(grep("fit[0-9]+$", ls(),value=T))
#   coefs<-lapply(model.list,function(x)coef(x))
#   flattened_coefs <- as.data.frame(unlist(coefs))
#   if (length(flattened_coefs) > 0) {
#     colnames(flattened_coefs) <- c('coefs')
#     flattened_coefs$index <- row.names(flattened_coefs)
#     flattened_coefs$index <- as.numeric(gsub('[a-zA-Z.]','',flattened_coefs$index))
#     flattened_coefs$model_component <- gsub('[a-z0-9.]','',row.names(flattened_coefs))
#   }
#   
#   flattened_coefs <- rbind(flattened_coefs,flattened_zeros)
#   
#   ####### Fills in na's with predicted values
#   for (z in 2:ncol(finished_matrix)) {
#     A <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'A',1]
#     B <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'B',1]
#     C <- flattened_coefs[flattened_coefs$index == z & flattened_coefs$model_component == 'C',1]
#     for (i in 1:nrow(finished_matrix)){
#       if (is.na(finished_matrix[i,z])) {
#         finished_matrix[i,z] <- A*exp(-B*C^log(as.numeric(finished_matrix[i,1])))
#       }
#     }
#   }
#   
#   
#   #### writes out final output
#   transposed_predictions <- t(finished_matrix)
#   myData <- as.data.frame(transposed_predictions[-c(1), ], stringsAsFactors = F)
#   cohort_names <- row.names(myData)
#   myData <- sapply( myData, as.numeric )
#   myDataNet <- myData*.7
#   myData <- as.data.frame(myData)
#   myDataNet <- as.data.frame(myDataNet)
#   rownames(myData) <- cohort_names
#   rownames(myDataNet) <- cohort_names
#   
#   
#   
#   base_info <- cbind(retention_curve_sums$aCPI,
#                      retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua,
#                      myData$d7_ARPU / retention_curve_sums$aCPI,
#                      myData$d30_ARPU / retention_curve_sums$aCPI,
#                      myData$d90_ARPU / retention_curve_sums$aCPI)
#   colnames(base_info) <- c('aCPI','Installs','d7 ROI','d30 ROI','d90 ROI')
#   final_output <- cbind(base_info,myData)
#   
#   
#   final_output <-  final_output %>%
#     arrange(desc(rownames(final_output)))
#   rownames(final_output) <- rev(cohort_names)
#   days_to_backout <- NA 
#   for (i in 1:nrow(final_output)) {
#     if(!is.na(final_output[i,'aCPI'])) {
#       days_to_backout[i] <- which.min(final_output[i,'aCPI'] > final_output[i,6:ncol(final_output)])  
#     } else {
#       days_to_backout[i] <- 0
#     }
#   }
#   current_yield <- as.numeric(diag(as.matrix(final_output[,13:ncol(final_output)]))) / final_output$aCPI
#   final_output$current_yield <- current_yield
#   final_output$days_to_backout <- days_to_backout
#   
#   # final_output[,1] <- dollar_format()(final_output[,1])
#   # final_output[,3] <- percent((final_output[,3]))
#   # final_output[,4] <- percent((final_output[,4]))
#   # final_output[,5] <- percent((final_output[,5]))
#   # final_output$current_yield <- percent(final_output$current_yield)
#   
#   channel_name <- rep(channel_choose[channel_choice_for_this_run,1],nrow(final_output))
#   
#   #final_output_test <- final_output[,c("aCPI","Installs","days_to_backout","current_yield","d7 ROI","d30 ROI","d90 ROI")]
#   
#   assign(paste("all_data_",channel_choose[channel_choice_for_this_run,1],sep = ''), cbind(channel_name,final_output))
#   
#   # base_info_net <- cbind(retention_curve_sums$aCPI,
#   #                        retention_curve_sums$ua_installs + retention_curve_sums$org_installs_from_ua,
#   #                        myDataNet$d7_ARPU / retention_curve_sums$aCPI,
#   #                        myDataNet$d30_ARPU / retention_curve_sums$aCPI,
#   #                        myDataNet$d90_ARPU / retention_curve_sums$aCPI)
#   # colnames(base_info_net) <- c('aCPI','Installs','d7 ROI','d30 ROI','d90 ROI')
#   # final_output_net <- cbind(base_info_net,myDataNet)
#   # 
#   # final_output_net <-  final_output_net[nrow(final_output_net):1, ]
#   # days_to_backout_net <- NA 
#   # for (i in 1:nrow(final_output_net)) {
#   #   days_to_backout_net[i] <- which.min(final_output_net[i,'aCPI'] > final_output_net[i,6:ncol(final_output_net)])
#   # }
#   # current_yield_net <- as.numeric(diag(as.matrix(final_output_net[,13:ncol(final_output_net)]))) / final_output_net$aCPI
#   # final_output_net$current_yield <- current_yield_net
#   # final_output_net$days_to_backout <- days_to_backout_net
#   # final_output_net[,1] <- dollar_format()(final_output_net[,1])
#   # final_output_net[,3] <- percent((final_output_net[,3]))
#   # final_output_net[,4] <- percent((final_output_net[,4]))
#   # final_output_net[,5] <- percent((final_output_net[,5]))
#   # final_output_net$current_yield <- percent(final_output_net$current_yield)
#   rm(list = ls(pattern = 'fit'))
#   rm(list = ls(pattern = 'too_small'))
#   
#   
#   # write.xlsx(final_output, file = 'predictions_aCPI.xlsx', sheetName = channel_choose[channel_choice_for_this_run,1], append = T)
#   
#   print(paste('this channel is:', channel_choose[channel_choice_for_this_run,1], sep=''))
# }
# 
# partner_final_data <- mget(grep("all_data_",ls(),value=T))
# partner_final_data <- do.call(rbind.data.frame, partner_final_data)
# partner_final_data$date <- gsub('[a-z._]','',row.names(partner_final_data))
# 
# partner_final_data <- partner_final_data[,c('date','channel_name','aCPI','Installs','days_to_backout','current_yield','d7 ROI','d30 ROI','d90 ROI')]
# dput(names(partner_final_data))
# colnames(partner_final_data) <- c("date", "channel_name", "aCPI", "Installs", "days_to_backout", 
#                                   "current_yield", "d7_ROI", "d30_ROI", "d90_ROI")
# insert_upload_job(project = '167707601509', dataset = "LEG_Predictions", table = "by_partner_orgdau",  values = partner_final_data, write_disposition = "WRITE_TRUNCATE")
# 
# 
# 
