# PURPOSE: Scrape bbref college years for players
`%>%` = dplyr::`%>%`

#' bbref_get_college_years_one_player
#' @name bbref_get_college_years_one_player
#' @title For a given bbref link, get the years in college for that player.
#' 
#' @note Use bbref_get_college_years function. This function is a helper function to that.
#' 
#' @param bbref_link First initial / bbref_id for a player. Ex: Damian Lillard -> l/lillada01
#' @return Data frame with one row per player
#' Columns: 
#'  bbref_id, first_year_in_school, last_year_in_school, school id (takes last year in school)
#'        
#' @examples 
#' # Get years in school for Damian Lillard
#' bbref_get_college_years_one_player('l/lillada01')
bbref_get_college_years_one_player = function(bbref_link) {
  url = paste0('https://www.basketball-reference.com/players/', bbref_link, '.html')
  
  # Read in page
  page = tryCatch({
    xml2::read_html(url)
  },
  error = function(e) {
    stop(paste0('Unable to read bbref page for ', bbref_link))
    NULL
  }
  )
  
  # Read play by play table
  college_table = tryCatch({
    page %>% rvest::html_nodes(xpath = '//comment()') %>% # select comments
      rvest::html_text() %>% # extract the comments
      paste(collapse = '') %>% # collapse to single string
      xml2::read_html() %>% # Reread page
      rvest::html_node('table#all_college_stats') %>% # read in college table
      rvest::html_table()
  },
  error = function(e) {
    NULL
  })
  
  if (is.null(college_table)) {
    return(
      data.frame(
        bbref_id = stringr::str_split(bbref_link, '\\/', simplify = TRUE)[2],
        first_year_in_school = NA,
        last_year_in_school = NA,
        college1_name = NA,
        college1_id = NA,
        college2_name = NA,
        college2_id = NA
      )
    )
  }
  # Get the years in college
  colnames(college_table) = college_table[1,]
  college_table = college_table[-1,1:2]
  college_table = college_table[-nrow(college_table),]
  # Get first year and last year in college
  first_year = college_table[1,1]
  last_year = college_table[nrow(college_table),1]
  if (substring(first_year, 1, 2) == 19 & substring(first_year, 6, 7) == '00') {
    first_year = 2000
  }
  else {
    first_year = as.numeric(paste0(substring(first_year, 1, 2), substring(first_year, 6, 7)))    
  }
  if (substring(last_year, 1, 2) == 19 & substring(last_year, 6, 7) == '00') {
    last_year = 2000
  }
  else {
    last_year = as.numeric(paste0(substring(last_year, 1, 2), substring(last_year, 6, 7)))    
  }

  # Now get school information
  college_links =     
    page %>% rvest::html_nodes(xpath = '//comment()') %>% # select comments
      rvest::html_text() %>% # extract the comments
      paste(collapse = '') %>% # collapse to single string
      xml2::read_html() %>% # Reread page
      rvest::html_node('table#all_college_stats') %>% 
      rvest::html_nodes('a')
  
  college_name = unique(college_links %>% rvest::html_attr('title'))
  college_id = unique(college_links %>% rvest::html_attr('href')) %>%
    stringr::str_extract('[=]{1}[a-z]+') %>% stringr::str_remove('=')
  
  
  return(data.frame(
    bbref_id = stringr::str_split(bbref_link, '\\/', simplify = TRUE)[2],
    first_year_in_school = first_year,
    last_year_in_school =last_year,
    college1_name = college_name[1],
    college1_id = college_id[1],
    college2_name = college_name[2],
    college2_id = college_id[2]    
  ))  
}

#' bbref_get_college_years
#' @name bbref_get_college_years
#' @title For a list of players, get the years they were in college
#' 
#' 
#' @param bbref Either a list of bbref links (first initial / bbref_id) or a data frame with a column named "bbref_link"
#' @param parallel Boolean, TRUE if run in parallel.
#' @param join_to_df Boolean, if bbref is a data frame, join the results to the data frame by 'bbref_id'
#' @return Data frame with one row per player. If join will add the columns first_year_in_school, last_year_in_school
#'  to the given data frame. If join is false, a data frame with bbref_id, first_year_in_school, last_year_in_school
#'
bbref_get_college_years = function(bbref, parallel = TRUE, join_to_df = TRUE) {
  if (is.data.frame(bbref)) {
    if ('bbref_link' %in% colnames(bbref)) {
      bbref_links = bbref$bbref_link 
    }
    else {
      stop("Column name 'bbref_link' must be in bbref data frame.")
    }
  }
  else {
    bbref_links = bbref
  }
  `%dopar%` = foreach::`%dopar%`
  # Set up parallel running
  if (parallel) {
    cores = parallel::detectCores()
    cl = parallel::makeCluster(cores[1] - 1)
    doParallel::registerDoParallel(cl)
  }
  # Run over each player
  player_df = foreach::foreach(bbref_link = bbref_links, .combine=rbind, .export = 'bbref_get_college_years_one_player') %dopar% {
    `%>%` = dplyr::`%>%`
    bbref_get_college_years_one_player(bbref_link)
  }
  # Stop parralel
  if (parallel) {
    parallel::stopCluster(cl)
  }
  
  if (all(is.data.frame(bbref), join_to_df)) {
    bbref_join = tryCatch({
      dplyr::inner_join(bbref, player_df, by = 'bbref_id')
    },
    error = function(e) {
      warning("Could not join onto bbref data frame, returning just player data frame.")
      player_df
    })
  }
  else {
    bbref_join = player_df
  }

  return(bbref_join) 
}
