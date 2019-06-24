# PURPOSE: Scrape basketball reference players and get the bbref_id
`%>%` = dplyr::`%>%`

#' bbref_get_player_id_one_season
#' @name bbref_get_player_id_one_season
#' @title Get all NBA players on basket-reference name, bbref link and bbref id for one season
#' @description For a given season, get all the NBA players listed on basket-reference.com
#'               their names and bbref id. Use bbref_get_player_id as an easier function.
#' 
#' @note Use bbref_get_player_id function. This function is a helper function to that.
#' 
#' @param season NBA season (by end of year). Ex: 2018-2019 season is 2019
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, first_name, last_name, bbref_link, bbref_id, season
#'        
#' @examples 
#' # Scrape for 2018-2019 Season
#' bbref_get_player_id_one_season(2019)
bbref_get_player_id_one_season = function(season) {
  # Create url
  url = paste0('https://www.basketball-reference.com/leagues/NBA_', season, '_totals.html')
  # Read in page
  page = tryCatch({
    xml2::read_html(url)
  },
  error = function(e) {
    stop(paste0('Unable to find bbref page for season ', season))
    NULL
  }
  )
  # Get all the links from the table
  table = page %>% rvest::html_node("table#totals_stats") %>%
    rvest::html_nodes("a")
  # Get the list of all the names
  names = table %>% rvest::html_text()
  # Get all of the hrefs
  links = table %>% rvest::html_attr("href")
  # Only get the player hrefs and subset
  player_idx = stringr::str_detect(links, "players")
  names = names[player_idx]
  links = links[player_idx]
  player_unique = duplicated(links)
  names = names[!player_unique]
  links = links[!player_unique]
  # Put into a dataframe
  player_df = data.frame(
    player_name = names,
    bbref_link = links
  )
  # Seperate player name into first and last name
  player_df = player_df %>%
    tidyr::separate(player_name, into = c("first_name_punc", "last_name_punc"), 
                    sep = " ", remove = FALSE, extra = "merge")
  # Create lower case, unpunctuated names for joining
  # Also create column for last name suffix (jr, iii, etc.)
  player_df = player_df %>%
    dplyr::mutate(first_name_lower = tolower(stringr::str_remove_all(first_name_punc, "[^A-Za-z\\-]+")),
                  last_name_lower = tolower(stringr::str_remove_all(last_name_punc, "[^\\s\\-A-Za-z]"))) %>%
    # Extract the suffix
    dplyr::mutate(last_name_suffix = stringr::str_extract(last_name_lower, "\\s(jr|i|ii|iii|iv|v)*$"),
                  last_name_lower = stringr::str_remove(last_name_lower, "\\s(jr|i|ii|iii|iv|v)*$"))  
  # Clean the link and get the id
  player_df = player_df %>% 
    dplyr::mutate(bbref_link = stringr::str_remove(stringr::str_remove(bbref_link, ".html"), "/players/"))
  
  player_df = player_df %>%
    dplyr::mutate(bbref_id = stringr::str_remove(bbref_link, "[a-z]{1}\\/"))
  
  # Add season as a column
  player_df = player_df %>%
    dplyr::mutate(season = season)
  
  # Put columns in correct order
  player_df = player_df %>%
    dplyr::select(player_name, first_name_punc, last_name_punc, first_name_lower, last_name_lower, last_name_suffix,
                  bbref_link, bbref_id, season)
  
  return(player_df)
}

#' bbref_get_player_id
#' @name bbref_get_player_id
#' @title Get all NBA players on basket-reference name, bbref link and bbref id for range of seasons
#' @description For a given season(s), get all the NBA players listed on basket-reference.com
#'               their names and bbref id. First and last season tell the first and last year
#'               the player shows up in within the range of seasons given.
#' 
#' @param season NBA season or seasons (season by end of year). Ex: 2018-2019 season is 2019
#' @param parallel Boolean. If true run in parallel.
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, first_name, last_name, bbref_link, bbref_id, first_season, last_season
#'        
#' @examples 
#' # Scrape for 2018-2019 Season
#' bbref_get_player_id(2019)
#' 
#' # Scrape for 2005 to 2019 seasons
#' bbref_get_player_id(2005:2019)
bbref_get_player_id = function(seasons, parallel = TRUE) {
  `%dopar%` = foreach::`%dopar%`
  # Set up parallel running
  if (parallel) {
    cores = parallel::detectCores()
    cl = parallel::makeCluster(cores[1] - 1)
    doParallel::registerDoParallel(cl)
  }
  # Run over each of the seasons given
  player_df = foreach::foreach(season = seasons, .combine=rbind, .export = 'bbref_get_player_id_one_season') %dopar% {
    `%>%` = dplyr::`%>%`
    bbref_get_player_id_one_season(season)
  }
  # Stop parralel
  if (parallel) {
    parallel::stopCluster(cl)
  }
  # Get first and last year player is in list
  player_df = player_df %>%
    dplyr::group_by(player_name, first_name_punc, last_name_punc, first_name_lower, last_name_lower, last_name_suffix,
                    bbref_link, bbref_id) %>%
    dplyr::summarise(first_season = min(season), last_season = max(season)) %>% 
    dplyr::ungroup()
  # Fix types 
  player_df = suppressMessages(readr::type_convert(player_df))
  player_df = player_df %>% dplyr::mutate(player_name = as.character(player_name))
  

  return(player_df) 
}
