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
#'  player_name, bbref_link, bbref_id, season
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
  # Clean the link and get the id
  player_df = player_df %>% 
    dplyr::mutate(bbref_link = stringr::str_remove(stringr::str_remove(bbref_link, ".html"), "/players/"))
  
  player_df = player_df %>%
    dplyr::mutate(bbref_id = stringr::str_remove(bbref_link, "[a-z]{1}\\/"))
  
  # Add season as a column
  player_df = player_df %>%
    dplyr::mutate(season = season)
  
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
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, bbref_link, bbref_id, first_season, last_season
#'        
#' @examples 
#' # Scrape for 2018-2019 Season
#' bbref_get_player_id(2019)
#' 
#' # Scrape for 2004 to 2019 seasons
#' bbref_get_player_id(2004:2019)
bbref_get_player_id = function(seasons) {
  `%dopar%` = foreach::`%dopar%`
  # Run over each of the seasons given
  player_df = foreach::foreach(season = seasons, .combine=rbind) %dopar% {
    `%>%` = dplyr::`%>%`
    bbref_get_player_id_one_season(season)
  }
  # Get first and last year player is in list
  player_df = player_df %>%
    dplyr::group_by(player_name, bbref_link, bbref_id) %>%
    dplyr::summarise(first_season = min(season), last_season = max(season))

  return(player_df) 
}
