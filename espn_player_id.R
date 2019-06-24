# PURPOSE: Scrape espn players and get the espn player id
`%>%` = dplyr::`%>%`

#' espn_get_player_id_count
#' @name espn_get_player_id_count
#' @title Get all NBA players on espn name, espn link, link number and player id for one season and one page
#' @description EPSN stats splits its player over multiple pages that are split by the link's 
#'              count flag in the url. This function scrapes one page (about 40 players) for a given season.
#'              Function returns a data frame with the players' names and ids. An extra column for the season
#'              is added for future use.
#' 
#' @note This a helper function for espn_get_player_id_season.
#' 
#' @param season NBA season (by end of year). Ex: 2018-2019 season is 2019
#' @param count Count for espn stats page. First page starts at count = 1 and goes up by about 40.
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, first_name, last_name, espn_link, espn_number, espn_id, season.
#'        
#' @examples 
#' # Scrape for 2018-2019 Season first page
#' espn_get_player_id_count(2019, 1)
espn_get_player_id_count = function(season, count) {
  # Create url
  url = paste0('http://www.espn.com/nba/statistics/player/_/stat/scoring-per-game/sort/avgPoints/',
               'year/', season, '/seasontype/2/qualified/false/count/', count)
  # Read in page
  page = tryCatch({
    xml2::read_html(url)
  },
  error = function(e) {
    NULL
  }
  )
  
  # If there aren't any player on the page, return an empty data frame
  if (!is.null(page)) {
    table = tryCatch({
      page %>% rvest::html_node(xpath = '//*[@id="my-players-table"]/div/div[2]/table') %>%
        rvest::html_nodes("a")
    },
    error = function(e) {
      return(data.frame())
    }
    )    
  }
  else {
    return(data.frame())
  }
  # If no links were found
  if (length(table) == 0) {
    return(data.frame())
  }
  
  # Get the list of all the names
  names = table %>% rvest::html_text()
  # Get all of the hrefs
  links = table %>% rvest::html_attr("href")
  # Only get the player hrefs and subset
  player_idx = stringr::str_detect(links, "player/_/id")
  names = names[player_idx]
  links = links[player_idx]
  player_unique = duplicated(links)
  names = names[!player_unique]
  links = links[!player_unique]
  # Put into a dataframe
  player_df = data.frame(
    player_name = as.character(names),
    espn_link = links
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
    dplyr::mutate(espn_link = stringr::str_remove(espn_link, "http://www.espn.com/nba/player/_/id/"))
  
  player_df = player_df %>%
    dplyr::mutate(espn_number = stringr::str_extract(espn_link, "[0-9]+"),
                  espn_id = stringr::str_remove(espn_link, "[0-9]+\\/"))
  
  # Add season as a column
  player_df = player_df %>%
    dplyr::mutate(season = season)
  
  # Put columns in correct order
  player_df = player_df %>%
    dplyr::select(player_name, first_name_punc, last_name_punc, first_name_lower, last_name_lower, last_name_suffix,
                  espn_link, espn_number, espn_id, season)

  return(player_df)
}

#' espn_get_player_id_season
#' @name espn_get_player_id_season
#' @title Get all NBA players on espn name, espn link, link number and player id for one season
#' @description For an entire season, scrapes all players who played at least one minute over the course of the season.
#'              Function returns a data frame with the players' names and ids. An extra column for the season
#'              is added for future use.
#' 
#' @note This a helper function for espn_get_player_id
#' @note Assumes that there were at most 600 players that played in the league for that year. Theoretically, it could miss players.
#' 
#' @param season NBA season (by end of year). Ex: 2018-2019 season is 2019
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, first_name, last_name, espn_link, espn_number, espn_id, season.
#'        
#' @examples 
#' # Scrape for 2018-2019 Season
#' espn_get_player_id_season(2019)
espn_get_player_id_season = function(season) {
  `%dopar%` = foreach::`%dopar%`
  # Run over possible counts
  # It looks like ESPN outputs rows of about 40
  # There are usually around 530-550 players per season, use 600 to be safe
  player_df = foreach::foreach(count = seq(1, 600, by = 40), .combine=rbind) %dopar% {
    `%>%` = dplyr::`%>%`
    espn_get_player_id_count(season, count)
  }
  
  return(player_df)
}

#' espn_get_player_id
#' @name espn_get_player_id
#' @title Get all NBA players on espn name, espn link, link number and player id for range of seasons
#' @description For the given range of seasons, scrapes all players who played at least one minute over the course of the timeframe.
#'              Function returns a data frame with the players' names and ids. Additional columns for the first and last season
#'              that player showed up in the timeframe.
#' 
#' @note Assumes that there were at most 600 players that played in the league for any given year. Theoretically, it could miss players.
#' 
#' @param season NBA season or range of seasons (by end of year). Ex: 2018-2019 season is 2019
#' @param parallel Boolean. True is run in parralel.
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, first_name, last_name, espn_link, espn_number, espn_id, first_season, last_season
#'        
#' @examples 
#' # Scrape for 2018-2019 Season
#' espn_get_player_id(2019)
#' # Scrape for 2005 to 2019 Season
#' espn_get_player_id(2005:2019)
espn_get_player_id = function(seasons, parallel = TRUE) {
  `%dopar%` = foreach::`%dopar%`
  # Set up parallel running
  if (parallel) {
    cores = parallel::detectCores()
    cl = parallel::makeCluster(cores[1] - 1)
    doParallel::registerDoParallel(cl)
  }
  # Run over each of the seasons given
  player_df = foreach::foreach(season = seasons, .combine=rbind, 
                               .export = c('espn_get_player_id_count', 'espn_get_player_id_season')) %dopar% {
    `%>%` = dplyr::`%>%`
    espn_get_player_id_season(season)
  }
  
  # Stop clusters
  if (parallel) {
    parallel::stopCluster(cl)
  }
  
  # Get first and last year player is in list
  player_df = player_df %>%
    dplyr::group_by(player_name, first_name_punc, last_name_punc, first_name_lower, last_name_lower, last_name_suffix, 
                    espn_link, espn_number, espn_id) %>%
    dplyr::summarise(first_season = min(season), last_season = max(season)) %>% 
    dplyr::ungroup()
  
  player_df = suppressMessages(readr::type_convert(player_df))
  player_df = player_df %>% dplyr::mutate(player_name = as.character(player_name))
  
  return(player_df) 
}