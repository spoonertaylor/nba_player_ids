# PURPOSE: Scrape sports-reference players and get the bbref_id
`%>%` = dplyr::`%>%`

#' sport_ref_player_id_letter
#' @name sport_ref_player_id_letter
#' @title Get all College Basketballs players on sports-reference name, id and school
#' @description For a given last name starting letter, get all the players listed on sports-reference.com/cbb
#'               their names and player id and schools. Use sport_ref_player_id as an easier function.
#' 
#' @note Use sport_ref_player_id function. This function is a helper function to that.
#' @note Caps the number of schools at 3. If you played at more than 3 schools, sorry.
#' 
#' @param letter Starting letter of last name
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, sport_ref_id, begin_school, end_school, school name and id for schools 1-3
#'        
#' @examples 
#' # Scrape for all players with the last name 'A'
#' sport_ref_player_id_letter('a')
sport_ref_player_id_letter = function(letter) {
  letter = tolower(letter)
  # Build url for last name starting with letter
  url = paste0('https://www.sports-reference.com/cbb/players/', letter, '-index.html')  
  # Read in page
  page = tryCatch({
    xml2::read_html(url)
  },
  error = function(e) {
    stop(paste0('Unable to find sport-reference page for last name ', letter))
    NULL
  }
  )
  
  # Get all the individual names
  all_p = page %>% rvest::html_nodes('p')
  # Get rid of any lines that doesn't have a player name
  all_p = suppressWarnings(all_p[stringr::str_detect(all_p, '/cbb/players/[_a-z]+')])
  # Get the text parts
  text = all_p %>% rvest::html_text()
  # Split on the multiple spaces
  split_text = stringr::str_split(text, "\\s\\s", n = 2, simplify = TRUE)
  # Player names
  names = split_text[,1]
  # Years in school
  years = stringr::str_extract(split_text[,2], "[0-9]{4}[-][0-9]{4}")
  # School names
  school = stringr::str_extract(split_text[,2], "([A-Za-z]+\\s*)+(; [A-Za-z]+\\s*)*")
  school_names = stringr::str_extract_all(school, "[A-Za-z]+(\\s{1}[A-Za-z]+)*", simplify = TRUE)
  school_names_df = as.data.frame(school_names[,1:ncol(school_names)], stringsAsFactors = FALSE)
  if (ncol(school_names_df) < 3) {
    school_names_df[, (ncol(school_names_df) + 1):3] = NA
  }
  colnames(school_names_df) = paste0("school_", 1:3, "_name")
  # Blanks to NA
  school_names_df = school_names_df %>% dplyr::mutate_all(list(~ifelse(nchar(.) == 0, NA, .)))
  
  # Get all of the links
  links = suppressWarnings(stringr::str_extract_all(all_p, '\\/cbb\\/[a-z/\\-0-9]+', simplify = TRUE))
  # Extract player ids
  player_id = stringr::str_remove(links[,1], "/cbb/players/")
  # Extract all of the school ids
  schools = t(apply(links[, -1], 1, function(x) stringr::str_remove(stringr::str_remove(x, "/cbb/schools/"), "/")))
  # Convert to DF
  school_df = as.data.frame(schools[,1:ncol(schools)], stringsAsFactors = FALSE)
  if (ncol(school_df) < 3) {
    school_df[, (ncol(school_df) + 1):3] = NA
  }
  school_df = school_df %>% dplyr::mutate_all(list(~ifelse(nchar(.) == 0, NA, .)))
  colnames(school_df) = paste0("school_", 1:3, "_id")
  # Create df to return
  sport_ref = data.frame(
    player_name = names,
    sport_ref_id = player_id,
    years_in_school = years
  ) %>% tidyr::separate(years_in_school, into = c("begin_school", "end_school"), sep = "-")
  
  player_df = cbind(sport_ref, school_names_df, school_df)
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
  
  player_df = player_df %>%
    dplyr::select(player_name, first_name_punc, last_name_punc, first_name_lower,
                  last_name_lower, last_name_suffix, sport_ref_id, dplyr::everything())
  
  return(player_df)
}


#' sport_ref_player_id
#' @name sport_ref_player_id
#' @title Get all players on sports-reference CBB name, player id, years in school, school(s)
#' @description For a given range of letter(s), get all the players listed on sports-reference.com/cbb
#'               their names and player id and school info. 
#' 
#' @param letters Range of letters to get the last name of. NULL is all players.
#' @param parallel Boolean. If true run in parallel.
#' @return Data frame with one row per player
#' Columns: 
#'  player_name, sport_ref_id, begin_school, end_school, school name and id for schools 1-3
#'        
#' @examples 
#' # Scrape for all players
#' sport_ref_player_id(NULL)
#' 
#' # Scrape for players with the last name Q
#' sport_ref_player_id('Q')
sport_ref_player_id = function(last_name_letters = NULL, parallel = TRUE) {
  `%dopar%` = foreach::`%dopar%`
  # If null, use all letters
  if (is.null(last_name_letters)) {
    last_name_letters = paste(letters)
  }
  else {
    last_name_letters = tolower(last_name_letters)
  }
  
  # Set up parallel running
  if (parallel) {
    cores = parallel::detectCores()
    cl = parallel::makeCluster(cores[1] - 1)
    doParallel::registerDoParallel(cl)
  }
  player_df = foreach::foreach(letter = last_name_letters, .combine=rbind, .export = 'sport_ref_player_id_letter') %dopar% {
    `%>%` = dplyr::`%>%`
    sport_ref_player_id_letter(letter)
  }
  # Stop clusters
  if (parallel) {
    parallel::stopCluster(cl)
  }
  
  player_df = suppressMessages(readr::type_convert(player_df))

  return(player_df) 
}
