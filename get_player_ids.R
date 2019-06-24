# PURPOSE: Run final scraping functions in one script

# * Scripts ----
source('data_scraping/player_ids/bbref_player_id.R')
source('data_scraping/player_ids/espn_player_id.R')
source('data_scraping/player_ids/sports_ref_player_id.R')
source('data_scraping/bbref_tables/years_in_college.R')
# * SEASONS ----
# Remember seasons are coded as end year.
# 2004-2005 season is 2005.
start_season = 2005 # Earlist season to scrape
end_season = 2019 # Latest season to scrape

# * Bbref ----
bbref_players = bbref_get_player_id(start_season:end_season, parallel = TRUE)

# * ESPN ----
espn_players = espn_get_player_id(start_season:end_season, parallel = TRUE)

# * Sports Reference ----
sports_ref_players = sport_ref_player_id(last_name_letters = NULL, parallel = TRUE)

# * Join ----
join_bbref_espn = function(bbref_df, espn_df) {
  # Remove columns that are duplicate across data frames
  espn_df_v2 = espn_df %>% dplyr::select(-c(player_name, first_name_punc, last_name_punc, last_name_suffix))
  # Same names, been in league same years
  df_join = dplyr::inner_join(bbref_df, espn_df_v2, by = c('first_name_lower', 'last_name_lower', 'first_season', 'last_season'))
  # Find the resulting players
  leftovers_bbref = suppressMessages(dplyr::anti_join(bbref_df, df_join))
  leftovers_espn = suppressMessages(dplyr::anti_join(espn_df_v2, df_join))
  # Match now just by last name and years
  last_name_join = dplyr::inner_join(leftovers_bbref, leftovers_espn, by = c('last_name_lower', 'first_season', 'last_season')) %>%
    select(-first_name_lower.y) %>% rename(first_name_lower = first_name_lower.x)
  # Find the resulting players
  leftovers_bbref = suppressMessages(dplyr::anti_join(leftovers_bbref, last_name_join, by = c('last_name_lower', 'first_season', 'last_season'))) %>%
    mutate(player_name_lower = paste(first_name_lower, ' ', last_name_lower))
  leftovers_espn = suppressMessages(dplyr::anti_join(leftovers_espn, last_name_join,  by = c('last_name_lower', 'first_season', 'last_season'))) %>%
    mutate(player_name_lower = paste(first_name_lower, ' ', last_name_lower))
  # Fuzzy match by last name
  fuzzy = fuzzyjoin::stringdist_inner_join(leftovers_bbref, leftovers_espn, 
                                by = 'player_name_lower',
                                method = "jw", 
                                max_dist = 99, 
                                distance_col = "dist") %>%
    group_by(player_name_lower.x) %>%
    top_n(1, -dist)
  # Remove the .x in the column names
  fuzzy = fuzzy %>%
    dplyr::select(dplyr::contains('.x'), bbref_link, bbref_id, espn_link, espn_number, espn_id) %>%
    dplyr::rename_at(dplyr::vars(dplyr::contains('.x')), dplyr::funs(stringr::str_remove(., '\\.x'))) %>%
    dplyr::select(-dplyr::contains('.y'))
  # Bind the joins together
  df_final = dplyr::bind_rows(df_join, last_name_join, fuzzy) %>%
    dplyr::select(-player_name_lower)
  
  # Remove the Tony Mitchell experience
  df_final = df_final %>% dplyr::filter(!duplicated(bbref_id))
  
  if (nrow(df_final) != nrow(bbref_df)) {
    warning(paste0("Oh no! Final df did not result in the starting number of rows."))
  }
  
  return(df_final)
}

join_bbref_sports_ref = function(bbref_df, sports_df) {
  # Get years in school for basketball reference
  bbref_df = bbref_get_college_years(bbref_df, parallel = TRUE, join_to_df = TRUE)
  no_college_players = bbref_df %>% dplyr::filter(is.na(first_year_in_school))
  college_players = bbref_df %>% dplyr::filter(!is.na(first_year_in_school))
  # Remove 'University of' to match sports ref
  college_players = college_players %>% 
    mutate(college1_name = stringr::str_trim(stringr::str_remove(college1_name, 'University of|University')),
           college2_name = stringr::str_trim(stringr::str_remove(college2_name, 'University of|University')),
           college1_id = as.character(college1_id),
           college2_id = as.character(college2_id)
    )
  # Remove columns that are duplicate across data frames
  sports_v2 = sports_df %>% dplyr::select(-c(player_name, first_name_punc, last_name_punc, last_name_suffix)) %>%
    mutate(school_1_id = as.character(school_1_id))
  # Same names, same years in school
  df_join = dplyr::inner_join(college_players, sports_v2, by = c('first_name_lower', 'last_name_lower', 
                                                          'first_year_in_school' = 'begin_school', 
                                                          'last_year_in_school' = 'end_school',
                                                          'college1_name' = 'school_1_name'))
  # Find the resulting players
  leftovers_df1 = suppressMessages(dplyr::anti_join(college_players, df_join))
  # Same names, just last year in school only
  leftovers_df2 = suppressMessages(dplyr::anti_join(sports_v2, df_join, by = c('first_name_lower', 'last_name_lower', 
                                                                               'begin_school' = 'first_year_in_school', 
                                                                               'end_school' = 'last_year_in_school')))
  # Use the school id to get a little more people with the same college
  df_join_no_college = dplyr::inner_join(leftovers_df1, leftovers_df2, 
                                         by = c('first_name_lower', 'last_name_lower',
                                                'first_year_in_school' = 'begin_school',
                                                'last_year_in_school' = 'end_school',
                                                'college1_id' = 'school_1_id'))
  
  leftovers_df1 = suppressMessages(dplyr::anti_join(leftovers_df1, df_join_no_college))
  leftovers_df2 = suppressMessages(dplyr::anti_join(leftovers_df2, df_join_no_college, by = c('first_name_lower', 'last_name_lower', 
                                                                               'begin_school' = 'first_year_in_school', 
                                                                               'end_school' = 'last_year_in_school')))
  
  # Join by just first and last year in school
  df_join_years = dplyr::inner_join(leftovers_df1, leftovers_df2, 
                                         by = c('first_name_lower', 'last_name_lower',
                                                'first_year_in_school' = 'begin_school',
                                                'last_year_in_school' = 'end_school'))
  
  leftovers_df1 = suppressMessages(dplyr::anti_join(leftovers_df1, df_join_years))
  leftovers_df2 = suppressMessages(dplyr::anti_join(leftovers_df2, df_join_years, by = c('first_name_lower', 'last_name_lower', 
                                                                                   'begin_school' = 'first_year_in_school', 
                                                                                   'end_school' = 'last_year_in_school')))
  
  # Join by just only last year in school
  df_join_last_year = dplyr::inner_join(leftovers_df1, leftovers_df2, 
                                    by = c('first_name_lower', 'last_name_lower',
                                           'last_year_in_school' = 'end_school'))
  
  leftovers_df1 = suppressMessages(dplyr::anti_join(leftovers_df1, df_join_last_year))
  leftovers_df2 = suppressMessages(dplyr::anti_join(leftovers_df2, df_join_last_year, by = c('first_name_lower', 'last_name_lower', 
                                                                                         'begin_school' = 'first_year_in_school', 
                                                                                         'end_school' = 'last_year_in_school')))
  
  
  # # Just first letter of first name and last name
  # # And years in school
  # df_join_last_name = dplyr::inner_join(leftovers_df1 %>% dplyr::mutate(first_name_first = substring(first_name_lower, 1, 1)), 
  #                                       leftovers_df2 %>% dplyr::mutate(first_name_first = substring(first_name_lower, 1, 1)), 
  #                                       by = c('last_name_lower',
  #                                              'first_name_first',
  #                                              'first_year_in_school' = 'begin_school',
  #                                              'last_year_in_school' = 'end_school')) %>%
  #   dplyr::select(-first_name_first)
  # 
  # leftovers_df1 = suppressMessages(dplyr::anti_join(leftovers_df1, df_join_last_year))
  # leftovers_df2 = suppressMessages(dplyr::anti_join(leftovers_df2, df_join_last_year, by = c('first_name_lower', 'last_name_lower', 
  #                                                                                            'begin_school' = 'first_year_in_school', 
  #                                                                                            'end_school' = 'last_year_in_school')))
  # 
  # Name is the same but last year in school is different
  # Some weird transfers
  df_join_first_year = dplyr::inner_join(leftovers_df1, 
                                        leftovers_df2, 
                                        by = c('last_name_lower',
                                               'first_name_lower',
                                               'first_year_in_school' = 'begin_school'))
  
  leftovers_df1 = suppressMessages(dplyr::anti_join(leftovers_df1, df_join_first_year))
  leftovers_df2 = suppressMessages(dplyr::anti_join(leftovers_df2, df_join_first_year, by = c('first_name_lower', 'last_name_lower', 
                                                                                             'begin_school' = 'first_year_in_school', 
                                                                                             'end_school' = 'last_year_in_school')))
  
  
  
  
  # First letter of first name
  # And years in school
  df_join_last_name = dplyr::inner_join(leftovers_df1 %>% dplyr::mutate(first_name_first = substring(first_name_lower, 1, 1)), 
                                        leftovers_df2 %>% dplyr::mutate(first_name_first = substring(first_name_lower, 1, 1)), 
                                        by = c('last_name_lower',
                                               'first_name_first',
                                               'first_year_in_school' = 'begin_school',
                                               'last_year_in_school' = 'end_school')) %>%
    dplyr::select(-first_name_first)
  
  leftovers_df1 = suppressMessages(dplyr::anti_join(leftovers_df1, df_join_last_name))
  leftovers_df2 = suppressMessages(dplyr::anti_join(leftovers_df2, df_join_last_name, by = c('last_name_lower', 
                                                                                             'begin_school' = 'first_year_in_school', 
                                                                                             'end_school' = 'last_year_in_school')))
  
  # Just filter to the edge cases
  leftovers_df1_sub = leftovers_df1 %>%
    dplyr::filter(stringr::str_detect(tolower(player_name),
            'robert whaley|mbah a moute|jeter|devyn marble|sheldon mac'))
  leftovers_df2 = leftovers_df2 %>% 
    dplyr::filter(stringr::str_detect(paste0(first_name_lower, ' ', last_name_lower),
            'robert whaley|mbah a moute|pooh jeter|devyn marble|sheldon mcc'))
  
  # Then fuzzy match the names together
  fuzzy = fuzzyjoin::stringdist_inner_join(leftovers_df1_sub, leftovers_df2, 
                                           by = 'last_name_lower',
                                           method = "jw", 
                                           max_dist = 99, 
                                           distance_col = "dist") %>%
    group_by(last_name_lower.x) %>%
    top_n(1, -dist)
  # Remove the .x in the column names
  fuzzy = fuzzy %>%
    dplyr::select(-dplyr::contains('.y')) %>%
    dplyr::rename_at(dplyr::vars(dplyr::contains('.x')), list(~stringr::str_remove(., '\\.x'))) 
  
  # For the rest
  leftovers_df1 = suppressMessages(anti_join(leftovers_df1, leftovers_df1_sub))
  
  # Bind the df's together
  # Ones without sports-ref links
  no_sports = rbind(no_college_players, leftovers_df1) %>%
    dplyr::rename(school_1_name = college1_name, school_1_id = college1_id,
                  school_2_name = college2_name, school_2_id = college2_id) %>%
    dplyr::mutate(sport_ref_id = NA, school_3_name = NA, school_3_id = NA)
  
  sports_ref = dplyr::bind_rows(
    df_join,
    df_join_no_college,
    df_join_years,
    df_join_last_year,
    df_join_last_name,
    df_join_first_year,
    fuzzy
  ) %>% 
    select(-c(college1_name, college1_id, college2_name, college2_id, first_name_lower.x, first_name_lower.y, dist,
              first_year_in_school, last_year_in_school)) %>%
    rename(first_year_in_school = begin_school, last_year_in_school = end_school)
  
  # Bind together
  final_df = suppressWarnings(dplyr::bind_rows(no_sports, sports_ref))
  
  if (nrow(final_df) > nrow(bbref_df)) {
    warning("Final DF has more rows than we started with, check duplicates")
  }
  else if (nrow(final_df) < nrow(bbref_df)) {
    warning("Final DF has less rows than we started wtih, who did we lose?")
  }
  
  return(final_df)
}

join_data = function(bbref_df, espn_df, sports_df) {
  bbref_espn = join_bbref_espn(bbref_df, espn_df)
  bbref_sports = join_bbref_sports_ref(bbref_df, sports_df)
  # Join together
  final_df = dplyr::left_join(bbref_espn, bbref_sports,
                              by = c("player_name", 'first_name_punc', 'last_name_punc',
                                     'first_name_lower', 'last_name_lower', 'last_name_suffix',
                                     'bbref_link', 'bbref_id', 'first_season', 'last_season'))
  
  return(final_df)
}
