# PURPOSE: Scrape NBA.com historical players to get player ids
`%>%` = dplyr::`%>%`

nba_dot_com_player_id = function() {
  url = 'https://stats.nba.com/players/list/?Historic=Y'
  # Read in page
  page = tryCatch({
    xml2::read_html(url)
  },
  error = function(e) {
    stop(paste0('Could not read NBA.com player page'))
    NULL
  }
  )
  
  ul = page %>% rvest::html_nodes('a')
  
  page2 = page %>% rvest::html_node('body') %>%
    rvest::html_nodes(xpath = '//comment()') %>%
    rvest::html_text() %>% # extract the comments
    paste(collapse = '') %>% # collapse to single string
    xml2::read_html() %>% # Reread page
    rvest::html_nodes('a')
  # Get all the links off the page
  links = page %>% rvest::html_text()
  # Only get the player hrefs and subset
  player_idx = suppressWarnings(stringr::str_detect(links, "\\/player\\/"))
  names = names[player_idx]
  links = links[player_idx]
  
  
  
}

pos_table = page %>% rvest::html_nodes(xpath = '//comment()') %>% # select comments
  rvest::html_text() %>% # extract the comments
  paste(collapse = '') %>% # collapse to single string
  xml2::read_html() %>% # Reread page
  rvest::html_node('table#pbp') %>% # read in play by play table
  rvest::html_table()


# start a server and open a navigator (firefox by default)
rsDriver(browser = "chrome")
driver <- remoteDriver()
driver$open()

# go to google
driver$navigate(url)

# get source code
page <- driver$getPageSource()

# convert to xml for easier parsing
page_xml <- read_html(page[[1]])