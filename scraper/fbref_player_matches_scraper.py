import pandas as pd
from scraper.fbref_scraper import FbrefScraper
import time
import numpy as np

class FbrefPlayerMatchesScraper(FbrefScraper):
    """
    Scrapes all match-level data for specified matches. 

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping match-level statistics for all players in a single match

    Attributes
    ----------
    stats : list
        list of statistical categories to scrape data for

    Main Functionality Methods
    --------------------------
    scrape_save_new_player_match_stats(self, update_id):
        Scrape and save Football Reference player stats based on the latest matches data added
        in the current batch upload.
    
    scrape_player_match_stats(self, match_id, home_id, away_id):
        Given a Football Reference match id, home team id, and away team id, fetch all available
        player statistics for both teams.

    Notes
    -----
    Match-level player data is broken down into 7 distinct categories:

        1. summary - general match information and base-level statistics
        2. Passing* - statistics related to pass frequency, accuracy and distance
        3. Passing Types* - statistics related to various passing types
        4. Possession* - advanced statistics related to possession
        5. Defense* - advanced statistics related to defending
        6. Miscellaneous - statistics related to fouls, cards and penalties
        7. Keeper - statistics related to team goalkeeping

    * Only matches with advanced statistics have these categories available
    """
    # Initializaiton Methods
    def __init__(self):
        # Call parent class (FbrefScraper) initialization
        super().__init__(name='player_matches')
        self.stats = ['summary', 'passing', 'passing_types',
                      'defense', 'possession', 'misc', 'keeper']

    
    # Main Functionality Methods
    def scrape_save_new_player_match_stats(self, update_id):
        """
        Scrape and save Football Reference player stats based on the latest matches data added
        in the current batch upload.
        
        Parameters
        ----------
        update_id : str, optional
            ID corresponding to update date and run number.
        """
        try:
            # Scrape new player match stats
            new_player_matches_dict = self.scrape_new_player_match_stats(update_id)

            # Define file path and save to file
            file_path = f"data/fbref/player_matches/raw/player_matches_raw_{update_id}.json"
            self.save_raw_data(new_player_matches_dict, file_path)

            # Log successful scrape
            self.logger.info(f"Successfully scraped and saved player match stats data from {update_id}!")

        except Exception as e:
            # Log error
            self.logger.error(f"Could not scrape and save team stats data from {update_id} due to error: {e}")
    
    def scrape_player_match_stats(self, match_id, home_id, away_id):
        """
        Given a Football Reference match id, home team id, and away team id, fetch all
        available player statistics for both teams.

        Parameters
        ----------
        match_id : str
            Football Reference match id.
        home_id : str
            Football Reference team id for the home team.
        away_id : str
            Football Reference team id for the away team.

        Returns
        -------
        dict
            A dictionary containing all player statistics for the given match. The dictionary structure is:
            {
                home_id: {
                    'pids': list,
                    'stat_type_1': JSON_data,
                    'stat_type_2': JSON_data,
                    ...
                },
                away_id: {
                    'pids': list,
                    'stat_type_1': JSON_data,
                    'stat_type_2': JSON_data,
                    ...
                }
            }
            Where each stat_type_x is a different type of player statistic (e.g., summary, keeper, etc.).
        """
        # Initialize dictionary to store data
        player_match_stats_dict = {}
        player_match_stats_dict[home_id] = {}
        player_match_stats_dict[away_id] = {}

        # Set URL to scrape data
        url = f"https://fbref.com/en/matches/{match_id}/"

        # Get player ids
        match_ids = self.extract_all_pids(match_id, home_id, away_id)

        # Sleep to adhere to scraping policy
        time.sleep(6)

        # Scrape data from the URL
        dfs = self.scrape_data_html(url)

        # Determine which tabs to scrape based on the number of available tabs
        if len(dfs) >= 19:
            tab_names = self.stats
        else:
            tab_names = ['summary', 'keeper']

        # Iterate over home and away teams
        for ii, _id in enumerate([home_id, away_id]):
            # Store player ids for the team
            player_match_stats_dict[_id]['pids'] = match_ids[ii]
            
            # Calculate the offset for tab indexing
            adder = 0 + ii * 7
            
            # Iterate over different stat types
            for jj, tab in enumerate(tab_names):
                # Adjust dataframe index based on the number of tabs
                if len(tab_names) == 7:
                    df = dfs[3 + jj + adder].droplevel(level=0, axis=1)
                else:
                    df = dfs[-4 + (ii * 2) + jj].droplevel(level=0, axis=1)
                
                # Add suffixes to repeated headers to make them unique
                df = self.add_suffix_to_repeat_headers(df)
                
                # Convert dataframe to JSON format and store in dictionary
                player_match_stats_dict[_id][tab] = df.to_json()

        return player_match_stats_dict

    # Helper Methods
    def scrape_new_player_match_stats(self, update_id):
        """
        Scrape Football Reference player stats based on the latest matches data added in the current batch upload.

        All player team league seasons that end after old_match_reference_date will be scraped.

        Note that some player team league seasons may have a smaller subset of statistics available.

        Parameters
        ----------
        update_id : str, optional
            ID corresponding to the update date and run number.

        Returns
        -------
        dict
            A dictionary containing scraped player match statistics for each match in the latest data batch.

        """
        # Load matches data added in the current batch upload
        new_matches = pd.read_csv(f"data/fbref/matches/matches_{update_id}.csv", index_col=0)
        
        # Join with league-season table to get advanced stats info
        ls = self.data_dict['ls']
        matches_w_adv = pd.merge(new_matches, ls[['ls_id', 'has_adv_stats']], left_on='ls_id', right_on='ls_id', how='left')

        # Dictionary to store scraped data
        new_player_matches_dict = {}

        # Create dict to store data
        new_player_matches_dict = {}

        # Get number of ids for progress check and initialize scrape times list
        num_items = matches_w_adv.shape[0]
        scrape_times = []

        # Iterate through dataframe, scraping data for each match row
        for i, row in matches_w_adv.iterrows():
            try:
                # Being Timer
                start_time = time.time()
                # Scrape Data
                new_player_matches_dict[row.match_id] = self.scrape_player_match_stats(row.match_id, row.home_team_id, row.away_team_id)
                time.sleep(6)
                # End Timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully scraped player stats for match: {row.match_id}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            except Exception as e:
                self.logger.error(f"Could not scrape player stats for match: {row.match_id} due to error: {e}")
                time.sleep(6) # fbref scraping limit
        self.logger.info(f"Successfully scraped new team match stats for update: {update_id}. It took {np.sum(scrape_times):.2f} seconds to complete.")
        return new_player_matches_dict

    def extract_all_pids(self, match_id, home_id, away_id):
        """
        Extract all player ids for both home and away teams in a given match

        Parameters
        ----------
        match_id : str
            Football Reference match id
        home_id : str
            Football Reference team id for the home team
        away_id : str
            Football Reference team id for the away team

        Returns
        -------
        tuple
            Tuple containing a list of home team player ids and away team player ids
        """
        # Construct URL
        url = f"https://fbref.com/en/matches/{match_id}/"

        # Scrape data
        soup = self.scrape_data_requests(url)

        # Parse player ids
        home_table = soup.find('table', class_='stats_table sortable', id='stats_{}_summary'.format(home_id))
        away_table = soup.find('table', class_='stats_table sortable', id='stats_{}_summary'.format(away_id))
        home_ids = self.extract_ids(regex_pattern=r"players/(\w{8})/", table=home_table)
        away_ids = self.extract_ids(regex_pattern=r"players/(\w{8})/", table=away_table)

        return home_ids, away_ids
