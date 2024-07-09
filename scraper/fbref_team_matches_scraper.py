import pandas as pd
import numpy as np
import requests
import re
import json
from bs4 import BeautifulSoup
import time
from scraper.fbref_scraper import FbrefScraper

class FbrefTeamMatchesScraper(FbrefScraper):
    """
    Scrapes all match-level data for specified teams. 

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping match stats for teams. 

    Attributes
    ----------
    stats : list
        list of statistical categories to scrape data for

    Main Functionality Methods
    --------------------------
    scrape_save_new_team_match_stats(self, month_year, old_match_reference_date):
        Scrape and save new team stats
    scrape_team_match_stats(self, tid, sl, lg_id, stat):
        Scrape fbref team stats for a given team-league-season.

    Notes
    -----
    Match-level team data is broken down into 9 distinct categories:

        1. Schedule - general match information and base-level statistics
        2. Shooting -  statistics related to shot frequency, accuracy and distance
        3. Passing* - statistics related to pass frequency, accuracy and distance
        4. Passing Types* - statistics related to various passing types
        5. GCA - advanced statistics related to goal-creating actions
        6. Possession* - advanced statistics related to possession
        7. Defense* - advanced statistics related to defending
        8. Miscellaneous - statistics related to fouls, cards and penalties
        9. Keeper - statistics related to team goalkeeping

    * Only matches with advanced statistics have these categories available
    """
    # Initialization Methods
    def __init__(self, is_new_ls):
        # Call parent class (FbrefScraper) initialization
        super().__init__(name='team_matches')
        self.stats = ['schedule', 'shooting', 'passing', 'passing_types',
                      'gca', 'possession', 'defense', 'misc', 'keeper']
        self.is_new_ls = is_new_ls

    # Main Class Functionality Methods
    def scrape_save_new_team_match_stats(self, update_id, old_match_reference_date):
        """
        Scrape and save new team stats
        
        Parameters
        ----------
        update_id : str, optional
            ID corresponding to update date and run number.
        old_match_reference_date : str
            Date in `%Y-%m-%d` format. Fetch matches for league seasons with end dates after this date.
        """
        try:
            # Scrape new team match stats 
            new_team_matches_dict = self.scrape_new_team_match_stats(update_id, old_match_reference_date)
            
            # Define file path for saving the scraped data
            file_path = f"data/fbref/team_matches/raw/team_matches_raw_{update_id}.json"
            
            # Save the scraped data
            self.save_raw_data(new_team_matches_dict, file_path)

            # Log successful scrape
            self.logger.info(f"Successfully scraped and saved team stats data from {update_id}!")

        except Exception as e:
            # Handle any exceptions that occur during the process
            self.logger.error(f"Could not scrape and save team stats data from {update_id} due to error: {e}")
        
    def scrape_team_match_stats(self, tid, sl, lg_id, stat):
        """
        Scrape fbref team stats for a given team-league-season.

        Parameters
        ----------
        tid : str
            fbref team ID.
        sl : str
            fbref season ID (long format).
        lg_id : int
            fbref league ID.
        stat : str
            Stat category to scrape. Must be one of the values in self.stats.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the stats for the given team-league-season and stat category.
        """
        # Check if the provided stat is valid
        if stat not in self.stats:
            raise ValueError("Invalid stat. Must be in self.stats list")
        
        # Set tls_id
        tls_id = str(tid) + "_" + str(lg_id) + "_" + str(sl)[:4]
        
        # Construct the URL for scraping
        url = f"https://fbref.com/en/squads/{tid}/{sl}/matchlogs/c{lg_id}/{stat}/"
        if stat == 'schedule':
            try:
                # Scrape the schedule data
                stat_df = self.scrape_data_html(url, index=0)  # Note: this is different for the schedule
                stat_df = self.add_suffix_to_repeat_headers(stat_df)  # Get unique column names
                stat_df = stat_df.dropna(subset=['Date']).reset_index(drop=True)
                stat_df['tls_id'] = tls_id
                time.sleep(6)  # fbref scraping limit
                
                # Get match IDs from the schedule
                match_ids = self.get_matches_from_schedule(tid, sl, lg_id)
                if match_ids is None:
                    stat_df['match_id'] = np.nan
                else:
                    stat_df['match_id'] = match_ids
            except Exception as e:
                self.logger.error(f"Could not scrape data for {stat} statistics for {tid}-{lg_id}-{sl} due to error {e}")
        else:
            try:
                # Scrape the data for other stats
                stat_df = self.scrape_data_html(url, index=0).droplevel(level=0, axis=1)
                stat_df = self.add_suffix_to_repeat_headers(stat_df)  # Get unique column names
                stat_df = stat_df.dropna(subset=['Date']).reset_index(drop=True)
                stat_df['tls_id'] = tls_id
                stat_df['match_id'] = np.nan
            except Exception as e:
                self.logger.error(f"Could not scrape data for {stat} statistics for {tid}-{lg_id}-{sl} due to error {e}")
        return stat_df
    
    # Helper Methods
    def scrape_new_team_match_stats(self, update_id, old_match_reference_date, ):
        """
        Scrape fbref team stats based on both new team league seasons and prior scrape for all possible statistics.

        All team league seasons that end after old_match_reference_date will be scraped.
        Note that some team league seasons only have a smaller subset of statistics available.

        Parameters
        ----------
        update_id : str
            ID corresponding to the update date and run number.
        old_match_reference_date : str
            Date in `%Y-%m-%d` format. Fetch matches for league seasons with end dates after this date.

        Returns
        -------
        dict
            Dictionary with all team match stat data for updated team-league-season IDs.
        """
        # Get tls that were ongoing already and need to be added to
        tls = self.data_dict['tls']
        ls = self.data_dict['ls']
        tls_old_update = tls.merge(ls[['ls_id', 'lg_end']], on='ls_id', how='left')
        tls_old_update = tls_old_update[tls_old_update.lg_end > old_match_reference_date].reset_index(drop=True)

        # Get all new tls added if any
        if self.is_new_ls == True:
            # Get reference team-league-seasons and concat dataframes 
            file_path = f"data/fbref/tls/tls_{update_id}.csv"
            tls_new_update = pd.read_csv(file_path, index_col=0)
            # Concat dataframes
            tls_update = pd.concat([tls_new_update, tls_old_update], axis=0).reset_index(drop=True)
        else:
            tls_update = tls_old_update

        # Get number of ids for progress check and initialize scrape times list
        num_items = tls_update.shape[0]
        scrape_times = []

        # Initiate dict for storing stat date
        new_team_matches_dict = {}
       
        # Iterate through tls_update rows to scrape data and store
        for i, row in tls_update.iterrows():
            # Begin Timer
            start_time = time.time()
            # Create subdict for each unique tls id
            tls_id = row.tls_id
            new_team_matches_dict[tls_id] = {}
            # determine which stats to scrape:
            if row.has_adv_stats == "check_after_start":
                stats = []
            elif row.has_adv_stats == "no":
                stats = ['schedule', 'shooting', 'keeper', 'misc']
            else:
                stats = self.stats
            try:
                for stat in stats:
                    # Get data
                    stat_df = self.scrape_team_match_stats(row.tid, row.season_long, row.lg_id, stat)
                    # store in stat_dfs dict
                    new_team_matches_dict[tls_id][stat] = stat_df.to_json()
                    time.sleep(6) # fbref scraping limit
                # End Timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully scraped team match stats for tls: {row.tls_id}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            except Exception as e:
                self.logger.error(f"Could not scrape stats for tls: {tls_id} due to error: {e}")
                time.sleep(6) # fbref scraping limit
        self.logger.info(f"Successfully scraped new team match stats for update: {update_id}. It took {np.sum(scrape_times):.2f} seconds to complete.")
        return new_team_matches_dict
        
    def get_matches_from_schedule(self, tid, sl, lg_id):
        """
        Retrieve match IDs from the scheduling stats page for a specific team, season, and league.

        Parameters
        ----------
        tid : str
            fbref team ID.
        sl : str
            fbref season ID.
        lg_id : int
            fbref league ID.

        Returns
        -------
        list of str or None
            A list of match IDs for the given team-league-season. Returns None if an error occurs.
        """
        url = "https://fbref.com/en/squads/{}/{}/matchlogs/c{}/schedule/".format(tid, sl, lg_id)
        url = "https://fbref.com/en/squads/{}/{}/matchlogs/c{}/schedule/".format(tid, sl, lg_id)
        try:
            # Send an HTTP GET request to the constructed URL
            response = requests.get(url)

            # Parse the response content using BeautifulSoup
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find the relevant script tag containing JSON data
            scripts = soup.find_all('script', {'type': 'application/ld+json'})
            json_content = scripts[1].string

            # Load the JSON content
            data = json.loads(json_content)

            # Initialize an empty list to store match IDs
            match_ids = []

            # Iterate through the JSON data to extract match URLs and match IDs
            for i in range(len(data)):
                match_url = data[i].get("url", "")
                match_id = re.findall(r"matches/(\w{8})/", match_url)[0]
                match_ids.append(match_id)
            # Return the list of match IDs
            return match_ids
        except Exception as e:
            self.logger.error(f"Could not fetch match ids for {tid}-{lg_id}-{sl} due to error: {e}")

    