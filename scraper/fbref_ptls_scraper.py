import pandas as pd
import numpy as np
import time
import re
from scraper.fbref_scraper import FbrefScraper

class FbrefPlayerTeamLeagueSeasonsScraper(FbrefScraper):
    """
    Scrapes player ids for a given team, league, and season

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping league-season data from FBref.

    Main Functionality Methods
    --------------------------
    get_save_all_pids(self, update_id):
        Retrieve player IDs for all team-league-season entries based on the last tls update and save to a JSON file.
    get_player_ids(self, tid, sl, lg_id):
        Retrieve all player IDs for a specific team-league-season combination from FBref.
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefScraper) initialization
        super().__init__(name='ptls')

    # Main Functionality Methods
    def get_save_all_pids(self, update_id):
        """
        Retrieve player IDs for all team-league-season entries based on the last tls update and save to a JSON file.

        Parameters
        ----------
        update_id : str
            id corresponding to update date and run number
        """
        # get ids
        ptls_dict = self.get_all_pids(update_id)
        # save to file
        file_path = f"data/fbref/ptls/raw/ptls_dict_{update_id}.json"
        try:
            self.save_raw_data(ptls_dict, file_path)
            print(f"Successfully scraped and saved new fbref player ids for update: {update_id}")
        except Exception as e:
            # Log error
            self.logger.error(f"Could not scrape and save new fbref player ids for update {update_id} due to error: {e}!")

    def get_player_ids(self, tid, sl, lg_id):
        """
        Retrieve all player IDs for a specific team-league-season combination from FBref.

        Parameters
        ----------
        tid : str
            FBref team ID.
        sl : str
            Season in long format (e.g., '2023-2024').
        lg_id : str
            FBref league ID.

        Returns
        -------
        list
            List of FBref player IDs (pids).
        """
        # Create URL from passed parameters
        url = f"https://fbref.com/en/squads/{tid}/{sl}/c{lg_id}/"
        # Fetch data and parse to BeautifulSoup object
        soup = self.scrape_data_requests(url)
        
        # Extract player ids from soup object and return
        table = soup.find("table", {"class":"stats_table"})
        pids = []
        if table:
            rows = table.find_all("tr")
            for row in rows[1:]:
                links = row.find_all("a", href=True)
                if len(links) > 0:
                    for l in links:
                        if len(re.findall(r"en/players/(\w{8})/matchlogs", l['href'])) > 0:
                            pid = re.findall(r"en/players/(\w{8})/matchlogs", l['href'])[0] # parse pid
                            pids.append(pid)
        return pids
    
    # Helper Methods
    def get_all_pids(self, update_id):
        """
        Retrieve player IDs for all team-league-season entries from the corresponding tls update.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        dict
            A dictionary where keys are team-league-season IDs (tls_ids) and values are lists of player IDs (pids).

        """
        # Load updated team league season data
        file_path = f"data/fbref/tls/tls_{update_id}.csv"
        tls_update = pd.read_csv(file_path, index_col=0)
        
        # Get number of ids for progress check and initialize scrape times list
        num_items = tls_update.shape[0]
        scrape_times = []

        # Initialize dict for storing data
        ptls_dict = {}

        # Iterate through updated team league seasons rows
        for i, row in tls_update.iterrows():
            # Extract team-league-season id
            tls_id = row.tls_id
            try:
                # Begin Timer
                start_time = time.time()
                ptls_dict[tls_id] = self.get_player_ids(row.tid, row.season_long, row.lg_id)
                print(f"Successfully scraped player ids for team league season: {tls_id}")
                print()
                time.sleep(6) # fbref scraping restriction
                # End Timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully Scraped player ids for {tls_id}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            except Exception as e:
                # Log Error
                self.logger.error(f"Could not scrape player ids for team league season: {tls_id} due to error: {e}")
                time.sleep(6) # fbref scraping restriction
                
        return ptls_dict
    
    
        
        
            
        
        
            
            
            