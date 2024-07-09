import pandas as pd
import numpy as np
import time
import re
from scraper.fbref_scraper import FbrefScraper

class FbrefMatchesScraper(FbrefScraper):
    """
    Scrapes unique mtach ids, given a football reference league ID and season ID.

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping match data from FBref.

    Main Functionality Methods
    --------------------------
    get_and_save_new_matches(self, update_id, match_reference_date):
        Retrieve new matches data from the latest league season update and save it to a file.
    get_matches_from_league_season(self, lg_id, sl):
        Fetch and parse match data for a specified league and season from FBref.
    """
    # Initialization Methods
    def __init__(self, is_new_ls=True):
        # Call parent class (FbrefEntity) initialization
        super().__init__(name='matches')
        self.is_new_ls = is_new_ls

    # Main Functionality Methods
    def get_and_save_new_matches(self, update_id, old_match_reference_date):
        """
        Retrieve new matches data from the latest league season update and save it to a file.

        Parameters
        ----------
        update_id : str
            Identifier corresponding to the update date and run number.
        old_match_reference_date : str
            Date in `%Y-%m-%d` format. Fetch matches for league seasons with end dates after this date.
        is_new_ls : bool; optionsl
            Whether or not there are new league-seaons from prior update. Default to True
        
        Returns
        -------
        None

        """
        try:
            # Retrieve new matches data
            new_matches_dict = self.get_new_matches(update_id, old_match_reference_date)
            
            # Save dictionary to file
            file_path = f"data/fbref/matches/raw/new_matches_dict_{update_id}.json"
            self.save_raw_data(new_matches_dict, file_path)
            
            # Confirmation message
            print(f"Successfully saved matches data for data update: {update_id}")
        except Exception as e:
            # Error handling
            print(f"Could not save matches data for data update: {update_id} due to error: {e}")

    def get_matches_from_league_season(self, lg_id, sl):
        """
        Fetch and parse match data for a specified league and season from FBref.

        Parameters
        ----------
        lg_id : str
            FBref league ID.
        sl : str
            FBref season ID.

        Returns
        -------
        dict
            A dictionary where keys are match IDs and values are dictionaries containing:
            - 'date': Date of the match.
            - 'home_team_id': ID of the home team.
            - 'away_team_id': ID of the away team.
            - 'ls_id': Combined ID of league and season.

        """
        # Construct URL
        url = f"https://fbref.com/en/comps/{lg_id}/{sl}/schedule/"
        
        try:
            # Scrape data and parse HTML
            soup = self.scrape_data_requests(url)
            matches_dict = {}

            # Find and parse the table containing match data
            table = soup.find("table", {"class": "stats_table"})
            
            if table:
                rows = table.find_all("tr")
                for row in rows[1:]:
                    links = row.find_all("a", href=True)
                    if links:
                        match_links = [l for l in links if re.findall(r"en/matches/(?:\w{8}|\d{4}-\d{2}-\d{2})", l['href'])]
                        squad_links = [l for l in links if re.findall(r"en/squads", l['href'])]

                        # Extract match and squad IDs
                        date_id = next((re.findall(r"en/matches/(\d{4}-\d{2}-\d{2})", l['href'])[0] for l in match_links), np.nan)
                        match_id = next((re.findall(r"matches/(\w{8})", l['href'])[0] for l in match_links[1:]), np.nan) 
                        home_id = re.findall(r"squads/(\w{8})", squad_links[0]['href'])[0] if squad_links else np.nan
                        away_id = re.findall(r"squads/(\w{8})", squad_links[1]['href'])[0] if len(squad_links) > 1 else np.nan
                        
                        # Store match data in dictionary
                        matches_dict[match_id] = {
                            'date': date_id,
                            'home_team_id': home_id,
                            'away_team_id': away_id,
                            'ls_id': f"{lg_id}_{sl}"
                        }

            print(f"Successfully fetched and parsed new matches data for league: {lg_id} season: {sl}!")
            return matches_dict
        
        except Exception as e:
            # Log Error 
            self.logger.error(f"Failed to fetch and parse new matches data for league: {lg_id} season: {sl} due to error: {e}")
            return None

    def get_new_matches(self, update_id, old_match_reference_date):
        """
        Retrieve new matches data for league seasons from two different sources:
        
        1) New league seasons from the latest league season update.
        2) Prior league seasons whose end date occurs after the `old_match_reference_date`.

        The method scrapes all matches for any league season whose end date occurs after the `old_match_reference_date`.

        Parameters
        ----------
        update_id : str
            Identifier corresponding to the update date and run number.
        old_match_reference_date : str
            Date in `%Y-%m-%d` format. Fetch matches for league seasons with end dates after this date.
        is_new_ls : bool; optionsl
            Whether or not there are new league-seaons from prior update. Default to True
        Returns
        -------
        dict
            A dictionary where keys are league-season IDs (`ls_id`) and values are dictionaries containing match data fetched from FBref.

        """
        # Load old league-season data
        ls = self.data_dict['ls']

        if self.is_new_ls == True:
            # Load new league seasons
            file_path_new_ls = f"data/fbref/ls/ls_{update_id}.csv"
            new_ls_update = pd.read_csv(file_path_new_ls, index_col=0)

            # Load prior league seasons with end date > match_reference_date
            old_ls_update = ls[ls.lg_end >= old_match_reference_date]

            # Join the two dataframes or take only 
            ls_update = pd.concat([new_ls_update, old_ls_update], axis=0).reset_index(drop=True)
        
        else:
            # If there are no new league seasons, just take matches from seasons in progress
            ls_update = ls[ls.lg_end >= old_match_reference_date].reset_index(drop=True)

        # Get number of ids for progress check and initialize scrape times list
        num_items = ls_update.shape[0]
        scrape_times = []

        # Initialize dictionary to store data
        new_matches_dict = {}

        # Iterate through ls_update dataframe
        for i, row in ls_update.iterrows():
            # Begin Timer
            start_time = time.time()
            new_matches_dict[row.ls_id] = self.get_matches_from_league_season(row.lg_id, row.season_long)
            time.sleep(6) # fbref scraping restriction
            # End Timer
            end_time = time.time()
            # Append scrape time
            scrape_times.append(end_time - start_time)
            # Calculate and print estimated time left
            est_time_left = np.mean(scrape_times) * (num_items - i - 1)
            print(f"Successfully Scraped seasons for league: {row.lg_id} season: {row.season_long}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
        return new_matches_dict
                

        
        
        
    