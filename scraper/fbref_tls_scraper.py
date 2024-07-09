import pandas as pd
import numpy as np
import time

from scraper.fbref_scraper import FbrefScraper

class FbrefTeamLeagueSeasonsScraper(FbrefScraper):
    """
    Extracts all team ids from a given league and season and stores/saves in a team-league-season dataframe.

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping team-league-season data from FBref.

    This class should be used to update team data after new league-season data has been updated and cleaned.

    Main Functionality Methods
    --------------------------
    scrape_save_all_new_team_links(self, update_id):
        Scrape and save all teams from using new league seasons data and save to file.
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefScraper) initialization
        super().__init__(name='tls')
    
    # Main Functionality Methods
    def scrape_save_all_new_team_links(self, update_id):
        """
        Scrape and save all teams from using new league seasons data and save to file.

        Parameters
        ----------
        update_id : str
            id corresponding to update date and run number
        """
        # Scrape Data
        tls_dict = self.scrape_all_new_team_links(update_id)
        # Save to file
        file_path = f"data/fbref/tls/raw/team_links_{update_id}.json"
        self.save_raw_data(tls_dict, file_path)
    
    # Helper Methods
    def scrape_all_new_team_links(self, update_id):
        """
        Scrape all team links for new league seasons.

        New league seasons are loaded based on latest league seasons update determined by update_id argument.

        Parameters
        ----------
        update_id : str
            id corresponding to update date and run number

        Returns
        -------
        dict
            Dictionary containing league-season keys with list of team links as values
        """
        # Load new league seasons dataframe
        file_path = f"data/fbref/ls/ls_{update_id}.csv"
        ls_update = pd.read_csv(file_path, index_col=0)

        # Create empty dataframe
        tls_dict = {}

        # Get number of ids for progress check and initialize scrape times list
        num_items = ls_update.shape[0]
        scrape_times = []

        for i, row in ls_update.iterrows():
            try:
                # Begin Timer
                start_time = time.time()
                # Scrape Data
                team_links = self.scrape_team_links(row.lg_id, row.season_long) # get team links
                # Store Data
                tls_dict[f"{row.ls_id}"] = team_links
                time.sleep(6) # fbref web scraping limit
                # End Timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully Scraped team links for {row.lg_id}-{row.season_long}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            
            except Exception as e:
                self.logger.error(f"Could not scrape tls data for league: {row.lg_id} and season: {row.season_long}. Error: {e}")
        
        # Log total time
        self.logger.info(f"It took a total of {np.sum(scrape_times):.2f} seconds to scrape new team links data")
        return tls_dict

    def scrape_team_links(self, lg_id, season_long):
        """
        Given a league id and season in long format, get all team urls corresponding to league-season. 

        Parameters
        ----------
            lg_id: football reference league id
            season_long: football reference season id

        Returns
        -------
        list
            list of team link URLs
        """ 
        # Format URL from passed parameters
        url = f"https://fbref.com/en/comps/{str(lg_id)}/{season_long}/stats/"
        try:
            # Get Data
            soup = self.scrape_data_requests(url)
            if soup is not None:
                # Extract team links from the league URL
                team_links = []
                table = soup.find("table", {"class": "stats_table"})
                if table:
                    rows = table.find_all("tr")
                    for row in rows[1:]:  # Skipping the header row
                        team_link = row.find("a", href=True)
                        if team_link:
                            team_links.append("https://fbref.com" + team_link["href"])
                print(f"Successfully scraped team links for league: {lg_id} season: {season_long}")
                return team_links
            else:
                # Log error
                self.logger.error(f"Could not scrape team links for league: {lg_id} season: {season_long}")
                return None
        except Exception as e:
            # Log error
            self.logger.error(f"Could not scrape team links for league: {lg_id} season: {season_long} due to error {e}")

    
            
    
        
        

    