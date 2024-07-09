import numpy as np
import time
import re
from scraper.fbref_scraper import FbrefScraper

class FbrefLeagueSeasonsScraper(FbrefScraper):
    """
    Scrapes unique league-season values, given a football reference league ID.

    This class also scrapes data relating to league-season start and end dates, whether advanced stats
    are available, and the round format.

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping league-season data from FBref.

    Main Functionality Methods
    --------------------------
    scrape_and_save_all_league_seasons(self, update_id):
        Scrape and save all updated league seasons
    scrape_all_league_seasons(self):
        Iterate through all currently tracked leagues to get all possible league seasons.
    scrape_league_season(self, lg_id):
        Given an FBref league ID, get list of long season ids that FBref has data for.

    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefScraper) initialization
        super().__init__(name='ls')

    # Main Class Functionality Methods
    def scrape_and_save_all_league_seasons(self, update_id):
        """
        Scrape and save all updated league seasons

        Parameters
        ----------
        update_id : str
            id corresponding to update date and run number
        """
        # Scrape Data
        ls_dict = self.scrape_all_league_seasons()

        # Save Data
        file_path = f"data/fbref/ls/raw/ls_dict_{update_id}.json"
        self.save_raw_data(ls_dict, file_path)
    
    def scrape_all_league_seasons(self):
        """
        Iterate through all currently tracked leagues to get all possible league seasons.

        Returns
        -------
        dict
            A dictionary with league IDs as keys and lists of seasons in long format as values.

        Notes
        -----
        - This method iterates through each league ID in the 'leagues' table and collects all available
        season names using both HTML scraping and requests methods.
        - It handles cases where no seasons are found by trying an alternative method after a delay.
        - Progress and estimated time remaining are printed during execution.
        """
        # Load leagues table
        leagues = self.data_dict['leagues']

        # Get number of ids for progress check and initialize scrape times list
        num_items = len(leagues.lg_id)
        scrape_times = []

        # Initialize dict for storing league season data
        ls_dict = {}
        for i, lg_id in enumerate(leagues.lg_id):
            try:
                # Start Timer
                start_time = time.time()
                # Scraper Data
                seasons_long = self.scrape_league_season(lg_id)
                # Try requests method if html method yielded no results
                if len(seasons_long) == 0: 
                    time.sleep(6)
                    seasons_long = self.scrape_league_season_requests(lg_id)
                # Store scraped data
                ls_dict[int(lg_id)] = seasons_long
                time.sleep(6)  # sleep for 6 seconds due to fbref scraping restriction
                # End timer
                end_time = time.time() 
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully Scraped seasons for {lg_id}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            except Exception as e:
                self.logger.error(f"Web request error fetching data for {lg_id}: {e}")
        total_time = np.sum(scrape_times)
        print(f"It took {total_time:.4f} seconds to scrape all league seasons")
        return ls_dict

    def scrape_league_season(self, lg_id):
        """
        Given an FBref league ID, returns a list of long season names that FBref has data for.

        Parameters
        ----------
        lg_id : int or str
            A football reference league ID; typically a 1-3 digit number. Can be passed in int or str form.

        Returns
        -------
        list of str
            A list of long season names for the given league ID.
        """
        url = f"https://fbref.com/en/comps/{str(lg_id)}/history/"
        data = self.scrape_data_html(url, index=0)
        if data is None:
            seasons_long = []
        elif 'Season' in data.columns:
            seasons_long = list(data.Season)
        elif 'Year' in data.columns:
            seasons_long = list(data.Year)
        else:
            seasons_long = []
        if len(seasons_long) == 0:
            self.logger.error(f"Could not extract any seasons for league {lg_id} using requests method")
        else:
            print(f"Successfully extracted seasons for league {lg_id} using requests method!")
        return seasons_long

    def scrape_league_season_requests(self, lg_id):
        """
        Extract league seasons using requests for competitions with different formatting.

        Parameters
        ----------
        lg_id : int or str
            A football reference league ID.

        Returns
        -------
        list of str
            A list of long season names for the given league ID.
        """
        print(lg_id)
        url = f"https://fbref.com/en/comps/{str(lg_id)}/history/"
        soup = self.scrape_data_requests(url)
        comps = soup.find_all('span', class_='section_anchor')
        # create empty list to store
        seasons_long = []
        for comp in comps:
            try:
                sl = re.findall(r"\d{4}-\d{4}", comp['data-label'])[0] # try two different season types
                seasons_long.append(sl)
            except:
                try:
                    sl = re.findall(r"\d{4}", comp['data-label'])[0] # try two different season types
                    seasons_long.append(sl)
                except:
                    continue
        if len(seasons_long) == 0:
            print(f"Could not extract any seasons for league {lg_id} using requests method")
        else:
            print(f"Successfully extracted seasons for league {lg_id} using requests method!")
        return seasons_long

    # Helper Methods
    def scrape_league_season_start_end(self, lg_id, season_long):
        """
        Scrape the league start and end dates for a given league ID and season in long format,
        and determine whether the league has rounds.

        Parameters
        ----------
        lg_id : int or str
            The football reference league ID.
        season_long : str
            The season in long format (e.g., '2023-2024' or '2022'); acts as football reference season id

        Returns
        -------
        tuple
            A tuple containing league start date, end date, and a boolean indicating whether the league has rounds.

        Raises
        ------
        Exception
            If there is an error while scraping or processing the data.

        Notes
        -----
        - This method fetches data from the specified URL using HTML scraping.
        - It checks if the competition has multiple rounds by looking for the 'Round' column in the scraped data.
        - It converts the 'Date' column to datetime format and returns the minimum and maximum dates as league start and end dates.
        - If an error occurs during scraping, NaN values are returned for league start date, end date, and the round indicator.
        """
        # Create URL
        url = f"https://fbref.com/en/comps/{str(lg_id)}/{season_long}/schedule/"
        try:
            data = self.scrape_data_html(url, index=0)
            # Check if competition only has a single regular season round or multiple rounds
            if 'Round' in data.columns:
                has_rounds = True
            else:
                has_rounds = False
            # Convert date column to datetime format and extract min for league start and max for tentative league end
            data = data.dropna(subset=['Date']).reset_index(drop=True)
            dates = [x for x in data.Date if x != 'Date']
            lg_start = min(dates)
            lg_end = max(dates)
            print(f"Successfully got league start: {lg_start} and end: {lg_end} for league: {lg_id} season: {season_long}")
        except Exception as e:
            # Log error and set return to nas
            self.logger.error(f"Could not get league start or end for league: {lg_id} season {season_long} due to error {e}")
            lg_start = np.nan
            lg_end = np.nan
            has_rounds = np.nan
        return (lg_start, lg_end, has_rounds)

    def update_lg_start_end(self, reference, update_id):
        """
        Update league start and end dates for leagues starting after a reference date,
        especially for competitions with multiple rounds.

        This function iterates through the league seasons table, scrapes updated start
        and end dates using `scrape_league_season_start_end` method, and saves a dataframe
        with updated values to a temporary file.

        Parameters
        ----------
        reference : datetime
            Only update league seasons starting after this reference date.
        update_id : str
            id corresponding to update date and run number

        Notes
        -----
        - This method uses `scrape_league_season_start_end` to fetch updated league start and end dates.
        - It does not overwrite the production version of league season data; it saves to a temporary file
        for verification and validation.
        - Sleeps for 3 seconds between each scraping operation due to scraping restrictions on the source website.

        Raises
        ------
        - Exception: If there is an error while iterating through league seasons, scraping data,
        or saving the updated data to file.

        """
       
        # Load league seasons table from data dictionary
        ls = self.data_dict[self.name]
        
        # Filter league seasons starting after the reference date
        ls_subset = ls[ls.lg_start >= reference].reset_index(drop=True)
        
        # Initialize lists to store new start and end dates
        new_lg_start = []
        new_lg_end = []
        
        # Get number of rows for progress check and initialize scrape times list
        num_items = ls_subset.shape[0]
        scrape_times = []

        # Iterate through filtered league seasons
        for i, row in ls_subset.iterrows():
            try:
                # Begin timer
                start_time = time.time()
                # Scrape updated start and end dates for each league season
                data = self.scrape_league_season_start_end(row.lg_id, row.season_long)
                new_lg_start.append(data[0])
                new_lg_end.append(data[1])
                # Sleep for 6 seconds due to scraping restriction
                time.sleep(6)
                # End timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Record estimated time left and update progress
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully Scraped league-season start/end {row.lg_id}-{row.season_long}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
                
            except Exception as e:
                # Log error if an exception occurs
                self.logger.error(f"Failed to update and save league start and end date for league: {row.lg_id} and season: {row.season_long}. Error: {e}")
                new_lg_start.append(row.lg_start)
                new_lg_end.append(row.lg_end)
                # Fbref Scraping restriction
                time.sleep(6)
        
        # Update league season dataframe with new start and end dates
        ls_subset.lg_end = new_lg_end
        ls_subset.lg_start = new_lg_start
        
        # Save updated dataframe to a temporary file
        try:
            self.save_raw_data(raw_data=ls_subset, file_path=f"data/fbref/ls/temp/updated_lg_end_{update_id}.csv")
            # Calculate and print execution time
            elapsed_time = np.sum(scrape_times)
            self.logger.info(f"It took {elapsed_time:.4f} seconds to successfully update and save league season start and end dates for leagues beginning after {reference}")

        except Exception as e:
            # Log error if an exception occurs
            self.logger.error(f"Failed to save updated league start and end dates. Error: {e}")

    def check_adv_stats(self, lg_id, season_long):
        """
        Check if a league-season has advanced statistics based on league ID and season.

        Parameters
        ----------
        lg_id : int
            Football reference league ID.
        season_long : str
            The season in long format (e.g., '2023-2024' or '2022'); acts as football reference season id

        Returns
        -------
        str
            Indicates the availability of advanced statistics:
            - 'group_only': If advanced statistics are available only for specific leagues.
            - 'yes': If advanced statistics like expected goals (xG) are available for the league-season.
            - 'no': If advanced statistics are not available.

        Notes
        -----
        - Certain leagues (ID 8, 14, 19) are hardcoded to have 'group_only' type of advanced stats.
        - For other leagues, it checks the schedule data of the specified season to determine
        the presence of the 'xG' column as an indicator of advanced statistics availability.
        """
        # Check if the league is hardcoded to have 'group_only' advanced stats
        if lg_id in [8, 14, 19]:
            has_adv_stats = 'group_only'
        else:
            # Scrape schedule data for the specified league ID and season
            url = f"https://fbref.com/en/comps/{lg_id}/{season_long}/schedule/"
            data = self.scrape_data_html(url=url, index=0)
            
            # Check if 'xG' column exists in the data to determine advanced stats availability
            if 'xG' in data.columns:
                has_adv_stats = 'yes'
            else:
                has_adv_stats = 'no'
        
        return has_adv_stats
        
    
        
            
            
            
        
        

    
