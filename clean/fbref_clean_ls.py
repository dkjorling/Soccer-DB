import pandas as pd
import numpy as np
import time
from clean.fbref_clean import FbrefClean

class FbrefCleanLeagueSeasons(FbrefClean):
    """
    Parse new league seasons from all league seasons, clean data, and save to file.

    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning league-season data from raw data scraped by the FbrefLeagueSeasonsScraper class

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
        Extract new league seasons from raw league-seasons data and clean the data
    get_and_save_new_league_seasons(self, update_id):
         Get and save new league seasons for all unique leagues given a raw dictionary with all league-season combos.
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='ls')
        self.primary_key = 'ls_id'

    # Main Class Functionality Methods
    def clean_data(self, update_id, new_match_reference_date):
        """
        Clean raw league seasons data

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        new_match_reference_date : str
            Date used to filter out leagues that start after this date

        """
        clean_data = self.league_seasons_clean(update_id, new_match_reference_date)
        return clean_data

    def get_and_save_new_league_seasons(self, update_id):
        """
        Get and save new league seasons for all unique leagues given a raw dictionary with all league-season combinations.

        Parameters
        ----------
        update_id : str
            id corresponding to update date and run number
        """
        try:
            start_time = time.time()
            # get new league seasons
            new_league_seasons = self.get_new_league_seasons(update_id)
            # save to file
            file_path = f"data/fbref/ls/temp/new_league_seasons_only_{update_id}.csv"
            self.save_data(new_league_seasons, file_path)
            end_time = time.time()
            elapsed_time = end_time - start_time
            self.logger.info(f"It took {elapsed_time:.4f} seconds to extract and save new league seasons")
        except Exception as e:
            self.logger.error(f"Could not update and save league start and end due to error: {e}")
    
    # Helper Methods
    def league_seasons_clean(self, update_id, new_match_reference_date):
        """
        Concatenates all raw data and formats it to match the production table.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        new_match_reference_date : str
            Date used to filter out leagues that start after this date

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the cleaned new league season data, formatted to match the production table.
        
        Notes
        -----
        The function performs the following steps:
        1. Loads raw data from multiple sources (league start and end dates, advanced stats availability, and new league seasons).
        2. Merges the data into a single DataFrame.
        3. Sets the API data source column to NaN.
        4. Retrieves league names and merges them into the DataFrame.
        5. Generates a unique league season ID and formats the DataFrame to match the production table.
        6. Drops rows with NaN values or duplicates in the 'ls_id' column.
        7. Prints the time taken to complete the cleaning process and returns the cleaned DataFrame.
        8. Logs an error and returns None if an exception occurs during the process.
        """
        try:
            # Begin timer
            start_time = time.time()
            # Load data
            lg_start_end = self.load_raw_data(f"data/fbref/ls/raw/new_league_start_end_{update_id}.csv")
            lg_has_adv = self.load_raw_data(f"data/fbref/ls/raw/new_league_has_adv_stats_{update_id}.csv")
            lg_new_seasons_only = self.load_raw_data(f"data/fbref/ls/temp/new_league_seasons_only_{update_id}.csv")
            leagues = self.data_dict['leagues'] # load leagues table to get additional information
            # Merge and clean dfs
            ls_new_merged = lg_start_end.merge(lg_has_adv, on=['lg_id', 'season_long']).merge(lg_new_seasons_only, on=['lg_id', 'season_long'])
            # No longer using api data source so set to na
            ls_new_merged['ls_api'] = np.nan 
            # Get league names
            ls_new = ls_new_merged.merge(leagues[['lg_id', 'lg_name']], how='left', on=['lg_id']) 
            # Change league name to match
            ls_new = ls_new.rename(columns={'name':'lg_name'}) 
            ls_new['ls_id'] = ls_new.lg_id.astype(str) + "_" + ls_new.season.astype(str)
            ls_new = ls_new[self.db_table.columns]
            # Exclude leagues that have not started yet
            ls_new = ls_new[ls_new.lg_start > new_match_reference_date]
            # Drop league season id duplicates
            ls_new = ls_new.dropna(subset=['ls_id']).drop_duplicates(subset=['ls_id']).reset_index(drop=True)
            # End timer
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Successfully cleaned new league season data. It took {elapsed_time:.4f} seconds complete")
            return ls_new
        except Exception as e:
            self.logger.error(f"Could not clean new league season data due to error: {e}")
            return None
    
    
    def get_new_league_seasons(self, update_id):
        """
        Get new league seasons for all unique leagues given a raw dictionary with all league-season combinations.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pd.DataFrame
            A DataFrame with three columns: 'lg_id', 'season' (short format), and 'season_long' (long format).

        Raises
        ------
        Exception
            If any error occurs during the process.
        """
        # Load raw ls dictionary data
        ls_dict = self.load_raw_data(f"data/fbref/ls/raw/ls_dict_{update_id}.json")
        # Create empty list to store dfs
        dfs = []
        # Iterate through league season raw data
        for k, v in ls_dict.items():
            new_ls_short, new_ls_long = self.get_new_seasons(k, v)
            df = pd.DataFrame({'lg_id':k, 'season':new_ls_short, 'season_long':new_ls_long})
            dfs.append(df)
        # Concatenate dataframes
        new_league_seasons = pd.concat(dfs, axis=0, ignore_index=True)
        new_league_seasons.season = new_league_seasons.season.astype(int)
        new_league_seasons.lg_id = new_league_seasons.lg_id.astype(int)
        return new_league_seasons
    
    def get_new_seasons(self, lg_id, total_season_long):
        """
        Given a fbref league ID and a list of total seasons in long format, returns a list of seasons not in the database.

        This function uses the current production league_seasons table as a reference to determine which league seasons are already included.
        Note: Database begins from the 2017-2018 seasons, so anything before this season is excluded.

        Parameters
        ----------
        lg_id : int
            The fbref league ID.
        total_season_long : list of str
            A list containing all seasons associated with the specified league in long format.

        Returns
        -------
        tuple of (list of int, list of str)
            A tuple containing two lists:
            - new_ls_short: List of seasons in short format (years).
            - new_ls_long: List of seasons in long format (e.g., "2017-2018").

        Notes
        -----
        The function performs the following steps:
        1. Subsets the league seasons table based on the given league ID.
        2. Lists the current long seasons already present in the database.
        3. Iterates through all given seasons, and if a season is not in the current list and is from 2017 onwards, it is added to the new lists.
        """
        # subset the league seasons table based on lg_id
        ls_subset = self.db_table[self.db_table.lg_id == int(lg_id)]
        # list current long seasons
        current_sl = list(set(ls_subset.season_long))
        # iterate through all seasons
        new_ls_short = []
        new_ls_long = []
        for sl in total_season_long:
            s = int(str(sl)[:4])
            if (str(sl) not in current_sl) & (s >= 2017): # exclude seasons prior to 2017
                new_ls_short.append(s)
                new_ls_long.append(sl)
        return new_ls_short, new_ls_long

    
        
    def save_updated_league_season_start_end(self, update_id):
        """
        Takes updated league season start and end date output from ls_scraper and saves to the production table.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        None

        Raises
        ------
        Exception
            If any error occurs during the process.

        Notes
        -----
        This method performs the following steps:
        1. Loads the updated league season start and end dates from a temporary file.
        2. Drops rows with missing values from the loaded data.
        3. Backs up the current production table.
        4. Updates the league season start and end dates in the production table with the new values.
        5. Saves the updated production table to the specified file path.
        """
        try:
            updated_start_end = self.load_raw_data(f"data/fbref/ls/temp/updated_lg_end_{update_id}.csv")
            updated_start_end = updated_start_end.dropna().reset_index(drop=True) # drop nas
            # Backup old table
            self.backup_table(update_id)
            # Update applicable league season start and end dates 
            ls_new = self.db_table
            ls_new.loc[ls_new.ls_id.isin(updated_start_end.ls_id), 'lg_start'] = list(updated_start_end.lg_start)
            ls_new.loc[ls_new.ls_id.isin(updated_start_end.ls_id), 'lg_end'] = list(updated_start_end.lg_end)
            # Save to file
            self.save_data(data=ls_new, file_path="data/db_tables/ls.csv")
            print(f"Successfully updated league season start and end dates for league seasons beginning after {update_id}!")
        except Exception as e:
            self.logger.error(f"ould not update league season start and end dates. Error occurred: {e}")

            

   
        
            

    
        
        
                                      

    