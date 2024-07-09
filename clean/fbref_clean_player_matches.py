import pandas as pd
import numpy as np
import time
import json
from clean.fbref_clean_match_stats import FbrefCleanMatchStats
from utils.helpers import load_data

class FbrefCleanPlayerMatches(FbrefCleanMatchStats):
    """
    This class is used for cleaning match-level player data

    This class inherits from FbrefCleanMatchStats and provides additional methods specifically
    for cleaning match stat data from raw data scraped by the FbrefPlayerMatchesScraper class

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id, old_match_reference_date, new_match_reference_date):
        Cleans raw player match data using the `match_stat_clean` method and handles logging.
    """
    # Initialization Methods
    def __init__(self, old_match_reference_date, new_match_reference_date):
        # Call parent class (FbrefCleanMatchStats) initialization
        super().__init__(
            name='player_matches',
            old_match_reference_date=old_match_reference_date,
            new_match_reference_date=new_match_reference_date
            )
        self.primary_key = 'player_match'
        self.stats = ['summary', 'passing', 'passing_types',
                      'defense', 'possession', 'misc']

    # Main Functionality Methods
    def clean_data(self, update_id):
        """
        Cleans raw player match data using the `match_stat_clean` method and handles logging.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        old_match_reference_date : str
            Date referencing lat update latest match date
        new_match_reference_date : str
            Date referencing this update latest match date

        Returns
        -------
        pandas.DataFrame
            A concatenated dataframe of cleaned match statistics.

        """
        try:
            # Clean the data using the match_stat_clean method
            clean_data = self.match_stat_clean(update_id)
            # Log success message
            self.logger.info(f"Successfully cleaned all player matches data for {update_id}!")
            return clean_data
        except Exception as e:
            # Log error message if an exception occurs
            self.logger.error(f"Could not clean player matches data for {update_id} due to error: {e}")

    # Helper Methods
    def parse_concat_match_stat_dfs(self, new_player_matches_dict):
        """
        Parses raw player match data from a dictionary, concatenates the dataframes for each statistic,
        and returns a combined dictionary of dataframes along with a list of any matches that failed to parse.

        Parameters
        ----------
        new_player_matches_dict : dict
            Dictionary containing raw player match data.

        Returns
        -------
        tuple
            A tuple containing:
            - combined_new_player_matches_dict: Dictionary of concatenated dataframes for each statistic.
            - bad_matches: Dictionary containing a list of matches that failed to parse.
        """
        # Get number of ids for progress check and initialize scrape times list
        num_items = len(list(new_player_matches_dict.keys()))
        scrape_times = []

        # Initiate dictionary to store parsed dataframes
        combined_new_player_matches_dict = {}
        # Initiate dictionary to get errors
        bad_matches = {'bad_matches': []}

        # Iterate through raw data
        for i, match in enumerate(list(new_player_matches_dict.keys())):
            try:
                # Begin timer
                start_time = time.time()
                # Iterate through each team
                for team in new_player_matches_dict[match].keys():
                    # Get player ids for each team
                    pids =  new_player_matches_dict[match][team]['pids']
                    # Iterate through each statistic
                    for stat in list(new_player_matches_dict[match][team].keys())[1:-1]: # exclude player ids and keeper 
                        df = pd.read_json(new_player_matches_dict[match][team][stat]) # convert json df to pandas df
                        df = df.iloc[:-1,:] # exclude totals row
                        df['pid'] = pids
                        df['tid'] = team
                        df['match_id'] = match
                        if stat in combined_new_player_matches_dict:
                            combined_new_player_matches_dict[stat] = pd.concat([combined_new_player_matches_dict[stat],df], axis=0)
                        else:
                            combined_new_player_matches_dict[stat] = df
                # End timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully parsed player stats for match: {match}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            except Exception as e:
                # Log error and append to bad matches
                self.logger.error(f"Could not parse player data for match: {match} due to error: {e}")
                bad_matches['bad_matches'].append(match)
        return combined_new_player_matches_dict, bad_matches
    
    def clean_primary_stat_df(self, summary_df):
        """
        Cleans the summary dataframe by adding necessary columns, merging with additional data,
        and renaming columns based on predefined mappings.

        Parameters
        ----------
        summary_df : pandas.DataFrame
            The raw summary dataframe to be cleaned and formatted.

        Returns
        -------
        pandas.DataFrame
            The formatted summary dataframe with necessary columns and renamed headers.

        """
        # Load necessary data dictionaries
        data_dict = load_data()  # Assuming load_data() is a function that loads required data
        matches = data_dict['matches']  # DataFrame containing match details
        tls = data_dict['tls']  # DataFrame containing team league season details

        # Merge summary_df with matches to add ls_id and date
        summary_df = summary_df.merge(matches[['match_id', 'ls_id', 'date']], on='match_id', how='left')

        # Create tls_id combining tid and ls_id
        summary_df['tls_id'] = summary_df['tid'] + "_" + summary_df['ls_id']

        # Merge with tls to add season_long
        summary_df = summary_df.merge(tls[['tls_id', 'season_long']], on='tls_id', how='left')

        # Create additional columns
        summary_df['player_match'] = summary_df['pid'] + "_" + summary_df['match_id']
        summary_df['team_match'] = summary_df['tid'] + "_" + summary_df['match_id']
        summary_df['start'] = np.nan  # Initialize 'start' column with NaN values

        # Convert season_long to string
        summary_df['season_long'] = summary_df['season_long'].astype(str)

        # Ensure all required columns are present and set to NaN if missing
        all_cols = self.column_map['summary']['all_cols']
        for col in all_cols:
            if col not in summary_df.columns:
                summary_df[col] = np.nan

        # Select and rename columns as per predefined mappings
        summary_formatted = summary_df[all_cols].rename(columns=self.column_map['summary']['rename_cols'])

        return summary_formatted.reset_index(drop=True)

    def clean_non_primary_stat_df(self, stat_df, stat, update_id=None):
        """
        Format advanced statistics tables for player matches.

        This method processes advanced statistics dataframes (excluding summary or goalkeeper stats)
        to ensure they are formatted correctly for further analysis.

        Parameters
        ----------
        stat_df : pandas.DataFrame
            The dataframe containing raw advanced statistics for player matches.
        stat : str
            The type of advanced statistic, which must be one of ['passing', 'passing_types', 'defense', 'possession', 'misc'].

        Returns
        -------
        pandas.DataFrame
            The formatted dataframe with the necessary columns and renamed headers.
        """
        # Create a unique identifier for each player match
        stat_df['player_match'] = stat_df.pid + "_" + stat_df.match_id

        # Select the necessary columns from the column map
        df_formatted = stat_df[self.column_map[stat]['all_cols']]

        # Rename the columns according to the column map
        for k, v in self.column_map[stat]['rename_cols'].items():
            df_formatted = df_formatted.rename(columns={k: v})

        return df_formatted
        
