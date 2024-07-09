import pandas as pd
import numpy as np
import time
from clean.fbref_clean_match_stats import FbrefCleanMatchStats

class FbrefCleanKeeperMatches(FbrefCleanMatchStats):
    """
    This class is used for cleaning match-level keeper data

    This class inherits from FbrefCleanMatchStats and provides additional methods specifically
    for cleaning keeper match stat data from raw data scraped by the FbrefPlayerMatchesScraper class

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
        Cleans raw player match data using the `match_stat_clean` method and handles logging.
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='keeper_matches')
        self.primary_key = 'keeper_match'
        self.stats = ['summary', 'keeper']
    
    # Main Functionality Methods
    def clean_data(self, update_id):
        """
        Cleans raw keeper match data using the `match_stat_clean` method and handles logging.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame
            A concatenated dataframe of cleaned match statistics.

        """
        try:
            clean_data = self.keeper_matches_clean(update_id)
            print(f"Successfully cleaned all keeper matches data for {update_id}!")
            return clean_data
        except Exception as e:
            print(f"Could not clean keeper matches data for {update_id} due to error: {e}")
            return None
    
    def keeper_matches_clean(self, update_id):
        """
        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        """
        # Load raw data from player matches directory
        file_path = f"data/fbref/player_matches/raw/player_matches_raw_{update_id}.json"
        new_player_matches_dict = self.load_raw_data(file_path)

        # Parse and concat individual stat dfs
        keeper_df_raw, bad_matches = self.parse_concat_keeper_stat_df(new_player_matches_dict)
        # Save bad_matches and keeper clean
        self.save_data(bad_matches, f"data/fbref/keeper_matches/temp/bad_matches_{update_id}.json")

        # Clean keeper df
        keeper_df_clean = self.clean_non_primary_stat_df(keeper_df_raw)
        file_path = f"data/fbref/keeper_matches/temp/keeper_matches_clean_{update_id}.csv"
        self.save_data(keeper_df_clean, file_path)

        # Concatenate temporary files and clean full dataframe
        new_keeper_matches_clean = self.concat_save_clean_keeper_matches(update_id)
        return new_keeper_matches_clean
    
    def concat_save_clean_keeper_matches(self, update_id):
        """
        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        """
        summary_df_clean = pd.read_csv(f"data/fbref/player_matches/temp/summary_df_clean_{update_id}.csv", index_col=0, dtype={'season_long':'str'}) # load data from player matches directory
        keeper_df_clean = pd.read_csv(f"data/fbref/keeper_matches/temp/keeper_matches_clean_{update_id}.csv", index_col=0)
        keeper_df_clean['player_match'] = keeper_df_clean.pid + "_" + keeper_df_clean.match_id
        summary_df_clean['keeper_match'] = summary_df_clean.player_match + "_G"
        keeper_df_clean['keeper_match'] = keeper_df_clean.player_match + "_G"

        # Add na columns
        for col in ['clean_sheets', 'pk_att_against', 'pk_allowed', 'pk_saved', 'pk_missed']:
            keeper_df_clean[col] = np.nan

        new_keeper_matches_clean = keeper_df_clean.merge(summary_df_clean[['keeper_match', 'tls_id','team_match','season_long','date','start', 'position']], how='left', on='keeper_match')
        new_keeper_matches_clean = new_keeper_matches_clean[self.column_map['all_cols']]
        new_keeper_matches_clean = new_keeper_matches_clean.reset_index(drop=True)
        new_keeper_matches_clean.to_csv(f"data/fbref/player_matches/player_matches_{update_id}.csv")
        return new_keeper_matches_clean

    def parse_concat_keeper_stat_df(self, new_player_matches_dict):
        """
        Parses raw keeper match data from a dictionary, concatenates the dataframes for each statistic,
        and returns a dataframe containing all keeper data

        Parameters
        ----------
        new_player_matches_dict : dict
            A dictionary containing raw player match statistics data.
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame
            A concatenated dataframe containing goalkeeper statistics.
        dict
            A dictionary containing matches that could not be processed due to errors.
        """
         # Get number of ids for progress check and initialize scrape times list
        num_items = len(list(new_player_matches_dict.keys()))
        scrape_times = []

        # Initialize an empty dataframe to store goalkeeper statistics
        keeper_df_raw = pd.DataFrame()
        
        # Initialize a dictionary to store matches with errors
        bad_matches = {'bad_matches': []}

        # Iterate through the raw data
        for i, match in enumerate(list(new_player_matches_dict.keys())):
            try:
                # Begin timer
                start_time = time.time()
                # Iterate through each team in the match
                for team in new_player_matches_dict[match].keys():
                    # Get player IDs for the team
                    pids = new_player_matches_dict[match][team]['pids']
                    # Extract goalkeeper statistics
                    df = pd.read_json(new_player_matches_dict[match][team]['keeper'])
                    num_rows = df.shape[0]
                    # Assign player IDs to the dataframe (only for the number of rows in the goalkeeper dataframe)
                    df['pid'] = pids[-num_rows:]
                    df['tid'] = team
                    df['match_id'] = match
                    # Concatenate the goalkeeper dataframe with the existing data
                    keeper_df_raw = pd.concat([keeper_df_raw, df], axis=0)
                # End timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully scraped team keeper stats for match: {match}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            except Exception as e:
                # Print the error and add the match to the list of bad matches
                self.logger.error(f"Could not parse keeper data for match: {match} due to error: {e}")
                bad_matches['bad_matches'].append(match)
        
        # Reset the index of the concatenated dataframe
        keeper_df_raw = keeper_df_raw.reset_index(drop=True)
        
        return keeper_df_raw, bad_matches

    def clean_non_primary_stat_df(self, keeper_df):
        """
        Format keeper statistics tables for keeper matches.

        This method processes keeper statistics dataframe to ensure they are formatted correctly for further analysis.

        Parameters
        ----------
        keeper_df : pandas.DataFrame
            The dataframe containing raw keeper statistic data
            
        Returns
        -------
        pandas.DataFrame
            The cleaned keeper dataframe with the necessary columns and renamed headers.
        """
        # Create a unique identifier for each player match
        keeper_df['player_match'] = keeper_df.pid + "_" + keeper_df.match_id

        # Select the necessary columns from the column map
        keeper_df_clean = keeper_df[self.column_map['keeper']['all_cols']]

        # Rename the columns according to the column map
        for k, v in self.column_map['keeper']['rename_cols'].items():
            keeper_df_clean = keeper_df_clean.rename(columns={k: v})

        return keeper_df_clean
    