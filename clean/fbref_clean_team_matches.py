import pandas as pd
import numpy as np
import json
import time
import re
from clean.fbref_clean_match_stats import FbrefCleanMatchStats

class FbrefCleanTeamMatches(FbrefCleanMatchStats):
    """
    This class is used for cleaning match-level team data

    This class inherits from FbrefCleanMatchStats and provides additional methods specifically
    for cleaning match stat data from raw data scraped by the FbrefTeamMatchesScraper and FbrefPlayerMatchesScraper classes

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id, old_match_reference_date,  new_match_reference_date):
        Cleans raw team match data using the `match_stat_clean` method and handles logging.
    """
    # Initialization Methods
    def __init__(self, old_match_reference_date, new_match_reference_date):
        # Call parent class (FbrefCleanMatchStats) initialization
        super().__init__(
            name='team_matches',
            old_match_reference_date=old_match_reference_date,
            new_match_reference_date=new_match_reference_date
            )
        self.primary_key = 'team_match'

    # Main Functionality Methods
    def clean_data(self, update_id):
        """
        Cleans raw team match data using the `match_stat_clean` method and handles logging.

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
            self.logger.info(f"Successfully cleaned all team matches data for {update_id}!")
            return clean_data
        except Exception as e:
            # Log error message if an exception occurs
            self.logger.error(f"Could not clean team matches data for {update_id} due to error: {e}")
    
    # Helper Methods
    def parse_concat_match_stat_dfs(self, new_team_matches_dict):
        """
        Parses raw team match data from a dictionary, concatenates the dataframes for each statistic,
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
        num_items = len(list(new_team_matches_dict.keys()))
        scrape_times = []

        # Initiate dictionary to store parsed dataframes
        combined_new_team_matches_dict = {}
        # Initiate dictionary to get errors
        bad_matches = {'bad_matches': []}

        # Iterate through raw data
        for i, tls in enumerate(list(new_team_matches_dict.keys())):
            try:
                # Begin Timer
                start_time = time.time()
                for stat in list(new_team_matches_dict[tls].keys()): 
                    df = pd.read_json(new_team_matches_dict[tls][stat]) # convert json df to pandas df
                    df = df.iloc[:-1,:] # exclude totals row
                    # Check if stat is already populated in dict or not
                    if stat in combined_new_team_matches_dict:
                        combined_new_team_matches_dict[stat] = pd.concat([combined_new_team_matches_dict[stat],df], axis=0)
                    else:
                        combined_new_team_matches_dict[stat] = df
                # End Timer
                end_time = time.time()
                # Append scrape time
                scrape_times.append(end_time - start_time)
                # Calculate and print estimated time left
                est_time_left = np.mean(scrape_times) * (num_items - i - 1)
                print(f"Successfully parsed team match stats for tls: {tls}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
            except Exception as e:
                # Log error and append to bad matches
                self.logger.error(f"Could not parse team data for tls: {tls} due to error: {e}")
                bad_matches['bad_matches'].append(tls)
        return combined_new_team_matches_dict, bad_matches

    def clean_primary_stat_df(self, schedule_df_raw):
        """
        Cleans raw schedule dataframe into the format needed to insert into the team_matches table.

        Parameters
        ----------
        schedule_df_raw : pandas.DataFrame
            Raw data for schedule stat type taken from FbrefTeamMatchesScraper output.

        Returns
        -------
        pandas.DataFrame
            Returns the cleaned schedule dataframe `schedule_df_clean` ready for insertion into the team_matches table.

        """
        try:
            # Clean goals for and against columns
            schedule_df_clean = self.clean_team_schedule_gf_ga(schedule_df_raw)

            # Drop games where there is no goal info
            schedule_df_clean = schedule_df_clean[~schedule_df_clean.GF.isna()]
            
            # Clean result column and handle shootout results
            schedule_df_clean = self.clean_result(schedule_df_clean)
            
            # Clean venue column (home/away)
            schedule_df_clean.loc[:,'Venue'] = schedule_df_clean.Venue.apply(self.clean_venue)
            
            # Extract team ids and create unique team_match identifier
            schedule_df_clean = schedule_df_clean.copy()
            schedule_df_clean.loc[:, 'tid'] = [str(x)[:8] for x in list(schedule_df_clean.tls_id)]
            schedule_df_clean.loc[:, 'team_match'] = schedule_df_clean.tid + "_" + schedule_df_clean.match_id
            
            # Format columns of the cleaned dataframe
            schedule_df_clean = self.format_columns(schedule_df_clean, 'schedule')

            # Drop tls = inf values
            schedule_df_clean = schedule_df_clean[schedule_df_clean.tls_id != np.inf]
            
            # Drop rows with missing team_match and duplicates, reset index
            schedule_df_clean = schedule_df_clean.dropna(subset=['team_match']).drop_duplicates(subset=['team_match']).reset_index(drop=True)
            
            print("Successfully cleaned team matches schedule data!")
            return schedule_df_clean
        except Exception as e:
            # Log Error
            self.logger.error(f"Could not clean team matches schedule data due to error: {e}")

    def clean_non_primary_stat_df(self, stat_df_raw, stat, update_id):
        """
        Clean a single raw non-schedule stat dataframe into a format that can be concatenated and added to the final team matches CSV.

        This function requires a cleaned schedule dataframe containing match ID data.

        Parameters
        ----------
        stat_df_raw : pandas.DataFrame
            Raw dataframe scraped from fbref team gamelogs by FbrefTeamMatchesScraper.

        stat: str
            Name of the stat table (one of self.stats attributes).

        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame
            Returns the cleaned dataframe `stat_df_clean` ready for concatenation with other team matches dataframes.

        Raises
        ------
        ValueError
            If an invalid stat name is provided or if 'schedule' stat is attempted, as this function does not work for schedule data.

        Notes
        -----
        This function relies on a previously cleaned schedule dataframe, which should be saved using a month_year string in its filename.

        """
        # Raise value error if invalid stat name or schedule stat name used
        if stat not in self.stats:
            raise ValueError("Invalid stat reference. Must be in entity stats attribute list.")
        elif stat == 'schedule':
            raise ValueError("Invalid stat reference. This function does not work for the schedule stat.")

        try:
            # Obtain match_ids from the cleaned schedule dataframe
            stat_df_raw.Date = stat_df_raw.Date.astype(str)
            stat_df_raw = self.get_match_ids_from_schedule(stat_df_raw, update_id)
            
            # Extract team ids and create unique team_match identifier
            stat_df_raw['tid'] = [x[:8] for x in stat_df_raw.tls_id]
            stat_df_raw['team_match'] = stat_df_raw.tid + "_" + stat_df_raw['match_id']
            
            # Format columns of the raw dataframe
            stat_df_clean = self.format_columns(stat_df_raw, stat)
            
            # Drop rows with missing team_match and duplicates, reset index
            stat_df_clean = stat_df_clean.dropna(subset=['team_match']).drop_duplicates(subset=['team_match']).reset_index(drop=True)
            
            print(f"Successfully cleaned team matches for {update_id} {stat} data!")
            return stat_df_clean
        except Exception as e:
            self.logger.error(f"Could not clean team matches for {update_id} {stat} data due to error: {e}")
        
   
    def get_match_ids_from_schedule(self, stat_df_raw, update_id):
        """
        Use a left join to retrieve match ids for each stat table from the CLEAN schedule df. Returns stat_df with match ids.
        
        This function should be run after cleaning the schedule dataframe.

        Parameters
        ----------
        stat_df_raw: DataFrame
            Raw dataframe for a given stat type.

        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        DataFrame
            Returns the input dataframe `stat_df_raw` augmented with a 'match_id' column.

        Notes
        -----
        This function assumes that the schedule dataframe has been previously cleaned and saved using the 
        `clean_save_team_matches_schedule_df` method.

        """
        # Load latest clean schedule df
        file_path = f"data/fbref/team_matches/temp/schedule_df_clean_{update_id}.csv"
        try:
            schedule_df_clean = self.load_raw_data(file_path)
        except:
            print("Make sure to use clean_save_team_matches_schedule_df method to clean and save schedule df before using this method")
        
        # Drop existing match_id column from stat_df_raw to avoid duplication
        stat_df_raw = stat_df_raw.drop(columns=['match_id'])
        
        # Merge stat_df_raw with schedule_df_clean to obtain match_id
        stat_df_raw_match = stat_df_raw.merge(
            schedule_df_clean[['date', 'tls_id', 'match_id']],
            how='left',
            left_on=['Date', 'tls_id'], 
            right_on=['date', 'tls_id'])
        
        # Drop rows with NA match_ids and unnecessary columns
        stat_df_raw_match = stat_df_raw_match.dropna(subset=['match_id'])
        stat_df_raw_match = stat_df_raw_match.drop(columns=['Date']).reset_index(drop=True)
        
        return stat_df_raw_match

    def clean_team_schedule_gf_ga(self, schedule_df):
        """
        Cleans the GF (Goals For) and GA (Goals Against) columns in the schedule dataframe by extracting shootout stats.
        
        This function processes the GF and GA columns to separate regular match goals and shootout goals into distinct columns. 
        It creates two new columns: `so_gf` (shootout goals for) and `so_ga` (shootout goals against). 
        The function also ensures that all data is converted to float before returning the cleaned dataframe.

        Parameters
        ----------
        schedule_df : pandas.DataFrame
            Stat dataframe with stat type schedule. This is raw data from FbrefTeamMatchesScraper.

        Returns
        -------
        pandas.DataFrame
            Cleaned dataframe with separate columns for regular goals and shootout goals.
        """
        # Convert GF and GA columns to string type for easier manipulation
        schedule_df['GF'] = schedule_df['GF'].astype(str)
        schedule_df['GA'] = schedule_df['GA'].astype(str)

        # Initialize a dictionary to store cleaned values
        clean_cols = {
            'GF': {'clean': [], 'so': []},
            'GA': {'clean': [], 'so': []}
        }
    
        # Iterate through the dataframe rows
        for _, row in schedule_df.iterrows():
            for col in ['GF', 'GA']:
                # Extract regular goals and shootout goals using regex
                check = re.findall(r"(\d+)\s+\((\d+)\)", row[col])
                if check:
                    # If shootout stats are found, separate them
                    clean_cols[col]['clean'].append(check[0][0])
                    clean_cols[col]['so'].append(check[0][1])
                else:
                    # If no shootout stats are found, use the original value
                    clean_cols[col]['clean'].append(row[col])
                    clean_cols[col]['so'].append(np.nan)

        # Replace original columns with cleaned values
        schedule_df['GF'] = clean_cols['GF']['clean']
        schedule_df['GA'] = clean_cols['GA']['clean']
        schedule_df['so_gf'] = clean_cols['GF']['so']
        schedule_df['so_ga'] = clean_cols['GA']['so']

        # Convert columns to float type
        for col in ['GF', 'GA', 'so_gf', 'so_ga']:
            schedule_df[col] = schedule_df[col].astype(float)
        return schedule_df

    def clean_result(self, schedule_df):
        """
        This is a helper function that cleans the result column while taking into account shootout results.

        Returns schedule_df with a clean Result, GF, and GA column along with two new columns:
            1) so_gf: if the game had a shootout, shootout goals for
            2) so_ga: if the game had a shootout, shootout goals against

        Parameters
        ----------
        schedule_df : pandas.DataFrame
            Stat dataframe with stat type schedule. This is raw data from FbrefTeamMatchesScraper.

        Returns
        -------
        pandas.DataFrame
            DataFrame with cleaned Result column and new columns for shootout goals.
        """
        # Initialize list for data storing
        clean_result = []

        # Iterate through dataframe
        for i, row in schedule_df.iterrows():
            if (not pd.isna(row.so_gf)) & (not pd.isna(row.so_gf)):
                if row.so_gf > row.so_ga:
                    clean_result.append('W')
                elif row.so_gf < row.so_ga:
                    clean_result.append('L')
                else:
                    clean_result.append(row.Result)
            elif pd.isna(row.Result):
                if row.GF > row.GA:
                    clean_result.append('W')
                elif row.GF < row.GA:
                    clean_result.append('L')
                else:
                    clean_result.append('D')
            else:
                clean_result.append(row.Result)

        clean_result_series = pd.Series(clean_result, index=schedule_df.index)
    
        # Use .loc to set values to avoid SettingWithCopyWarning
        schedule_df.loc[:, 'Result'] = clean_result_series
        return schedule_df

    def clean_venue(self, old_venue):
        """
        Helper function that gives venue column more concise values

        Parameters
        ----------
        old_venue : str
            String representing long-form venue name ('home','away','neutral)
        """
        # Create Mapping
        venue_mapping = {
            'Home':'H',
            'Away':'A',
            'Neutral':'N'
        }
        try:
            return venue_mapping[old_venue]
        except:
            # Return old value if not in mapping
            return old_venue