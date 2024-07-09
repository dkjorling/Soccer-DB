import pandas as pd
import json
from clean.fbref_clean import FbrefClean

class FbrefCleanMatchStats(FbrefClean):
    """
    This is the base class for cleaning and saving match-level statistical data from football reference.

    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning match stat data from raw data scraped by the FbrefTeamMatchesScraper and FbrefPlayerMatchesScraper classes

    Attributes
    ----------
    stats : list
        list of statistical categories to scrape data for
    column_map : dict
        dict containing information to facilitate data cleaning

    Main Functionality Methods
    -------------------------- 
    match_stat_clean(self, update_id, old_match_reference_date,  new_match_reference_date):
        Loads raw match statistics data, parses and cleans individual statistics dataframes, 
        concatenates them together, and saves the cleaned data.

    """
    # Initialization Methods
    def __init__(self, name='match_stats', old_match_reference_date=None, new_match_reference_date=None):
        # Call parent class (FbrefClean) initialization
        super().__init__(name)
        self.set_column_map()
        self.set_stats()
        self.old_match_reference_date = old_match_reference_date
        self.new_match_reference_date = new_match_reference_date
        
    def set_column_map(self):
        """
        Load column map from config directory and set as attribute
        """
        with open("config/column_map.json", "r") as f:
            self.column_map = json.load(f)[self.name]
    
    def set_stats(self):
        """
        Load and set stats attribute from column map
        """
        self.stats = list(self.column_map.keys())[:-1]
    
    # Main Functionality Methods
    def match_stat_clean(self, update_id):
        """
        Loads raw match statistics data, parses and cleans individual statistics dataframes, 
        concatenates them together, and saves the cleaned data.

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
        # Load Data
        file_path = f"data/fbref/{self.name}/raw/{self.name}_raw_{update_id}.json"
        new_match_stats_dict = self.load_raw_data(file_path)

        # Parse and concat individual stat dfs
        combined_new_match_stats_dict, bad_matches = self.parse_concat_match_stat_dfs(new_match_stats_dict)
        # Save bad_matches
        self.save_data(bad_matches, f"data/fbref/{self.name}/temp/bad_matches_{update_id}.json")
            
        # Save clean individual stat dfs
        self.parse_clean_save_match_stat_dfs(combined_new_match_stats_dict, update_id=update_id)

        # Concatenate
        new_match_stats_clean = self.concat_clean_save_all_match_stats(update_id)

        # Only get matches between old reference and new referrnce date
        new_match_stats_clean = new_match_stats_clean[(new_match_stats_clean.date >= self.old_match_reference_date) &  (new_match_stats_clean.date <= self.new_match_reference_date)]
        
        return new_match_stats_clean.reset_index(drop=True)

    # Helper Methods
    def concat_clean_save_all_match_stats(self, update_id):
        """
        Combine all cleaned dataframes and save the result. This function ensures that for columns in the primary stat dataframe
        that are NaN, values from other dataframes are used if available.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame
            A concatenated and cleaned dataframe containing match statistics.
        """
        # Extract the primary stat (the first stat in the stats list)
        primary_stat = self.stats[0]

        # Load cleaned dataframes into a dictionary
        dfs = {}
        for stat in self.stats:
            if stat == primary_stat:
                dfs[stat] = pd.read_csv(f"data/fbref/{self.name}/temp/{stat}_df_clean_{update_id}.csv", dtype={'season_long': 'str'}, index_col=0)
            else:
                dfs[stat] = pd.read_csv(f"data/fbref/{self.name}/temp/{stat}_df_clean_{update_id}.csv", index_col=0)

        
        # First combine all dataframes except the primary stat dataframe
        for i, stat in enumerate(self.stats[1:]):
            if i == 0:
                df_concat = dfs[stat]
            else:
                df = dfs[stat]
                df_concat = df_concat.merge(df, how='outer', on=self.primary_key)

        # Drop duplicates and NaNs in the primary key
        df_concat = df_concat.dropna(subset=[self.primary_key])
        df_concat = df_concat.drop_duplicates(subset=[self.primary_key]).set_index(self.primary_key)
        print(df_concat.shape)

        # Process the primary stat dataframe to fill NaN values with data from other dataframes
        primary_stat_df = dfs[primary_stat].dropna(subset=[self.primary_key])
        primary_stat_df = primary_stat_df.drop_duplicates(subset=[self.primary_key]).set_index(self.primary_key)

        # Iterate through column mapping review columns
        try:
            for col in self.column_map[primary_stat]['review_cols']:
                test = primary_stat_df[primary_stat_df[col].isna()]
                idx = test[test.index.isin(df_concat.index)].index
                if len(idx) > 0:
                    primary_stat_df.loc[idx, col] = df_concat.loc[idx, col]
                # Drop duplicate columns from df_concat
                df_concat = df_concat.drop(columns=[col])
        except:
            pass

        # Reset indices
        df_concat = df_concat.reset_index()
        primary_stat_df = primary_stat_df.reset_index()

        # Append primary_stat_df to the concatenated dataframe, reorder columns, and save the final dataframe
        new_match_stats_clean = primary_stat_df.merge(df_concat, how='left', on=self.primary_key)
        new_match_stats_clean = new_match_stats_clean[self.column_map['all_cols']]
        new_match_stats_clean = new_match_stats_clean.reset_index(drop=True)

        # Save the final dataframe to a CSV file
        new_match_stats_clean.to_csv(f"data/fbref/{self.name}/{self.name}_{update_id}.csv")
        return new_match_stats_clean

    def parse_concat_match_stat_dfs(self, *args, **kwargs):
        """
        This method is a placeholder for parsing raw data output by match stat scraper classes.

        Subclasses must implement this method to provide specific functionality for cleaning stats data.

        Parameters
        ----------
        *args : tuple
            Variable-length positional arguments. These arguments can be used to pass any additional parameters as required by subclasses.

        **kwargs : dict
            Arbitrary keyword arguments. These arguments can be used to pass any additional keyword parameters as required by subclasses.

        Raises
        ------
        NotImplementedError
            This method is not implemented in the base class and must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement clean_data")

        
    def parse_clean_save_match_stat_dfs(self, combined_new_match_stat_dict, update_id):
        """

        Parameters
        ----------
        combined_new_team_matches_dict : pandas.DataFrame

        update_id : str
            The identifier corresponding to the update date and run number. 
        """
        #Format and Save Each Stat as a pandas df to temp file:
        for stat, stat_df in combined_new_match_stat_dict.items():
            if (stat == 'schedule') | (stat == 'summary'):
                stat_df_clean = self.clean_primary_stat_df(stat_df)
            else:
                stat_df_clean = self.clean_non_primary_stat_df(stat_df, stat, update_id)
            file_path = f"data/fbref/{self.name}/temp/{stat}_df_clean_{update_id}.csv"
            self.save_data(stat_df_clean, file_path)

    def clean_primary_stat_df(self, *args, **kwargs):
        """
        This method is a placeholder for cleaning the primary match stat dataframe. 

        Subclasses must implement this method to provide specific functionality for cleaning stats data.

        Parameters
        ----------
        *args : tuple
            Variable-length positional arguments. These arguments can be used to pass any additional parameters as required by subclasses.

        **kwargs : dict
            Arbitrary keyword arguments. These arguments can be used to pass any additional keyword parameters as required by subclasses.

        Raises
        ------
        NotImplementedError
            This method is not implemented in the base class and must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement clean_data")

    def clean_non_primary_stat_df(self, *args, **kwargs):
        """
        This method is a placeholder for cleaning the non-primary match stat dataframes.

        Subclasses must implement this method to provide specific functionality for cleaning stats data.

        Parameters
        ----------
        *args : tuple
            Variable-length positional arguments. These arguments can be used to pass any additional parameters as required by subclasses.

        **kwargs : dict
            Arbitrary keyword arguments. These arguments can be used to pass any additional keyword parameters as required by subclasses.

        Raises
        ------
        NotImplementedError
            This method is not implemented in the base class and must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement clean_data")


    def format_columns(self, stat_df_raw, stat):
        """
        Given a raw stat dataframe, clean column names and return a dataframe with only needed columns.

        This function is based on a customized dictionary containing column names to change and column names to keep.

        Parameters
        ----------
        stat_df_raw : pd.DataFrame
            Raw dataframe scraped from fbref team gamelogs by FbrefTeamMatchesScraper.
        stat : str
            Name of the stat table (one of the self.stats attributes).

        Returns
        -------
        pd.DataFrame
            Cleaned dataframe with renamed and selected columns.
        """
        # Take only columns you want
        cols = self.column_map[stat]['all_cols']
        stat_df_clean = stat_df_raw[cols]
        
        # Rename columns to your choice
        for k, v in self.column_map[stat]['rename_cols'].items():
            stat_df_clean = stat_df_clean.rename(columns={k: v})
        
        return stat_df_clean
    
    