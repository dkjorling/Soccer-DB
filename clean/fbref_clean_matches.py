import pandas as pd
import re
from clean.fbref_clean import FbrefClean

class FbrefCleanMatches(FbrefClean):
    """
    Parse new league seasons from all league seasons, clean data, and save to file.
        
    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning match data from raw data scraped by the FbrefMatchesScraper class

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
        Clean raw matches data for a specified update.
         
    """
    # Initializaiton Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='matches')
        # Set 'name' attribute specifically for FbrefSeasons entity
        self.primary_key = 'match_id'

    # Main Functionality Methods
    def clean_data(self, update_id):
        """
        Clean raw matches data for a specified update.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame or None
            Cleaned matches dataframe if successful, otherwise None.

        """
        try:
            # Extract new match reference from update id
            new_match_reference = re.findall(r"(\d{4}_\d{2}_\d{2})", update_id)[0]
            new_match_reference = new_match_reference.replace("_","-")
            # Clean Data
            clean_data = self.matches_clean(update_id, new_match_reference)
            print(f"Successfully cleaned matches data for {update_id}!")
            return clean_data

        except Exception as e:
            # Log error
            self.logger.error(f"Could not clean matches data for {update_id} due to error: {e}")
            return None

    # Helper Methods
    def matches_clean(self, update_id, new_match_reference):
        """
        Clean raw matches data obtained from the latest update.

        Parameters
        ----------
        update_id : str
            Identifier corresponding to the update date and run number.
        new_match_reference : str
            Date in '%Y-%m-%d' format. Filter out matches occurring after this reference date.

        Returns
        -------
        pandas.DataFrame
            Cleaned matches dataframe containing columns: ['match_id', 'date', 'home_team_id', 'away_team_id', 'ls_id'].

        """
        # Load raw matches data
        file_path = f"data/fbref/matches/raw/new_matches_dict_{update_id}.json"
        new_matches_dict = self.load_raw_data(file_path)

        # Initialize dataframe for storing
        new_matches_df = pd.DataFrame(columns=['match_id', 'date', 'home_team_id', 'away_team_id', 'ls_id'])
        ii = 0

        # Iterate through raw data
        for k in new_matches_dict.keys():
            for kk in new_matches_dict[k].keys():
                new_matches_df.loc[ii] = [kk, new_matches_dict[k][kk]['date'], new_matches_dict[k][kk]['home_team_id'], new_matches_dict[k][kk]['away_team_id'], k]
                ii += 1

        # Drop na values and duplicates
        new_matches_df_clean = new_matches_df.dropna(subset=['match_id']).drop_duplicates(subset=['match_id'])

        # Get matches only between old match reference and new match reference
        new_matches_df_clean = new_matches_df_clean[new_matches_df_clean.date <= new_match_reference]

        # Exclude matches already in player matches table
        pm = self.data_dict['player_matches']
        new_matches_df_clean = new_matches_df_clean[~new_matches_df_clean.match_id.isin(list(pm.match_id))].reset_index(drop=True)

        return new_matches_df_clean

    