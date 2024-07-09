import pandas as pd
from clean.fbref_clean import FbrefClean

class FbrefCleanPlayerTeamLeagueSeasons(FbrefClean):
    """
    Parse new player-team-league-season data, clean it and save to file

    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning player-team-league-season data from raw data scraped by the FbrefPlayerTeamLeagueSeasonsScraper class

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
        Clean and format raw player-team-league-season data extracted from FBref.
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='ptls')
        self.primary_key = 'ptls_id'
    
    # Main Functionality Methods
    def clean_data(self, update_id):
        """
        Clean and format raw player-team-league-season data extracted from FBref.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        
        Returns
        -------
        pd.DataFrame
            DataFrame containing cleaned player-team-league-season data in the format of the production PTLS table.
        """
        # Call class cleaning method
        clean_data = self.player_team_league_seasons_clean(update_id)
        # Return clean data
        return clean_data

    # Helper Methods
    def player_team_league_seasons_clean(self, update_id):
        """
        Clean and format raw player-team-league-season data extracted from FBref.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pd.DataFrame
            DataFrame containing cleaned player-team-league-season data in the format of the production PTLS table.
        """
        # Load raw data
        file_path = f"data/fbref/ptls/raw/ptls_dict_{update_id}.json"
        ptls_dict = self.load_raw_data(file_path)

        # Initialize dataframe for storing new ptls data
        ptls_df = pd.DataFrame(columns=['ptls_id', 'tls_id', 'pid'])
        ii = 0

        # Iterate through raw data dict
        for k in ptls_dict.keys():
            for pid in ptls_dict[k]:
                # Create ptls id
                ptls_id = str(pid) + "_" + str(k)
                # Store in dataframe
                ptls_df.loc[ii] = [ptls_id, k, pid]
                ii += 1

        # Drop nas and dupes and reset index
        ptls_df = ptls_df.dropna(subset=['ptls_id']).drop_duplicates(subset=['ptls_id']).reset_index(drop=True)
        return ptls_df

                
            
        
        
        
        