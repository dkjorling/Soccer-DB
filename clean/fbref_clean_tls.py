import pandas as pd
import numpy as np
import re
import time
from clean.fbref_clean import FbrefClean

class FbrefCleanTeamLeagueSeasons(FbrefClean):
    """
    Extract team ids from raw team links data and store as a dataframe

    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning team-league-season data from raw data scraped by the FbrefTeamLeagueSeasonsScraper class

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
        Extract team ids from team links, concatenate all raw data and format as a pandas dataframe
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='tls')
        # Set 'name' attribute specifically for FbrefSeasons entity
        self.primary_key = 'tls_id'

    # Main Functionality Methods
    def clean_data(self, update_id):
        """
        Extract team ids from team links, concatenate all raw data and format as a pandas dataframe

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        """
        clean_data = self.team_league_seasons_clean(update_id)
        return clean_data

    # Helper Methods
    def team_league_seasons_clean(self, update_id):
        """
        Concatenates all raw data and formats it to match the production table for team-league-seasons.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame or None
            Cleaned dataframe containing team-league-seasons data, or None if cleaning fails.

        Notes
        -----
        This method performs the following tasks:
        1. Parses team IDs from team links using `parse_team_link_dict`.
        2. Constructs a dataframe `new_tls` containing columns: 'tls_id', 'tid', 'lg_id', 'season', 'ls_id'.
        3. Merges season long and advanced statistics data from the league-seasons table (`ls`) into `new_tls`.
        4. Drops duplicates and NaN values, ensuring data integrity.
        5. Logs errors if cleaning fails and returns None.
        """
        try:
            # Start Timer
            start_time = time.time()

            # Parse team ids from team links
            tls_dict = self.parse_team_link_dict(update_id)

            # Create empty dataframe
            new_tls = pd.DataFrame(columns=['tls_id', 'tid', 'lg_id', 'season', 'ls_id'])
            i = 0 # used to insert rows

            # Iterate through dict rows and store data to new_tls dataframe
            for k, v in tls_dict.items():
                for tid in v:
                    tls_id = tid + "_" + k
                    new_tls.loc[i] = [tls_id, tid, k.split('_')[0], k.split('_')[1], k]
                    i += 1

            # Get season long and has adv stats from league-season table and merge to new_tls dataframe
            ls = self.data_dict['ls']
            new_tls = new_tls.merge(ls[['ls_id', 'season_long', 'has_adv_stats']], how='left', on='ls_id')
            new_tls = new_tls[self.db_table.columns]
            new_tls = new_tls.dropna(subset=['tls_id'])
            new_tls = new_tls.drop_duplicates(subset=['tls_id']).reset_index(drop=True)
            end_time = time.time()
            print(f"Successfully cleaned new team league season data. It took {end_time - start_time:.4f} seconds complete")
            return new_tls

        except Exception as e:
            # Log error
            self.logger.error(f"Could not clean new team league season data due to error: {e}")
            return None

    def parse_team_link_dict(self, update_id):
        """
        Parse all team ids from raw data corresponding to update_id. 
        
        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        dict
            Return dictionary with league season ids as keys and team ids as values
        """
        # Load raw data
        file_path = f"data/fbref/tls/raw/team_links_{update_id}.json"
        team_link_dict = self.load_raw_data(file_path)

        # Create dict for storing team ids
        tls_dict = {}
        
        for k, v in team_link_dict.items():
            # Store tids for each league_season
            tids = []
            for link in v:
                tids.append(self.parse_team_link(link))
            tls_dict[k] = tids
            print(f"Successfully parsed team ids from team links for league_season: {k}")
            
        return tls_dict

    def parse_team_link(self, team_link):
        """
        Given a team link, parse the team id and return it.
    
        Parameters
        ----------
        team_link : str
            URL for Football Ref team link.
            
        Returns
        -------
        str or None
            Team ID extracted from the provided team link, or None if parsing fails.
        """
        try:
            # Extract team id
            tid = re.findall(r"squads/([\w\d]{8})/", team_link)[0]
            return tid
        except Exception as e:
            # Log Error
            self.logger.error(f"Error parsing team link: {e}")
            return None