import pandas as pd
import numpy as np
from clean.fbref_clean import FbrefClean

class FbrefCleanSeasons(FbrefClean):
    """
    Parse new league seasons from all league seasons, clean data, and save to file.

    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning and updating seasons data from raw data scraped by the FbrefLeagueSeasonsScraper class

    This class should be used after new league season data has been cleaned and updated.

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
        Extract and sort Extract and sort all seasons from new league seasons  
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='seasons')
        self.primary_key = 'season'

    # Main Class Functionality Methods
    def clean_data(self, update_id=None):
        """
        Extract and sort all seasons from new league seasons  

        Parameters
        ----------
        update_id : str; optional
            The identifier corresponding to the update date and run number; defaults to None

        Returns
        -------
        pd.DataFrame
            Dataframe containing new seasons data

        """
        # Implement seasons_clean method
        clean_data = self.seasons_clean()
        return clean_data

    # Helper Methods
    def seasons_clean(self, update_id=None):
        """
        Extract and sort all seasons from new league seasons  

        Parameters
        ----------
        update_id : str; optional
            The identifier corresponding to the update date and run number; defaults to None

        Returns
        -------
        pd.DataFrame
            Dataframe containing new seasons data

        """
        # Load production league seasons data
        ls = self.data_dict['ls']
        # Extract seasons not in table
        seasons_new = pd.DataFrame(data={'season': np.sort(list(set(ls.season)))})
        return seasons_new

    