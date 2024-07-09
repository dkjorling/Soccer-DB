import pandas as pd
import numpy as np
import re
import datetime as dt
from clean.fbref_clean import FbrefClean

class FbrefPlayers(FbrefClean):
    """
    Parse new player meta-data for new players, clean data, and save to file.

    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning players data from raw data scraped by the FbrefPlayersScraper class

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
        
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='players')
        # Set 'name' attribute specifically for FbrefSeasons entity
        self.primary_key = 'pid'

    # Main Functionality Methods
    def clean_data(self, update_id):
        """
        Clean raw player data retrieved from raw sources.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pd.DataFrame or None
            Cleaned player data in DataFrame format if successful, otherwise None.

        """
        try:
            # Clean player data using players_clean method
            clean_data = self.players_clean(update_id)
            print(f"Successfully cleaned player data for {update_id}!")
            return clean_data
        except Exception as e:
            self.logger.error(f"Could not clean player data for {update_id} due to error: {e}")
            return None
    
    def id_save_all_new_players(self, update_id):
        """
        Identify new players not currently in the players table using the latest TLS update and save to file.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        """
        new_players_df = self.id_all_new_players(update_id)
        file_path = f"data/fbref/players/raw/new_players_pids_df_{update_id}.csv"
        self.save_data(new_players_df, file_path)

    # Helper Methods
    def players_clean(self, update_id):
        """
        Complete process of cleaning player data from raw player information.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pd.DataFrame
            Cleaned player data in the format:
                - pid: Player ID
                - player_name: Player's name
                - dob: Date of birth (in %Y-%m-%d format)
                - birth_city: Player's birth city
                - birth_country: Player's birth country
                - nationality: Player's nationality
                - height: Player's height in cm
                - weight: Player's weight in kg
                - photo: URL to player's photo
        """
        # Load raw data dictionary
        file_path = f"data/fbref/players/raw/new_players_dict_{update_id}.json"
        new_players_dict = self.load_raw_data(file_path)
        
        # Convert dictionary to DataFrame
        new_players_df = pd.DataFrame(columns=['pid', 'player_name', 'dob', 'birth_city', 'nationality', 'height', 'photo'])
        ii = 0
        for k in new_players_dict.keys():
            player_info = []
            player_info.append(k)  # Append pid first
            for kk in new_players_dict[k].keys():
                player_info.append(new_players_dict[k][kk])
            new_players_df.loc[ii] = player_info
            ii += 1
        
        # Clean height, weight, birth city, birth country, and date of birth columns
        new_players_df['weight'] = new_players_df.height.apply(self.clean_players_htwt, return_value='wt')
        new_players_df.height = new_players_df.height.apply(self.clean_players_htwt, return_value='ht')
        new_players_df['birth_country'] = new_players_df.birth_city.apply(self.clean_players_birthplace, return_value='country')
        new_players_df.birth_city = new_players_df.birth_city.apply(self.clean_players_birthplace, return_value='city')
        new_players_df.dob = new_players_df.dob.apply(self.clean_players_dob)
        
        return new_players_df


    def id_all_new_players(self, update_id):
        """
        Identify players that are currently not in the table from the prior TLS update.

        This method compares the player IDs in the provided TLS update file (`ptls_{update_id}.csv`) 
        with the player IDs already existing in the 'players' table stored in `self.data_dict`.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame or None
            A DataFrame containing the details of new player IDs that are not present in the current 'players' table.
            Returns None if there is an error during execution.

        Notes
        -----
        This method assumes that `self.data_dict['players']` contains the current players table.
        If new player IDs are found, they are returned as a DataFrame with columns matching the structure of the 
        TLS update file (`ptls_{update_id}.csv`), including 'pid'.

        """
        try:
            file_path = f"data/fbref/ptls/ptls_{update_id}.csv"
            ptls_update = pd.read_csv(file_path, index_col=0)
            players = self.data_dict['players']

            # Identify new player IDs
            ptls_update_ids = set(ptls_update['pid'].unique())
            current_player_ids = set(players['pid'].tolist())
            new_player_ids = list(ptls_update_ids - current_player_ids)
            
            # Filter TLS update data for new player IDs
            new_players_df = ptls_update[ptls_update['pid'].isin(new_player_ids)].reset_index(drop=True)

            print("Successfully identified new player IDs!")
            return new_players_df
        except Exception as e:
            print(f"Could not identify new player IDs due to error: {e}")
            return None


    

    def clean_players_dob(self, dob_raw):
        """
        Clean player date of birth (DOB) from raw data to convert it to '%Y-%m-%d' format.

        Parameters
        ----------
        dob_raw : str or datetime
            Raw date of birth string in the format '%B %d, %Y' or already in '%Y-%m-%d' format.

        Returns
        -------
        str or datetime
            Cleaned date of birth in '%Y-%m-%d' format, or np.nan if cleaning fails.

        """
        # Check if DOB is already in desired format
        if len(re.findall(r"\d{4}-\d{2}-\d{2}", str(dob_raw))) != 0:
            return dob_raw
        
        try:
            # Clean raw DOB string
            dob_clean = re.sub(r"\n", "", dob_raw)
            dob_clean = dob_clean.strip()
            
            # Parse string to datetime object
            dob_clean = dt.datetime.strptime(dob_clean, '%B %d, %Y')
            
            # Format the date in '%Y-%m-%d' format
            dob_clean = dob_clean.strftime('%Y-%m-%d')
        except:
            dob_clean = np.nan
        
        return dob_clean


    def clean_players_birthplace(self, city_raw, return_value='all'):
        """
        Clean player birthplace from raw data and return the specified value.

        Parameters
        ----------
        city_raw : str
            Raw string containing birthplace information in the format '<city>, <country>'.
        return_value : str, optional
            Specifies the value to return:
            - 'all': Returns a tuple (city, country).
            - 'city': Returns only the cleaned city value.
            - 'country': Returns only the cleaned country value.
            (Default is 'all')

        Returns
        -------
        tuple or str
            If 'return_value' is 'all', returns a tuple (city, country).
            If 'return_value' is 'city' or 'country', returns a string representing city or country.
        
        Raises
        ------
        ValueError
            If 'return_value' is not one of 'all', 'city', or 'country'.

        """
        try:
            splt = city_raw.split(',')
            # Clean country
            country_raw = splt[-1]
            country_clean = re.sub(r"['\[\]\",]", "", country_raw)
            country_clean = re.sub(r"in\s", "", country_clean)
            country_clean = country_clean.strip()
            
            # Clean city
            city_raw = splt[:-1]
            if len(city_raw) == 0:
                city_clean = np.nan
            else:
                city_clean = str(city_raw[0])
                city_clean = re.sub(r"['\[\]\",]", "", city_clean)
                city_clean = re.sub(r"[-]", " ", city_clean)
                city_clean = re.sub(r"in\s", "", city_clean)
                city_clean = city_clean.strip()
            
            # If city and country are equivalent, set city to np.nan
            if city_clean == country_clean:
                city_clean = np.nan
        except:
            city_clean = np.nan
            country_clean = np.nan
    
        # Return based on return_value
        if return_value == 'all':
            return (city_clean, country_clean)
        elif return_value == 'city':
            return city_clean
        elif return_value == 'country':
            return country_clean
        else:
            raise ValueError("return_value must be one of 'all', 'city', 'country'")


    def clean_players_htwt(self, htwt_raw, return_value='all'):
        """
        Clean player height and weight from raw data and return the specified value.

        Parameters
        ----------
        htwt_raw : str
            Raw string containing height and weight information in the format '<height>cm <weight>kg'.
        return_value : str, optional
            Specifies the value to return:
            - 'all': Returns a tuple (height, weight).
            - 'ht': Returns only the cleaned height value.
            - 'wt': Returns only the cleaned weight value.
            (Default is 'all')

        Returns
        -------
        tuple or float
            If 'return_value' is 'all', returns a tuple (height, weight).
            If 'return_value' is 'ht' or 'wt', returns a float representing height or weight.
        
        Raises
        ------
        ValueError
            If 'return_value' is not one of 'all', 'ht', or 'wt'.

        """
        # Clean height
        try:
            ht_raw = re.findall(r"\s*(\d+)cm", htwt_raw)[0]
            ht_clean = float(ht_raw)
        except:
            ht_clean = np.nan
        
        # Clean weight
        try:
            wt_raw = re.findall(r"\s*(\d+)kg", htwt_raw)[0]
            wt_clean = float(wt_raw)
        except:
            wt_clean = np.nan
        
        # Return based on return_value
        if return_value == 'all':
            return (ht_clean, wt_clean)
        elif return_value == 'ht':
            return ht_clean
        elif return_value == 'wt':
            return wt_clean
        else:
            raise ValueError("return_value must be one of 'all', 'ht', 'wt'")


    

    
    