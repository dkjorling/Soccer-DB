import pandas as pd
import numpy as np
from clean.fbref_clean import FbrefClean
from geopy.geocoders import Nominatim
from fuzzywuzzy import fuzz


class FbrefCleanTeams(FbrefClean):
    """
    This class cleans team meta-data fetched from the Rapid API Football API and merges with football-reference team id.

    This class inherits from FbrefClean and provides additional methods specifically
    for cleaning teams meta-data from raw data scraped by the APIScraper class.

    The class also attempts to find an approximate location of each team using Nominatim moduel of geopy.geocoders.

    Main Functionality Methods
    --------------------------
    clean_data(self, update_id):
         Clean raw teams data by updating addresses, obtaining coordinates, and ensuring data integrity.
    id_save_all_new_teams(self, update_id):
        Identify teams that are currently not in the teams table from the latest team league seasons (tls) update,
        and save them to a file.
    match_save_all_teams_to_api(self, update_id):
        Find API id matches for all new team names using data_processing.scrape_save_api_teams_from_all_league_seasons
        output and save to file.
    """
    # Initialization Methods
    def __init__(self):
        # Call parent class (FbrefClean) initialization
        super().__init__(name='teams')
        self.primary_key = 'tid'

    # Main Functionality MEthods
    def clean_data(self, update_id):
        """
        Clean raw teams data by updating addresses, obtaining coordinates, and ensuring data integrity.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        
        Returns
        -------
        pandas.DataFrame
            Cleaned dataframe containing team meta-data
        """
        try:
            # Clean data using class cleaning method
            clean_data = self.teams_clean(update_id)
            print(f"Successfully cleaned team data for {update_id}!")
            return clean_data
        except Exception as e:
            print(f"Could not clean team data for {update_id} due to error: {e}")
            return None

    def match_save_all_teams_to_api(self, update_id):
        """
        Find API id matches for all new team names using data_processing.scrape_save_api_teams_from_all_league_seasons
        output and save to file.
        
        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        """
        try:
            # Extract Fbref to API mapping dictionary
            fbref_to_api_dict = self.match_all_teams_to_api(update_id)

            # Save dict to file
            file_path = f"data/fbref/teams/temp/new_team_api_dict_{update_id}.json"
            self.save_data(fbref_to_api_dict, file_path)
            # Log success
            self.logger.info(f"Successfully matched and saved new teams to api ids for date: {update_id}!")
        except Exception as e:
            # Log Error
            self.logger.error(f"Could not match and save new teams to api ids for date: {update_id} due to error: {e}")

    def id_save_all_new_teams(self, update_id):
        """
        Identify teams that are currently not in the teams table using the latest team league seasons (tls) update,
        and save them to a file.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.
        """
        # Identify all new teams
        new_teams_df = self.id_all_new_teams(update_id)

        # Save data fo file
        file_path = f"data/fbref/teams/raw/new_teams_df_{update_id}.csv"
        self.save_data(new_teams_df, file_path)
    
    # Helper Methods
    def teams_clean(self, update_id):
        """
        Clean raw teams data by updating addresses, obtaining coordinates, and ensuring data integrity.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        pandas.DataFrame
            Cleaned dataframe containing team data with latitude ('lat') and longitude ('lon') columns.

        """
        # Load raw data
        file_path = f"data/fbref/teams/raw/teams_raw_{update_id}.csv"
        raw_data = self.load_raw_data(file_path)

        # Get robust addresses
        clean_data = self.update_all_addresses(raw_data)
        # Get coordinates and create latitude and longitude columns
        coords = clean_data.address.apply(self.get_lats_lons)
        lats = [c[0] for c in coords]
        lons = [c[1] for c in coords]
        clean_data['lat'] = lats
        clean_data['lon'] = lons
        # Drop address column which is only used to get coordinates
        clean_data = clean_data.drop(columns=['address'])

        # Drop tid na values and duplicate tids
        clean_data = clean_data.dropna(subset=['tid'])
        clean_data = clean_data.drop_duplicates(subset=['tid'])
        return clean_data

    def id_all_new_teams(self, update_id):
        """
        Identify teams that are currently not in the teams table from the latest team league seasons (tls) update.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        DataFrame or None
            Returns a DataFrame containing the new team IDs if successful, or None if an error occurs.
        """
        try:
            # Load latests tls update
            file_path = f"data/fbref/tls/tls_{update_id}.csv"
            tls_update = pd.read_csv(file_path, index_col=0)

            # Load teams table
            teams = self.data_dict['teams']

            # Identify new team ids
            tls_update_ids = list(set(tls_update.tid)) # get unique team ids from update
            new_team_tids = [x for x in tls_update_ids if x not in list(teams.tid)]
            new_teams_df = tls_update[tls_update.tid.isin(new_team_tids)].reset_index(drop=True)
            print("Successfully identified new tids!")
            return new_teams_df
        except Exception as e:
            self.logger.error(f"Could not identify new tids due to error: {e}")
            return None

    def match_team_to_api(self, fbref_team_name, api_teams):
        """
        Given a team name scraped from Football Reference and a list of API team names from the same league season,
        use the fuzzywuzzy library to return the best match from the list of team names.

        Parameters
        ----------
        fbref_team_name : str
            The team name scraped from Football Reference.
        api_teams : list of str
            List of API team names from the overlapping league season as the Football Reference team.

        Returns
        -------
        tuple
            A tuple containing the closest matched name and its associated index.
        """
        # Initialize empty list to store simlarity scores
        similarity_ratios = []

        # Iterate through each potential API team match
        for api_team in api_teams:
            try:
                # Calculate simlarity ratio and append
                similarity_ratios.append(fuzz.ratio(fbref_team_name, api_team))
                print("Successfully found api team match!")
            except Exception as e:
                print(f"Could not find api team match due to error: {e}")
        
        # Identify the max score and get the name and index of the score
        max_score = max(similarity_ratios)
        match_index = similarity_ratios.index(max_score)
        match_name = api_teams[match_index]
        return match_name, match_index

    def match_all_teams_to_api(self, update_id):
        """
        Find API ID matches for all new team names. Takes output from `data_processing.scrape_save_api_teams_from_all_league_seasons` function.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Returns
        -------
        dict
            A dictionary mapping each Football Reference team name to its corresponding API ID and Football Reference ID.

        """
        # Load new team names dict which is output from data_processing.scrape_save_api_teams_from_all_league_seasons
        file_path = f"data/fbref/teams/raw/new_team_names_dict_{update_id}.json"
        new_team_names_dict = self.load_raw_data(file_path)
        
        # Initialize dictionary to store mappings
        fbref_to_api_dict = {}
        
        # Iterate through each league-season in the new team names dictionary
        for ls in new_team_names_dict.keys():
            try:
                # Iterate through each Football Reference team name and its corresponding IDs
                for i, fbref_team_name in enumerate(new_team_names_dict[ls]['fbref_team_names']):
                    fbref_to_api_dict[fbref_team_name] = {}
                    fbref_to_api_dict[fbref_team_name]['fbref_tid'] = new_team_names_dict[ls]['fbref_tid'][i]  # Store tid in dict
                    
                    # Check if API IDs exist for the current league-season
                    if len(new_team_names_dict[ls]['api_ids']) == 0:
                        fbref_to_api_dict[fbref_team_name]['api_id'] = np.nan
                        print(f"Could not find a match for fbref team name: {fbref_team_name} because no API IDs were found.")
                    else:
                        # Create dictionary with API IDs as keys and names as values, excluding names already in production teams table
                        api_dict = dict(zip(new_team_names_dict[ls]['api_ids'], new_team_names_dict[ls]['api_team_names']))
                        keep_ids = [x for x in api_dict.keys() if x not in list(self.db_table['api_id'])]
                        keep_names = [api_dict[x] for x in keep_ids]
                        
                        # Find the best match for the Football Reference team name among the API team names
                        match_name, match_index = self.match_team_to_api(fbref_team_name, keep_names)
                        
                        # Get the API ID corresponding to the best match
                        api_id = keep_ids[match_index]
                        
                        # Store the API ID in fbref_to_api_dict
                        fbref_to_api_dict[fbref_team_name]['api_id'] = api_id
                        print(f"Successfully found a match for fbref team name: {fbref_team_name}. Match: {match_name}")
            except Exception as e:
                fbref_to_api_dict[fbref_team_name]['api_id'] = np.nan
                print(f"Could not find a match for fbref team name: {fbref_team_name} due to error: {e}")
        
        return fbref_to_api_dict


    def get_full_address(self, address_raw, api_id, venue, city, country):
        """
        Create more robust addresses to search with geolocator.

        Parameters
        ----------
        address_raw - str
            String representing raw address
        api_id - int
            Team API ID
        venue - str
            String representing stadium name
        city - str
            String representing city in which team is located
        country - str
            String representing country in which team is located
        
        Returns
        -------
        str
            Robust address
        """
        # If there is no api ID then just return back the raw adddress
        if (api_id == 0) | pd.isna(api_id):
            return address_raw
        # If there is no address, return a combination of venue, city and country if they exit
        elif (address_raw == None) | (type(address_raw) == float):
            if (venue == None) | (city == None):
                return address_raw
            else:
                address_full = str(venue) + " " + str(city) + ", " + str(country)
                return address_full
        # If there is an address, return combo of address, venue, city and country
        else:
            address_full = str(address_raw) + " " + str(venue) + " " + str(city) + ", " + str(country)
            return address_full

    def update_all_addresses(self, df_team_info):
        """
        Takes raw teams data output from data_processing.scrape_save_all_api_team_info and updates address column.
        This method uses the get_full_address to extract a new address.

        Parameters
        ----------
        df_team_info - pandas.DataFrame
            Dataframe containing team meta-data
        
        Returns
        -------
        pandas.DataFrame
            Returns the input dataframe with an updated address column
        """
        # Store new addresses in list
        full_addresses = []
        try:
            # Iterate through dataframe
            for _, row in df_team_info.iterrows():
                print(row.tid)
                full_addresses.append(self.get_full_address(row.address, row.api_id, row.venue, row.city, row.country))
            df_team_info['address'] = full_addresses
            return df_team_info
        except Exception as e:
            self.logger.error(f"Could not update team addresses for teams data due to error: {e}")

    def get_lats_lons(self, address):
        """
        Uses geolocator to get latitude and longitude of team stadium or city

        Parameters
        ----------
        address - str
            String representing a team's stadium or city

        Returns
        -------
        tuple
            Returns a tuple representing coordinates of team stadium or city
        """
        # Initialize geolocator
        geolocator = Nominatim(user_agent="my_geocoder")
        # Get coordinates
        location = geolocator.geocode(address)
        if (not pd.isna(address)) & (location != None):
            lat = geolocator.geocode(address).latitude
            lon = geolocator.geocode(address).longitude
        else:
            lat = np.nan
            lon = np.nan
        return lat, lon
        
    

    