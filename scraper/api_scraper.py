import pandas as pd
import requests
import time
from logging_config import logger
import decouple
from utils.helpers import load_data

class ApiScraper():
    """
    This class scrapes data from the Rapid API Football API.

    The main purpose of this class is to scrape team meta-data that is joined to the fbref teams dataframe by team name.

    Main Functionality Methods
    --------------------------
    get_team_info(self, tid_api):
        Fetch and parse relevant team meta-data given a Rapid API team id
    get_teams_from_league_season(self, lg_api, season_api):
        Fetches and parses all API team IDs and names given API league ID and API season ID.
    
    """
    # Initialization Methods
    def __init__(self):
        self.logger = logger
        self.set_api_headers()
        self.data_dict = load_data()

    def set_api_headers(self):
        """
        Set API Parameters needed to scrape Rapid API Football API.

        This method sets the headers attribute for the class. 

        It extracts the headers from the .env file in the directory. 
        """
        # Set up configuration to extract API parameters from .env file
        config = decouple.AutoConfig(' ')
        key = config('RAPIDAPIKEY')
        host = config('RAPIDAPIHOST')
        headers = {
            "X-RapidAPI-Key": key,
            "X-RapidAPI-Host": host
        }
        self.headers = headers
    
    # Main Functionality Methods
    def get_team_info(self, tid_api):
        """
        Fetch and parse relevant team meta-data given a Rapid API team id

        Parameters
        ----------
            tid_api : int
                API team id
        
        Returns
        -------
        list
            list containing desired team meta-data

        """
        data = self.fetch_team_info(tid_api)
        try:
            team_info = self.parse_team_info(data)
            print(f"Successfully parsed api team data for team: {tid_api}!")
            return team_info
        except Exception as e:
            self.logger.error(f"Could not parse api team data for team: {tid_api} due to error: {e}")
    
    def get_teams_from_league_season(self, lg_api, season_api):
        """
        Fetches and parses all API team IDs and names given API league ID and API season ID.

        Parameters
        ----------
        lg_api : int
            The API league ID.
        season_api : int
            The API season ID.

        Returns
        -------
        tuple
            A tuple containing two lists:
            - api_ids : list
                List of API team IDs.
            - api_names : list
                List of API team names.

        Notes
        -----
        - Constructs a URL using lg_api and season_api to fetch team data from an API.
        - Uses fetch_api method to retrieve data from the constructed URL.
        - Iterates through the fetched data to extract team IDs and names.
        """
        # Format URL used to get all teams for given league and season
        url = f"https://api-football-v1.p.rapidapi.com/v3/teams?league={lg_api}&season={season_api}"
        # Fetch Data
        data = self.fetch_api(url)

        # Iterate therough data to parse api_ids and api_names
        api_ids = []
        api_names = []
        for i in range(len(data)):
            api_ids.append(data[i]['team']['id'])
            api_names.append(data[i]['team']['name'])
        return api_ids, api_names
    
    # Helper Methods
    def fetch_api(self, url):
        """
        General fetch function from api. Returns data from request.

        Parameters
        ----------
        url - str
            URL used to access Rapid API Football Reference API 
        
        Returns
        -------
        dict
            Data in dictionary form from request
        """
        try:
            # Get Data
            response = requests.get(url, headers=self.headers)
            # Retry after one minute if response does not work as intended
            if response.status_code != 200:
                time.sleep(60)
                try:
                    response = requests.get(url, headers=self.headers)
                except Exception as e:
                    print(e)
            data = response.json()['response']
            print(f"Successfully fetched data from API for url: {url}!")
            return data
        except Exception as e:
            # Log Error
            self.logger.error(f"Could not fetch data from API for url: {url} due to error: {e}")
            return None

    def save_raw_data(self, raw_data, file_path):
        """
        Saves raw data to a specified file path.

        Parameters
        ----------
        raw_data : DataFrame or dict
            The raw data to be saved. It can be either a pandas DataFrame or a dictionary.
        file_path : str
            The file path where the data will be saved.

        Returns
        -------
        bool
            True if the data is successfully saved, False otherwise.

        Notes:
        ------
        - If raw_data is a pandas DataFrame, it will be saved as a CSV file.
        - If raw_data is a dictionary, it will be saved as a JSON file.
        - Prints a success message upon successful save.
        - Logs errors encountered during the save operation.
        """
        try:
            if isinstance(raw_data, pd.DataFrame):
                # Data is a pandas DataFrame, save it to a CSV file
                raw_data.to_csv(file_path)
                print(f"DataFrame saved to {file_path}")
                return True
            elif isinstance(raw_data, dict):
                # Data is a dictionary, save it to a JSON file
                with open(file_path, 'w') as f:
                    f.write(json.dumps(raw_data))
                print(f"Dictionary saved to {file_path}")
                return True
            else:
                # Unsupported data type
                print("Unsupported data type. Only pandas DataFrame or dictionary is supported.")
                return False
        except Exception as e:
            self.logger.error(f"Error saving data to {file_path}: {e}")
            return False

    def fetch_team_info(self, tid_api):
        """
        Fetch and parse relevant team info given an api team id
        
        Parameters
        ----------
            tid_api : int
                API team id
        
        Returns
        -------
        dict
            Dictionary containing meta-data for specific team
        """
        # Format URL used to get raw team meta data
        url = f"https://api-football-v1.p.rapidapi.com/v3/teams?id={str(int(tid_api))}"
        try:
            # Fetch Data
            data = self.fetch_api(url)[0] # take 0 index
            print(f"Successfully fetched api team data for team: {tid_api}!")
            return data
        except Exception as e:
            print(f"Could not fetch api team data for team: {tid_api} due to error: {e}")
            return None

    def parse_team_info(self, tid_api_data):
        """
        Parse team metadata from raw API data structure.

        Parameters
        ----------
        tid_api_data : dict
            Dictionary containing raw data for a singular team.

        Returns
        -------
        list
            A list containing desired team metadata in the following order:
            - code : str
                Team code.
            - country : str
                Country of the team.
            - city : str
                City where the team's venue is located.
            - venue : str
                Name of the team's venue.
            - capacity : int
                Capacity of the team's venue.
            - logo_url : str
                URL of the team's logo.
            - venue_url : str
                URL of an image of the team's venue.
            - address : str
                Address of the team's venue.

        Notes
        -----
        - Extracts team code, country, city, venue name, venue capacity, logo URL, venue image URL,
        and venue address from the provided tid_api_data dictionary.
        - Returns these extracted values as a list in the specified order.

        """
        # Extract Meta Data
        code = tid_api_data['team']['code']
        country = tid_api_data['team']['country']
        city = tid_api_data['venue']['city']
        venue = tid_api_data['venue']['name']
        capacity = tid_api_data['venue']['capacity']
        logo_url = tid_api_data['team']['logo']
        venue_url = tid_api_data['venue']['image']
        address = tid_api_data['venue']['address']
        return [code, country, city, venue, capacity, logo_url, venue_url, address]
            
    

                  
        
        
        
    

    

    
        
            
            
                
                
        
    

    

    