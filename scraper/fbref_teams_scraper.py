from scraper.fbref_scraper import FbrefScraper
import json

class FbrefTeamsScraper(FbrefScraper):
    """
    Scrapes team name given a football reference team id.
    
    The main purpose of this class is to extract a team name to attempt to match to Football API's database.
    The football API database contains much more information regarding team meta-data than football reference does.
    The team names extracted by this function are used to match against a team name in the API database.
    The team names are then joined together to obtain more meta-data for each team.

    This class should be used to update data after team-league-season data has updated. 

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping teams data from FBref.

    Main Functionality Methods
    --------------------------
    scrape_team_name(self, tid):

    """
    # Initilaization Methods
    def __init__(self):
        # Call parent class (FbrefScraper) initialization
        super().__init__(name='teams')

    # Main Functionality Methods
    def scrape_team_name(self, tid):
        """
        Given team id, get team name from football reference

        Parameters
        ----------
        tid : str
            football reference team id

        Returns
        -------
        str
            Returns team name
        """
        # Format URL
        url = f"https://fbref.com/en/squads/{tid}/"
        try:
            # Get Data and extract team name
            soup = self.scrape_data_requests(url)
            script_tag = soup.find('script', {'type': 'application/ld+json'})
            json_data = json.loads(script_tag.string)
            team_name = json_data['name']
            return team_name
        except Exception as e:
            # Log Error
            self.logger.error(f"Could not scrape team name for tid: {tid} due to error: {e}")
            return None

            
        
        
        
        
            
            
        