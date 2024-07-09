import pandas as pd
import numpy as np
import time
import re
from scraper.fbref_scraper import FbrefScraper

class FbrefPlayersScraper(FbrefScraper):
    """
    Scrapes football reference player meta-data

    This class inherits from FbrefScraper and provides additional methods specifically
    for scraping player meta-data from FBref.

    Main Functionality Methods
    --------------------------
    scrape_save_all_new_players_info(self, update_id):
        Scrape player information for all new players based on their player IDs and save to file.
    scrape_player_info(self, pid):
        Scrape and extract meta-data for a football reference player using their player ID.

    Notes
    -----
    Meta-Data includes:
    1. name
    2. date of birth
    3. birth city
    4. nationality
    5. height
    6. weight
    7. player photo link
    """
    def __init__(self):
        # Call parent class (FbrefScraper) initialization
        super().__init__(name='players')
    
    # Main Functionality Methods
    def scrape_save_all_new_players_info(self, update_id):
        """
        Scrape player information for all new players based on their player IDs and save to file.

        Parameters
        ----------
        update_id : str
            Identifier corresponding to the update date and run number.

        """
        try:
            # Scrape new player meta-data
            new_players_dict = self.scrape_all_new_players_info(update_id)

            # Save dict to file
            file_path = f"data/fbref/players/raw/new_players_dict_{update_id}.json"
            self.save_raw_data(new_players_dict, file_path)
        except Exception as e:
            self.logger.error(f"Could not scape and save new players info for data from: {update_id} due to errorL{e}")

    def scrape_player_info(self, pid):
        """
        Scrape and extract meta-data for a football reference player using their player ID.

        Parameters
        ----------
        pid : str
            Football reference player ID.

        Returns
        -------
        dict or None
            Dictionary containing raw player information:
                - 'name': Player's full name.
                - 'dob': Player's date of birth.
                - 'birth_city': Player's birth city.
                - 'nat': Player's nationality.
                - 'htwt': Player's height/weight string.
                - 'photo': URL of player's photo.

            Returns None if information retrieval fails.
        """
        # Define URL for scraping player information
        url = f"https://fbref.com/en/players/{pid}/"

        # Initialize dictionary to store player information
        player_info_dict = {
            'name': np.nan,
            'dob': np.nan,
            'birth_city': np.nan,
            'nat': np.nan,
            'htwt': np.nan,
            'photo': np.nan
        }

        try:
            # Scrape data and parse to BeautifulSoup object
            soup = self.scrape_data_requests(url)
            player_info = soup.find('div', {'class': 'players', 'id': 'info'})

            # Extract player name
            pname = player_info.find('span').text.strip()
            player_info_dict['name'] = pname

            # Extract additional player information
            strong_texts = [x.text for x in player_info.find_all('strong')]

            # Date of birth and birth city
            if 'Born:' in strong_texts:
                try:
                    birth_tag = player_info.find('span', {'data-birth': True, 'id': 'necro-birth'})
                    dob = birth_tag['data-birth']
                    birth_city_tag = birth_tag.find_next('span').text.strip()
                    if 'in' in birth_city_tag:
                        birth_city = re.findall(r'in (.+)', birth_city_tag)[0]
                        player_info_dict['birth_city'] = birth_city
                except Exception as e:
                    dob = player_info.find('strong', string='Born:').find_next('span').text.strip()
                player_info_dict['dob'] = dob

            # Nationality
            if 'National Team:' in strong_texts:
                nationality = player_info.find_all('strong', string='National Team:')[0].find_next('a').text.strip()
            elif 'Youth National Team:' in strong_texts:
                nationality = player_info.find_all('strong', string='Youth National Team:')[0].find_next('a').text.strip()
            elif 'Citizenship:' in strong_texts:
                nationality = player_info.find_all('strong', string='Citizenship:')[0].find_next('a').text.strip()
            else:
                nationality = np.nan
            player_info_dict['nat'] = nationality

            # Height/Weight
            ps = player_info.find_all('p')
            for p in ps:
                if re.search(r"\d+cm", p.text):
                    player_info_dict['htwt'] = p.text.strip()
                    break

            # Player photo URL
            try:
                photo = player_info.find('img')['src']
                player_info_dict['photo'] = photo
            except Exception as e:
                pass

            print(f"Successfully scraped player info for pid: {pid}")
            return player_info_dict

        except Exception as e:
            self.logger.error(f"Could not scrape player info for pid: {pid} due to error: {e}")
            return None

    # Helper Methods
    def scrape_all_new_players_info(self, update_id):
        """
        Scrape player meta-data for all new players based on their player IDs.

        Parameters
        ----------
        update_id : str
            Identifier corresponding to the update date and run number.

        Returns
        -------
        dict
            Dictionary containing player meta-data for all new players, where keys are player IDs and values
            are dictionaries with player information (name, date of birth, birth city, nationality, height/weight string, photo).
        """
        # Load new players player id dataframe
        file_path = f"data/fbref/players/raw/new_players_pids_df_{update_id}.csv"
        new_players_df = pd.read_csv(file_path, index_col=0)

        # Get number of ids for progress check and initialize scrape times list
        num_items = new_players_df.shape[0]
        scrape_times = []

        # Create empty dictioanry to store data in
        new_players_dict = {}

        # Iterate through new players
        for _, row in new_players_df.iterrows():
            # Begin Timer
            start_time = time.time()
            new_players_dict[row.pid] = self.scrape_player_info(row.pid)
            time.sleep(6) # fbref scraping limit
            # End Timer
            end_time = time.time()
            # Append scrape time
            scrape_times.append(end_time - start_time)
            # Calculate and print estimated time left
            est_time_left = np.mean(scrape_times) * (num_items - i - 1)
            print(f"Successfully Scraped player meta-data for {row.pid}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
        return new_players_dict

    
            
        
            
        
        

    

    