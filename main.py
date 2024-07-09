import scraper
import clean
import sys
from upload import upload
from utils import data_processing
from logging_config import logger



# Set Logger

###
class SoccerDBWorkflowController:
    """
    Controls the workflow for updating the soccer database
    """
    def __init__(
        self,
        update_id,
        ls_reference,
        last_date,
        old_match_reference_date,
        new_match_reference_date,
        is_new_ls
        ):

        self.logger = logger
        self.update_id = update_id
        self.ls_reference = ls_reference
        self.last_date = last_date
        self.old_match_reference_date = old_match_reference_date
        self.new_match_reference_date = new_match_reference_date
        self.is_new_ls = is_new_ls

        self.steps = [
            ("seasons", update_seasons, [self.update_id, self.last_date]),
            ("ls", update_ls, [self.update_id, self.ls_reference, self.last_date]),
            ("tls", update_tls, [self.update_id, self.last_date]),
            ("teams", update_teams, [self.update_id, self.last_date]),
            ("ptls", update_ptls, [self.update_id, self.last_date]),
            ("players", update_players, [self.update_id, self.last_date]),
            ("matches", update_matches, [self.update_id, self.old_match_reference_date, self.new_match_reference_date, self.is_new_ls]),
            ("team_matches", update_team_matches, [self.update_id, self.old_match_reference_date, self.new_match_reference_date, self.is_new_ls]),
            ("player_matches", update_player_matches, [self.update_id, self.old_match_reference_date, self.new_match_reference_date]),
            ("keeper_matches", update_keeper_matches, [self.update_id, self.new_match_reference_date]),
        ]

    def run_workflow(self, start_from=None, additional_args={}):
        start_index = 0
        if start_from:
            start_index = next((index for index, (name, _, _) in enumerate(self.steps) if name == start_from), 0)

        for name, func, args in self.steps[start_index:]:
            self.logger.info(f"Starting step: {name}")
            try:
                func(*args, **additional_args.get(name, {}))
                self.logger.info(f"Completed step: {name}")
            except Exception as e:
                self.logger.error(f"Error in step: {name} - {e}")

################## Individual Table Update Functions ##################
def update_seasons(update_id, last_date):
    """
    Update Seasons
    """
    # Set up Logger
    logger.info(f"Updating seasons for update id: {update_id}")

    # Initialize cleaner and uoploader
    seasons_cleaner = clean.fbref_clean_seasons.FbrefCleanSeasons()
    seasons_uploader = upload.UploadSoccerDB(name='seasons')

    # Clean Seasons
    seasons_cleaner.clean_save_update_data(last_date=last_date, update_id=update_id) # use same last data as league_seasons, no update_id

    # Upload Seasons
    seasons_uploader.upload_clean_data(update_id)

def update_ls(update_id, ls_reference, last_date):
    """
    Update league-seasons
    """
    # Set up logger
    logger.info(f"Updating league-seasons for update id: {update_id}")

    # Initialize scraper, cleaner, uploader
    ls_scraper = scraper.fbref_ls_scraper.FbrefLeagueSeasonsScraper()
    ls_cleaner = clean.fbref_clean_ls.FbrefCleanLeagueSeasons()
    ls_uploader = upload.UploadSoccerDB(name='ls')

    # Check if user wants to scrape data 
    user_input = input(f"Do you want to procede with scraping new league-season data? (yes/no): ").lower()
    if user_input == 'yes':
        # iterate through all leagues and scrape all seasons for each league
        ls_scraper.scrape_and_save_all_league_seasons(update_id=update_id)

        # Get new league seasons from raw data and save
        ls_cleaner.get_and_save_new_league_seasons(update_id=update_id)

        # Load in data to get league starts and end from scraper
        new_league_seasons = ls_cleaner.load_raw_data(f"data/fbref/ls/temp/new_league_seasons_only_{update_id}.csv")

        # Update and save new league season starts and ends
        data_processing.scrape_save_new_league_season_start_end(ls_scraper, new_league_seasons, update_id)

        # Update and save new league season advance stat status
        data_processing.check_save_new_league_season_adv_stats(ls_scraper, new_league_seasons, update_id)

    # Update and save all league-season start and end dates
    user_input = input(f"Do you want to procede with updating league starts and ends? (yes/no): ").lower()
    if user_input == 'yes':
        ls_scraper.update_lg_start_end(ls_reference, update_id)
    
    # Save updategd league-seasons
    ls_cleaner.save_updated_league_season_start_end(update_id)
    # update new league seasons and save to table:
    ls_cleaner.clean_save_update_data(last_date=last_date, update_id=update_id,)

    # Upload ls to Database
    ls_uploader.upload_clean_data(update_id)

def update_tls(update_id, last_date):
    """
    Update Team-League-Seasons
    """
    # Set up Logger
    logger.info(f"Updating team-league-seasons for update id: {update_id}")

    # Initialize scraper, cleaner, and uploader
    tls_scraper = scraper.fbref_tls_scraper.FbrefTeamLeagueSeasonsScraper()
    tls_cleaner = clean.fbref_clean_tls.FbrefCleanTeamLeagueSeasons()
    tls_uploader = upload.UploadSoccerDB(name='tls')

    # Check if user wants to scrape data 
    user_input = input(f"Do you want to procede with scraping team-league-season data? (yes/no): ").lower()
    if user_input == 'yes':
        # Scrape team links for new league seasons and save to raw data
        tls_scraper.scrape_save_all_new_team_links(update_id)
   
    # Update new team league seasons and save to table
    tls_cleaner.clean_save_update_data(last_date=last_date, update_id=update_id)

    # Upload tls to Database
    tls_uploader.upload_clean_data(update_id)

def update_teams(update_id, last_date):
    """
    Update Teams
    """
    # Set up Logger
    logger.info(f"Updating teams for update id: {update_id}")

    # Initialize Scrapers, Cleaner and Uploader
    teams_scraper = scraper.fbref_teams_scraper.FbrefTeamsScraper()
    teams_cleaner = clean.fbref_clean_teams.FbrefCleanTeams()
    api_scraper = scraper.api_scraper.ApiScraper()
    teams_uploader = upload.UploadSoccerDB(name='teams')

    # Identify new team ids and save to dataframe
    teams_cleaner.id_save_all_new_teams(update_id)

    # Ask user if wants to scrape new teams
    user_input = input(f"Do you want to procede with scraping teams data? (yes/no): ").lower()
    if user_input == 'yes':
        # Scrape new team names using the ids from above
        data_processing.scrape_save_new_team_names(teams_scraper, update_id)
    
    # Ask user if want to scrape new teams data from API
    user_input = input(f"Do you want to procede with scraping teams data from API? (yes/no): ").lower()
    if user_input == 'yes':
        # Use new teams' league seasons to merge with api league seasons and try to find team api id
        data_processing.scrape_save_api_teams_from_all_league_seasons(api_scraper, update_id)

    # Find best matching api_id and save
    teams_cleaner.match_save_all_teams_to_api(update_id)

    # Get teams info from Api and save as a dataframe
    data_processing.scrape_save_all_api_team_info(api_scraper, update_id)

    # Clean data and save to tablee
    teams_cleaner.clean_save_update_data(last_date=last_date, update_id=update_id)

    # Upload teams to Database
    teams_uploader.upload_clean_data(update_id)
    
def update_ptls(update_id, last_date):
    """
    Update player-team-league-seasons
    """
    # Set up Logger
    logger.info(f"Updating player-team-league-seasons for update id: {update_id}")

    # Initialize Scraper, Cleaner and Uploader
    ptls_scraper = scraper.fbref_ptls_scraper.FbrefPlayerTeamLeagueSeasonsScraper()
    ptls_cleaner = clean.fbref_clean_ptls.FbrefCleanPlayerTeamLeagueSeasons()
    ptls_uploader = upload.UploadSoccerDB(name='ptls')

    # Prompt user if want to scrape ptls data
    user_input = input(f"Do you want to procede with scraping ptls data? (yes/no): ").lower()
    if user_input == 'yes':
        # Scrape and save player ids
        ptls_scraper.get_save_all_pids(update_id)

    # Clean ptls, save, update table and save
    ptls_cleaner.clean_save_update_data(last_date=last_date,update_id=update_id)

    # Upload ptls to Database
    ptls_uploader.upload_clean_data(update_id)

def update_players(update_id, last_date):
    """
    Update players
    """
    # Set up Logger
    logger.info(f"Updating players for update id: {update_id}")

    # Initialize Scraper, Cleaner and Uploader
    players_scraper = scraper.fbref_players_scraper.FbrefPlayersScraper()
    players_cleaner = clean.fbref_clean_players.FbrefPlayers()
    players_uploader = upload.UploadSoccerDB(name='players')

    # Identify new player ids and save to dataframe
    players_cleaner.id_save_all_new_players(update_id)

    # Prompt user if wants to procede scraping data or not
    user_input = input(f"Do you want to procede with scraping players data? (yes/no): ").lower()
    if user_input == 'yes':
        # scrape and save raw players info
        players_scraper.scrape_save_all_new_players_info(update_id)

    # clean players, save, update table and save
    players_cleaner.clean_save_update_data(last_date=last_date,update_id=update_id)

    # Upload players to Database
    players_uploader.upload_clean_data(update_id)

def update_matches(update_id, old_match_reference_date, new_match_reference_date, is_new_ls):
    """
    Update Matches
    """
    # Set up Logger
    logger.info(f"Updating matches for update id: {update_id}")

    # Initialize Scraper, Cleaner and Uploader
    matches_scraper = scraper.fbref_matches_scraper.FbrefMatchesScraper(is_new_ls)
    matches_cleaner = clean.fbref_clean_matches.FbrefCleanMatches()
    matches_uploader = upload.UploadSoccerDB(name='matches')

    # Prompt user if wants to procede scraping data or not
    user_input = input(f"Do you want to procede with scraping matches data? (yes/no): ").lower()
    if user_input == 'yes':
        # Ccrape new matches
        matches_scraper.get_and_save_new_matches(update_id, old_match_reference_date) # use old match_reference_date

    # Clean matches, save, update table and save
    matches_cleaner.clean_save_update_data(last_date=new_match_reference_date, update_id=update_id) # use new match_reference_date

    # Upload matches to Database
    matches_uploader.upload_clean_data(update_id)

def update_team_matches(update_id, old_match_reference_date, new_match_reference_date, is_new_ls):
    """
    Update team matches
    """
    # Set up Logger
    logger.info(f"Updating team-matches for update id: {update_id}")

    # Initialize Scraper, Cleaner and Uploader
    team_matches_scraper = scraper.fbref_team_matches_scraper.FbrefTeamMatchesScraper(is_new_ls)
    team_matches_cleaner = clean.fbref_clean_team_matches.FbrefCleanTeamMatches(
        old_match_reference_date=old_match_reference_date,
        new_match_reference_date=new_match_reference_date
    )
    team_matches_uploader = upload.UploadSoccerDB(name='team_matches')

    # Prompt user if wants to procede scraping data or not
    user_input = input(f"Do you want to procede with scraping matches data? (yes/no): ").lower()
    if user_input == 'yes':
        # scrape new team stats
        team_matches_scraper.scrape_save_new_team_match_stats(update_id, old_match_reference_date) # use old match ref 

    # Clean team matches, save, update table and save
    team_matches_cleaner.clean_save_update_data(last_date=new_match_reference_date, update_id=update_id) # use new match_reference_date

    # Upload team_matches to Database
    team_matches_uploader.upload_clean_data(update_id)

def update_player_matches(update_id, old_match_reference_date, new_match_reference_date):
    """
    Update player matches
    """
    # Set up Logger
    logger.info(f"Updating player-matches for update id: {update_id}")

    # Initialize Scraper, Cleaner and Uploader
    player_matches_scraper = scraper.fbref_player_matches_scraper.FbrefPlayerMatchesScraper()
    player_matches_cleaner = clean.fbref_clean_player_matches.FbrefCleanPlayerMatches(
        old_match_reference_date=old_match_reference_date,
        new_match_reference_date=new_match_reference_date
    )
    player_matches_uploader = upload.UploadSoccerDB(name='player_matches')

    # Prompt user if wants to procede scraping data or not
    user_input = input(f"Do you want to procede with scraping player matches data? (yes/no): ").lower()
    if user_input == 'yes':
        # scrape new player stats
        player_matches_scraper.scrape_save_new_player_match_stats(update_id)

    # Clean team matches, save, update table and save
    player_matches_cleaner.clean_save_update_data(last_date=new_match_reference_date, update_id=update_id) # use new match_reference_date

    # Upload player matches to Database
    player_matches_uploader.upload_clean_data(update_id)


def update_keeper_matches(update_id, new_match_reference_date):
    """
    Update keeper matches
    """
    # Set up logger
    logger.info(f"Updating keeper-matches for update id: {update_id}")

    # Initialize Cleaner and Uploader
    keeper_matches_cleaner = clean.fbref_clean_keeper_matches.FbrefCleanKeeperMatches()
    keeper_matches_uploader = upload.UploadSoccerDB(name='keeper_matches')

    # Clean keeper matches
    keeper_matches_cleaner.clean_save_update_data(last_date=new_match_reference_date, update_id=update_id) # use new match_reference_date

     # Upload keeper matches to Database
    keeper_matches_uploader.upload_clean_data(update_id)

if __name__ == "__main__":
    ############################# Parameters #############################

    update_id = '2024_07_01_run1'
    ls_reference = '2024-03-01' # specifically for updating league season start and end
    last_date = '2024-07-01'
    old_match_reference_date = '2024-04-30' # this date should coincide with last match data scrape date
    new_match_reference_date = '2024-07-01' # this date should coincide with this data scrape date
    is_new_ls = False # Are there any new league seasons since last update?


    #######################################################################
    start_from = sys.argv[1] if len(sys.argv) > 1 else None

    workflow = SoccerDBWorkflowController(
        update_id, 
        ls_reference, 
        last_date, 
        old_match_reference_date, 
        new_match_reference_date,
        is_new_ls
    )
    workflow.run_workflow(start_from)
    
    
    

    
    
    
    
    
    

    
        
        
        
    