import pandas as pd
import numpy as np
import json
import time
import scraper 
from logging_config import logger

def scrape_save_new_league_season_start_end(ls_scraper, new_league_seasons, update_id):
    """
    Uses the ls_scraper to iterate through each new league-season to get start and end dates.

    Parameters
    ----------
    ls_scraper : FbrefLeagueSeasonsScraper
        An instance of the FbrefLeagueSeasonsScraper class used to scrape league season start and end dates.
    new_league_seasons : pd.DataFrame
        A DataFrame containing the new league seasons, typically the output from the ls_entity get_and_save_new_league_seasons method.
    update_id : str
            The identifier corresponding to the update date and run number.

    Returns
    -------
    None
        This function does not return any value. It saves the scraped data to a CSV file.

    Notes
    -----
    - The function verifies that the ls_scraper is an instance of FbrefLeagueSeasonsScraper.
    - It initializes a DataFrame to store league ID, season (long format), start date, end date, and whether the season has rounds.
    - Iterates through each row in the new_league_seasons DataFrame, scraping start and end dates using the ls_scraper.
    - Saves the resulting DataFrame to a CSV file named "new_league_start_end_{month_year}.csv".
    - Prints success messages and the time taken to complete the scraping and saving process.
    - In case of an error, logs error message indicating the failure reason.
    """
    # Check type
    if not isinstance(ls_scraper, scraper.fbref_ls_scraper.FbrefLeagueSeasonsScraper):
        raise TypeError(f"Expected instance of FbrefLeagueSeasonsScraper, got {type(ls_scraper).__name__}")

    # Create empty dataframe
    lg_start_end = pd.DataFrame(columns=['lg_id', 'season_long', 'lg_start', 'lg_end', 'has_rounds'])
    i = 0

    # Get number of rows for progress check and initialize scrape times list
    num_items = new_league_seasons.shape[0]
    scrape_times = []

    # Iterate through new league seasons dataframe to get league start and end
    for _, row in new_league_seasons.iterrows():
        try:
            # Record start time
            start_time = time.time()
            # Get start/end data and add to dataframe
            start_end_rounds = ls_scraper.scrape_league_season_start_end(lg_id=row.lg_id, season_long=row.season_long)
            lg_start_end.loc[i] = [row.lg_id, row.season_long, start_end_rounds[0], start_end_rounds[1], start_end_rounds[2]]
            i += 1
            time.sleep(6) # Fbref request limit
            # Record end time
            end_time = time.time()
            # Add time to scrape times
            scrape_times.append(end_time - start_time)
            # Record estimated time left and update progress
            est_time_left = np.mean(scrape_times) * (num_items - i - 1)
            print(f"Successfully Scraped start/end for {row.lg_id}-{row.season_long}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
        
        except Exception as e:
            # Fbref scraping limit
            time.sleep(6)
            ls_scraper.logger.error(f"Could not scrape new league season start and end dates due to error: {e}")
        try:
            # Save dataframe to file
            lg_start_end.to_csv(f"data/fbref/ls/raw/new_league_start_end_{update_id}.csv")
            print("Sucessfully scraped and saved all league season start and end dates!")
            elapsed_time = np.sum(scrape_times)
            ls_scraper.logger.info(f"It took {elapsed_time:.4f} seconds to scrape and save all new league season start and end dates for update: {update_id}")
        
        except Exception as e:
            ls_scraper.logger.error("Could not save new league start and end")
    

def check_save_new_league_season_adv_stats(ls_scraper, new_league_seasons, update_id):
    """
    Use ls_scraper to iterate through each new league season to determine if it has advanced stats or not.

    Parameters
    ----------
    ls_scraper : FbrefLeagueSeasonsScraper
        An instance of the FbrefLeagueSeasonsScraper class used to check for advanced stats.
    new_league_seasons : pd.DataFrame
        A DataFrame containing the new league seasons, typically the output from the ls_entity get_and_save_new_league_seasons method.
    update_id : str
        The identifier corresponding to the update date and run number.

    Returns
    -------
    None
        This function does not return any value. It saves the data to a CSV file.

    Notes
    -----
    - The function verifies that the ls_scraper is an instance of FbrefLeagueSeasonsScraper.
    - It initializes a DataFrame to store league ID, season (long format), and whether the league has advanced stats.
    - Iterates through each row in the new_league_seasons DataFrame, checking for advanced stats using the ls_scraper.
    - Saves the resulting DataFrame to a CSV file named "new_league_has_adv_stats_{update_id}.csv".
    - Prints the time taken to determine whether each new league season has advanced stats.
    - In case of an error, logs an error message indicating the failure reason.
    """
    # Check type
    if not isinstance(ls_scraper, scraper.fbref_ls_scraper.FbrefLeagueSeasonsScraper):
        raise TypeError(f"Expected instance of FbrefLeagueSeasonsScraper, got {type(ls_scraper).__name__}")

    # Create Empty dataframe to store data in
    lg_has_adv = pd.DataFrame(columns=['lg_id','season_long', 'has_adv_stats'])
    i = 0

    # Get number of rows for progress check and initialize scrape times list
    num_items = new_league_seasons.shape[0]
    scrape_times = []

    for _, row in new_league_seasons.iterrows():
        try:
            # Begin Timer
            start_time = time.time()
            has_adv_stats = ls_scraper.check_adv_stats(row.lg_id, row.season_long)
            lg_has_adv.loc[i] = [row.lg_id, row.season_long, has_adv_stats]
            i += 1
            time.sleep(6) # fbref request limit
            # End Timer
            end_time = time.time()
            # Add time to scrape times
            scrape_times.append(end_time - start_time)
            # Record estimated time left and update progress
            est_time_left = np.mean(scrape_times) * (num_items - i - 1)
            print(f"Successfully Scraped adv stat type for {row.lg_id}-{row.season_long}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
        except Exception as e:
            ls_scraper.logger.error(f"Could not scrape new league: {row.lg_id} and season: {row.season_long} has adv stats due to error: {e}")

    try:
        # Save dataframe to file
        lg_has_adv.to_csv(f"data/fbref/ls/raw/new_league_has_adv_stats_{update_id}.csv")
        ls_scraper.logger.info(f"Successfully saved new league advanced stat dataframe!")
        # Produce and print total time taken
        elapsed_time = np.sum(scrape_times)
        ls_scraper.logger.info(f"It took {elapsed_time:.4f} seconds to determine whether each new league season has advanced stats for update: {update_id}")
    except Exception as e:
        ls_scraper.logger.error(f"Could not save new league has adv stats due to error: {e}")
        raise ValueError()

def scrape_save_new_team_names(teams_scraper, update_id):
    """
    Use FbrefTeamsScraper instance to iterate through all new team names and save dictionary with team ids as keys, team names as values.
    
    Takes output from clean.fbref_teams.id_save_all_new_teams() method

    Parameters
    ----------
    teams_scraper: FbrefTeamsScraper
        An instance of FbrefTeamsScraper used for scraping team names.
    update_id : str
        The identifier corresponding to the update date and run number.
    """
    # Make sure correct type
    if not isinstance(teams_scraper, scraper.fbref_teams_scraper.FbrefTeamsScraper):
        raise TypeError(f"Expected instance of FbrefTeamsScraper, got {type(teams_scraper).__name__}")
        
    # Load new teams df
    file_path = f"data/fbref/teams/raw/new_teams_df_{update_id}.csv"
    new_teams_df = pd.read_csv(file_path, index_col=0)

    if new_teams_df.shape[0] == 0:
        logger.info("There are no new teams to scrape!")
        return None

    # Create list to store names
    team_names = []

    # Get number of ids for progress check and initialize scrape times list
    num_items = new_teams_df.shape[0]
    scrape_times = []

    # Iterate through new teams
    for _, row in new_teams_df.iterrows():
        try:
            # Begin Timer
            start_time = time.time()
            # Get data and append 
            team_names.append(teams_scraper.scrape_team_name(row.tid))
            time.sleep(6) # fbref scraping limitation
            # End Timer
            end_time = time.time()
            # Append scrape time
            scrape_times.append(end_time - start_time)
            # Calculate and print estimated time left
            est_time_left = np.mean(scrape_times) * (num_items - i - 1)
            print(f"Successfully Scraped team name for {row.tid}. Completed {i+1} out of {num_items} items. Estimated time left: {est_time_left:.2f} seconds.")
        except Exception as e:
            teams_scraper.logger.error(f"Could not scrape and save new team names due to error: {e}")
    # Create team_name column
    new_teams_df['team_name'] = team_names

    # Save to file
    try:
        file_path_save = f"data/fbref/teams/raw/new_team_names_df_{update_id}.csv"
        teams_scraper.save_raw_data(new_teams_df, file_path_save)
        teams_scraper.logger.info(f"It took {np.mean(scrape_times):.2f} seconds to scrape new team names")
        return new_teams_df
    except Exception as e:
        teams_scraper.logger.error(f"Could not save new teams names due to error: {e}")
        raise ValueError()


def scrape_save_api_teams_from_all_league_seasons(api_scraper, update_id):
    """
    Scrape API team names and ids for all league season ids for new fbref teams.

    This is an intermediate function whose output data will be used to find a best api-fbref team match.
    
    Takes in output from scrape_save_new_team_names function.

    Parameters
    ----------
    api_scraper: ApiScraper
        An instance of ApiScraper used for scraping team meta-data.
    update_id : str
        The identifier corresponding to the update date and run number.

    Returns
    -------
    dict
        Dictionary with football reference league-season id as key and values for the following:
            api_ids - list of api ids for corresponding api league-season
            api_team_names - list of api team names for corresponding api_ids
            fbref_team_names - list of football reference team names for league season
            fbref_tid - list of football reference team ids for league season
    """
    # Check Scraper Type
    if not isinstance(api_scraper, scraper.api_scraper.ApiScraper):
        raise TypeError(f"Expected instance of ApiScraper, got {type(api_scraper).__name__}")

    # Load new team names df
    file_path = f"data/fbref/teams/raw/new_team_names_df_{update_id}.csv"
    new_team_names_df = pd.read_csv(file_path, index_col=0)

    # Extract API ID from leagues table and append to new team dfs
    leagues = api_scraper.data_dict['leagues']
    new_team_names_df = new_team_names_df.merge(leagues[['lg_id', 'api_id']], how='left', on='lg_id')

    
    # Get unique tls id names
    unique_ls = list(new_team_names_df.drop_duplicates(subset=['ls_id']).ls_id)
    # Create dict for storing
    api_tls_dict = {}
    
    # Iterate through each unique ls with new teams, pulling list of api team names and ids
    for ls in unique_ls:
        try:
            print(ls)
            subset_df = new_team_names_df[new_team_names_df.ls_id==ls].reset_index(drop=True)
            # Get API IDs and Team names for the given league-season
            api_ids, api_names = api_scraper.get_teams_from_league_season(subset_df.api_id[0], subset_df.season[0])
            # Store All lists in dict
            api_tls_dict[ls] = {} # create dictionary for each unique league season
            api_tls_dict[ls]['api_ids'] = api_ids
            api_tls_dict[ls]['api_team_names'] = api_names
            api_tls_dict[ls]['fbref_team_names'] = list(subset_df.team_name)
            api_tls_dict[ls]['fbref_tid'] = list(subset_df.tid)
            time.sleep(1)
        except Exception as e:
            api_scraper.logger.error(f"Could not get api_ids or api_names for league-season {ls} due to error: {e}")
        # Save to file
        file_save_path = f"data/fbref/teams/raw/new_team_names_dict_{update_id}.json"
        with open(file_save_path, 'w') as f:
            f.write(json.dumps(api_tls_dict))
        print("Successfully saved api team ids and names for new teams!")
    

def scrape_save_all_api_team_info(api_scraper, update_id):
    """
    This function uses the output from FbrefCleanTeams after the matching between football reference and Rapid API occurs.

    It uses the proper API ID to fetch full meta-data from the API using ApiScraper class.

    Parameters
    ----------
    api_scraper: ApiScraper
        An instance of ApiScraper used for scraping team meta-data.
    update_id : str
        The identifier corresponding to the update date and run number.
    """
    # Load dictionary containing team data
    file_path = f"data/fbref/teams/temp/new_team_api_dict_{update_id}.json"
    with open(file_path, 'r') as f:
        fbref_to_api_dict = json.load(f)

    # Create empty data framefor storing
    df_team_info = pd.DataFrame(columns=['tid', 'team_name', 'api_id', 'code', 'country', 'city',
                                         'venue', 'capacity', 'logo_url', 'venue_url', 'address'])
    ii = 0
    # Iterate through dictionary
    for k in fbref_to_api_dict.keys():
        try:
            tid_api = fbref_to_api_dict[k]['api_id']
            tid_fbref = fbref_to_api_dict[k]['fbref_tid']
            if pd.isna(tid_api):
                team_info = [np.nan] * 8
            else:
                team_info = api_scraper.get_team_info(tid_api)
            df_team_info.loc[ii] = [tid_fbref, k, tid_api] + team_info
            ii += 1
        except Exception as e:
            api_scraper.logger.error(f"Could not save raw teams data for {update_id} due to error: {e}")
    # Save raw teams data to file
    file_path = f"data/fbref/teams/raw/teams_raw_{update_id}.csv"
    api_scraper.save_raw_data(df_team_info, file_path)
    print(f"Successfully saved raw teams data for {update_id}!")
    
        

    

    
    
    

    
    
    
        
    


    
    