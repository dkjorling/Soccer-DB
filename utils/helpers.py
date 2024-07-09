import pandas as pd
import json

def load_data():
    """
    Load all updated database tables into a dictionary and return this dictionary.

    Parameters
    ----------
    stats : bool, optional
        Determines whether to include team_match, player_match, and keeper_match data.
        Defaults to False.

    Returns
    -------
    dict
        A dictionary containing pandas DataFrames for each database table loaded from CSV files.
        Keys correspond to table names: 'seasons', 'leagues', 'ls', 'teams', 'tls', 'players', 'ptls', 'matches'.
        If stats=True, additional keys 'team_matches', 'player_matches', and 'keeper_matches' are included.

    Notes
    -----
    This function loads the following database tables from CSV files:
    - seasons
    - leagues
    - ls (league seasons)
    - teams
    - tls (team league seasons)
    - players
    - ptls (player team league seasons)
    - matches

    If stats=True, it also loads:
    - team_matches
    - player_matches
    - keeper_matches

    Each table is loaded as a pandas DataFrame and stored in a dictionary under corresponding keys.
    """
    # Load data from each database table
    seasons = pd.read_csv("data/db_tables/seasons.csv", index_col=0)
    leagues = pd.read_csv("data/db_tables/leagues.csv", index_col=0)
    ls = pd.read_csv("data/db_tables/ls.csv", index_col=0)
    teams = pd.read_csv("data/db_tables/teams.csv", index_col=0)
    tls = pd.read_csv("data/db_tables/tls.csv", index_col=0)
    players = pd.read_csv("data/db_tables/players.csv", index_col=0)
    ptls = pd.read_csv("data/db_tables/ptls.csv", index_col=0)
    matches = pd.read_csv("data/db_tables/matches.csv", index_col=0)
    # Store data in dictionary
    data_dict = {
        'seasons': seasons,
        'leagues': leagues,
        'ls': ls,
        'teams': teams,
        'tls': tls,
        'players': players,
        'ptls': ptls,
        'matches': matches
    }
    # Add stat data if applicable
    team_matches = pd.read_csv("data/db_tables/team_matches.csv", index_col=0)
    player_matches = pd.read_csv("data/db_tables/player_matches.csv", index_col=0, dtype={'season_long': 'str', 'start':'str'})
    keeper_matches = pd.read_csv("data/db_tables/keeper_matches.csv", index_col=0, dtype={'season_long': 'str', 'start':'str', 'position':'str'})
    data_dict['team_matches'] = team_matches
    data_dict['player_matches'] = player_matches
    data_dict['keeper_matches'] = keeper_matches
    return data_dict

def load_db_dict():
    """
    Load a dictionary containing the last update date and a list of unique primary keys for each fbref entity.

    Returns
    -------
    dict
        A dictionary loaded from 'config/db_dict.json' containing:
        - 'last_update_date': Last update date of the database.
        - 'primary_keys': Dictionary where keys are entity names and values are lists of unique primary keys.

    Notes
    -----
    This function loads the database dictionary from the 'config/db_dict.json' file. It is expected to contain:
    - 'last_update_date': Date of the last update to the database.
    - 'primary_keys': Dictionary where each key corresponds to an entity (e.g., 'teams', 'players')
      and the value is a list of unique primary keys associated with that entity.
    """
    with open('config/db_dict.json', 'r') as f:
        db_dict = json.load(f)
    print("Successfully loaded db_dict")
    return db_dict

def write_db_dict(db_dict):
    """
    Save the provided db_dict to the 'data/config/db_dict.json' file.

    Parameters
    ----------
    db_dict : dict
        The dictionary containing data to be saved.

    Notes
    -----
    This function writes the provided dictionary (`db_dict`) to the 'data/config/db_dict.json' file.
    The JSON file is formatted with an indentation level of 2 spaces for readability.
    """
    with open('config/db_dict.json', 'w') as f:
        f.write(json.dumps(db_dict, indent=2))
    print("Successfully updated db_dict")




