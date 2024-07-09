import json

query_data_path = "config/create_table_queries/"
table_names = [
    'seasons', 'leagues', 'ls', 'teams', 'tls', 'matches',
    'players', 'ptls', 'team_matches', 'player_matches', 'keeper_matches',
    'team_values', 'player_values'
]

def make_create_table_queries_dict(data_path, table_names):
    """
    Create a dictionary to store all create table dicts
    """
    create_table_queries_dict = {}
    for name in table_names:
        # Load query
        with open(data_path + f"create_{name}_table.txt", "r") as f:
            query = f.readlines()
        # Store in dict
        create_table_queries_dict[name] = query
    return create_table_queries_dict
    

if __name__ == "__main__":
    create_table_queries_dict = make_create_table_queries_dict(query_data_path, table_names)
    with open("config/create_table_queries.json", "w") as f:
        f.write(json.dumps(create_table_queries_dict))
    