import sqlite3

def create_database(db_path):
    """
    Create database

    Parameters
    ----------
    db_path : str
        Location to store database
    """
    connection = sqlite3.connect(db_path)
    connection.close()

if __name__ == "__main__":
    db_path = 'data/soccerdb.db'  # Adjust the path as necessary
    create_database(db_path)
    print(f"Database created at {db_path}")