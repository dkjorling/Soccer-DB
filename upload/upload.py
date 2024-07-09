import sqlite3
import json
from logging_config import logger
import pandas as pd
import re
from tqdm import tqdm
from utils.helpers import load_db_dict, write_db_dict


class UploadSoccerDB:
    """
    This is the base class for all database-related functions.

    This class is responsible for both creating and updating database tables.

    Main Functionality Methods
    --------------------------
    upload_clean_data(self, update_id=None):
        Clean data by checking primary and foreign key constraints and upload data to sqlite database table.
    create_table(self):
        Create table in the SQLite database if it does not already exist. 
    drop_table(self):
         Drop a table from the SQLite database.
    """
    # Initialization Methods
    def __init__(self, name=None):
        self.logger = logger
        self.set_name(name)
        self.set_create_table_query()
        self.set_primary_key()

    def set_name(self, name):
        valid_entity_names = [
            'seasons', 'leagues', 'ls', 'teams', 'tls', 'players',
            'ptls', 'matches', 'team_matches', 'player_matches', 'keeper_matches',
            'player_values', 'team_values'
        ]
        # Validate 'name' attribute against the list of valid entity names
        if name in valid_entity_names:
            self.name = name
        else:
            self.name = None
    
    def set_create_table_query(self):
        """
        Loads create table query using class name
        """
        with open("config/create_table_queries.json", "r") as f:
            create_table_queries_dict = json.load(f)
        try:
            self.create_table_query = "".join(create_table_queries_dict[self.name])
        except Exception as e:
            self.logger.error(f"Could not load query for table {self.name} due to error: {e}")
            self.create_table_query = None
            
    def set_primary_key(self):
        """
        Use config file to set primary key
        """
        # Load name to primary key mapping
        with open("config/primary_key_config.json","r") as f:
            name_to_primary = json.load(f)

        # Set primary key attribute using mapping
        self.primary_key = name_to_primary[self.name]
    
    # Main Functionality Methods
    def upload_clean_data(self, update_id=None):
        """
        Clean data by checking primary and foreign key constraints and upload data to sqlite database table.

        Parameters
        ----------
        data
            Either initial data from data/db_tables directory or new data from class specific data directory
        update_id : str
            ID corresponding to update date and run number.

        """
        # Load File
        if update_id != None:
            data = pd.read_csv(f"data/fbref/{self.name}/{self.name}_{update_id}.csv", index_col=0)
        else:
            data = pd.read_csv(f"data/db_tables/{self.name}.csv", index_col=0)

        # Clean Data using constraints
        clean_data = self.check_table_constraints(data)

        # Upload to database
        self.upload_to_db(clean_data)

        # Update db_dict
        self.update_db_dict(clean_data)
    
    def load_table(self, table):
        """
        Fetch all data from the specified table in the SQLite database and return as a pandas DataFrame.

        Parameters
        ----------
        table : str
            Name of table to load

        Returns
        -------
        pandas.DataFrame
            All rows from the specified table as a DataFrame.
        """
        # Connect to database
        con = self.db_connect()

        # Create Query
        qstr = f"SELECT * FROM {table}"  # get all data

        try:
            # Execute query
            data = pd.read_sql_query(qstr, con)
            print(f"Table '{table}' loaded successfully.")
            con.close()
            return data
        except sqlite3.Error as e:
            self.logger.error(f"Error occurred while loading data from table '{table}': {e}")
            con.close()

    def create_table(self):
        """
        Create table in the SQLite database if it does not already exist. 

        This function relies on create_table_queries.json file in the config folder,
        which stores create table queries for each table in the db.

        Parameters
        ----------
        table_name : str
            Name of table 

        Raises
        ------
        sqlite3.Error
            If an error occurs during table creation.
        """
        # Connect to DB and Create Table
        con = self.db_connect()
        cur = con.cursor()
        try:
            cur.execute(self.create_table_query)
            con.commit()
            print(f"Successfully created {self.name} table!")
        except sqlite3.Error as e:
            self.logger(f"Error occurred while creating the {self.name} table:", e)
        cur.close()
        con.close()

    def drop_table(self):
        """
        Drop a table from the SQLite database.

        Parameters
        ----------
        table : str
            Name of the table to drop.

        Returns
        -------
        None

        Raises
        ------
        sqlite3.Error
            If an error occurs during the operation.

        Notes
        -----
        This function attempts to drop the specified table from the SQLite database.
        If the operation fails, it prints an error message.
        """
        con = self.db_connect()
        cur = con.cursor()
        qstr = f"DROP TABLE IF EXISTS {self.name}"  # Added IF EXISTS to avoid errors if the table doesn't exist
        try:
            cur.execute(qstr)
            con.commit()
            print(f"Table '{self.name}' dropped successfully.")
        except sqlite3.Error as e:
            self.logger.error(f"Error occurred while dropping the table '{self.name}': {e}")
        finally:
            cur.close()
            con.close()

    # Helper Methods
    def upload_to_db(self, data, name=None):
        """
        Upload data to database

        update_id : str; optional
            ID corresponding to update date and run number.
            If not passed, data from entire table will try to be uploaded

        """
        # Connect to DB
        con = self.db_connect()
        try:
            # Get the total number of rows in the DataFrame
            total_rows = len(data)
            
            # Use tqdm to create a progress bar
            with tqdm(total=total_rows) as pbar:
                # Iterate over DataFrame in chunks
                for start in range(0, total_rows, 5000):
                    end = min(start + 5000, total_rows)
                    chunk = data.iloc[start:end]
                    
                    # Print the shape of the chunk
                    print(chunk.shape)
                    
                    # Append the chunk to the SQLite table
                    # Allows for passing different tablee name
                    if name != None:
                        chunk.to_sql(name, con, if_exists='append', index=False)
                    else:
                        chunk.to_sql(self.name, con, if_exists='append', index=False)
                    
                    # Update the progress bar
                    pbar.update(end - start)
                    
                    print("Chunk done!")
                    print()
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            # Close the database connection
            con.close()

    def db_connect(self):
        """
        Connect to the SQLite database.

        Parameters
        ----------
        db_path : str, optional
            Path to the SQLite database file. Defaults to '../data/your_database.db'.

        Returns
        -------
        sqlite3.Connection
            Connection object to the SQLite database.
        """
        db_path = 'data/soccerdb.db'
        # connect to db
        try:
            con = sqlite3.connect(db_path)
            print("Connection Successful!")
            return con # return connection object if successful
        except sqlite3.Error as e:
            self.logger.error("Error occurred while creating tables:", e)

    def check_tables(self):
        """
        Check the tables in the SQLite database and print their names.

        This function connects to the SQLite database, queries the list of tables,
        and prints each table name. If an error occurs during the query, it is printed.

        Notes
        -----
        The function uses the `sqlite_master` table to retrieve the list of tables.
        """
        con = self.db_connect()
        cur = con.cursor()
        qstr = "SELECT name AS TABLE_NAME FROM sqlite_master WHERE type='table';"
        try:
            cur.execute(qstr)
            for row in cur:
                print(row)
        except sqlite3.Error as e:
            self.logger.error("Error occurred while creating tables:", e)
        cur.close()
        con.close()
    
    def check_columns(self):
        """
        Check the columns of a specified table in the SQLite database.

        Parameters
        ----------
        table : str
            Name of the table to check the columns for.

        Returns
        -------
        list of str
            List of column names in the specified table.

        Notes
        -----
        This function uses the `PRAGMA table_info` command to retrieve column information.
        """
        con = self.db_connect()
        cur = con.cursor()
        qstr = f"PRAGMA table_info({self.name});"
        cols = []
        try:
            cur.execute(qstr)
            for row in cur:
                cols.append(row)
        except sqlite3.Error as e:
            self.logger.error("Error occurred while creating tables:", e)
        cur.close()
        con.close()
        return cols

    
    def check_table_constraints(self, data):
        """
        Cleans data by checking table constraints before uploading to database.

        Returns
        -------
        Clean data
        """
        # Load database dict
        db_table = self.load_table(self.name)

        if db_table.shape[0] != 0:
            # Filter out duplicates if data already in table
            _ids = list(db_table[self.primary_key])
            # Filter out data already in database
            data = data[~data[self.primary_key].isin(_ids)]

        # Filter out data that does not comply with foreign key constraints
        foreign_keys = self.get_foreign_keys_from_query()
        for key in foreign_keys:
            data = self.check_foreign_key_constraint(data, key)

        # Convert integer columns and reset index
        int_keys = self.get_int_columns_from_query()
        data = self.convert_to_int(data, int_keys).reset_index(drop=True)
        return data

    def convert_to_int(self, data, int_cols):
        """
        Convert columns that should be of type int to int for database insertion

        Parameters
        ----------
        data : pandas dataframe
            Dataframe containing columns to be converted
        int_cols : list of str
            List of dataframe column names to convert to int
        
        Returns
        -------
        pandas dataframe
            Returns dataframe with converted columns
        """
        for col in int_cols:
            data[col] = pd.to_numeric(data[col], errors='coerce').astype('Int64')
        return data
    
    def get_int_columns_from_query(self):
        """
        Given a create table query, get all int columns and return as list.

        Returns
        -------
        list of str
            List of fields that should be of integer type prior to database table insertion
        """
        # Initiate empty list to store integer keys
        int_keys = []

        # Iterate through create table query to extract INT fields
        for query_line in self.create_table_query:
            if 'INT' in query_line:
                int_keys.append(query_line.strip().split(" ")[0])
        return int_keys
    
    def check_foreign_key_constraint(self, data, foreign_key):
        """
        Checks data against foreign key contraints.

        Parameters
        ----------
        data : pandas dataframe

        foreign_key : str

        Returns
        -------
        pandas dataframe
            Returns data filtered to comply with foreign key constraint
        """
        foreign_key_ids = load_db_dict()[foreign_key]['ids']
        data = data[data[foreign_key].isin(foreign_key_ids)]
        return data

    def get_foreign_keys_from_query(self):
        """
        Given a create table query, get all foreign keys that constrain database insertion.

        Returns
        -------
        list of str
            List of foreign keys
        """
        foreign_keys = []
        for query_line in self.create_table_query:
            if 'FOREIGN KEY' in query_line:
                foreign_keys.append(re.findall(r"FOREIGN\sKEY\s\((.+)\)\sREFERENCES",query_line)[0])
        return foreign_keys

    def update_db_dict(self, clean_data):
        """
        Update the database dictionary with the latest table update date and primary key ids.

        Parameters
        ----------
        new_table : pandas.DataFrame
            The updated table containing all old and new primary key values.
        last_date : str
            The latest data point in "%Y-%m-%d" format

        Notes
        -----
        This method updates the database dictionary (`db_dict`) with the following information:
        - Updates the 'update' key under `self.name` with `last_date`.
        - Sets the 'ids' key under `self.name` to the list of primary key values from `new_table[self.primary_key]`.
        After updating, the `db_dict` is saved back to the file using `write_db_dict`.
        """
        try:
            # Load db dict
            db_dict = load_db_dict()

            # Get new ids
            _ids = list(clean_data[self.primary_key])

            # Update values
            db_dict[self.name]['ids'] = _ids

            # Save to file
            write_db_dict(db_dict)
            print("Successfully updated db_dict!")
        except Exception as e:
            self.logger.error(f"Could not update db_dict file due to error: {e}") 


