import pandas as pd
import json
import time
import os
from utils.helpers import load_data, load_db_dict, write_db_dict
from logging_config import logger

class FbrefClean:
    """
    This is the base class for cleaning and saving data scraped by football reference scraping classes.
    

    Attributes
    ----------
    name : str; optional
        Name of the class data
    data_dict : dict
        contains data for all updated tables
    db_table : dataframe
        contains data for only class table
    primary_key : str
        key relating to database primary key
    
    Main Functionality Methods
    --------------------------
    clean_save_update_data(self, last_date, month_year=None, stats=False):
        Cleans raw data, saves the clean data to a file, backs up the current production table, 
        updates the production table with new values, and saves the updated table to production.
    check_data_vs_other_table(self, update_id, other_tables, foreign_keys):
        Checks foreign keys in clean data vs other tables to see if they exist in other tables.
        Saves a dictionary to file which can be investigated further.
    """
    # Initialization Methods
    def __init__(self, name=None):
        self.logger = logger
        self.data_dict = load_data()
        self.set_name(name)
        self.set_db_table() 
        self.primary_key = None

    def set_valid_entity_names(self):
        self.valid_entity_names = [
            'seasons', 'leagues', 'ls', 'teams', 'tls', 'players', 'ptls',
            'matches', 'team_matches', 'player_matches', 'keeper_matches'
        ]

    def set_name(self, name):
        valid_entity_names = [
            'seasons', 'leagues', 'ls', 'teams', 'tls', 'players', 'ptls',
            'matches', 'team_matches', 'player_matches', 'keeper_matches'
        ]
        # Validate 'name' attribute against the list of valid entity names
        if name in valid_entity_names:
            self.name = name
        else:
            self.name = None

    def set_db_table(self):
        self.db_table = self.data_dict[self.name]

    # Main Class Functionality Methods
    def clean_save_update_data(self, last_date, update_id=None, skip_clean=False):
        """
        Cleans raw data, saves the clean data to a file, backs up the current production table, 
        updates the production table with new values, and saves the updated table to production.

        Parameters
        ----------
        last_date : str
            Date in "%Y-%m-%d" format corresponding to the latest data point.
        update_id : str, optional
            ID corresponding to update date and run number.
        skip_clean : bool, optional
            If True, skips the data cleaning process. Default is False.

        Returns
        -------
        pandas.DataFrame
            The updated production table dataframe object.

        Raises
        ------
        Exception
            If the user cancels the process or if any error occurs during the execution of the function.

        Notes
        -----
        This method performs the following steps:
        1. Cleans the raw data, unless `skip_clean` is True.
        2. Saves the cleaned data to a file.
        3. Backs up the current production table.
        4. Updates the production table with new values.
        5. Saves the updated table to production.
        6. Returns the updated production table.
        """
        try:
            start_time = time.time()
            # Clean data with option for user to skip
            if skip_clean == False:
                clean_data = self.clean_data(update_id)
                # Save new clean data
                file_path = f"data/fbref/{self.name}/{self.name}_{update_id}.csv"
                self.save_data(clean_data, file_path)
            # Check if user wants to procede to backup
            user_input = input(f"Do you want to procede with cleaning the data? (yes/no): ").lower()
            if user_input != 'yes':
                raise Exception(f"Data cleaning for update id: {update_id} cancelled by user")
            # Backup old data
            self.backup_table(update_id)
            original_rows = self.db_table.shape[0] # get original df row number
            # Check if user wants to procede to updating main table
            user_input = input(f"Do you want to procede with updating the production table? (yes/no): ").lower()
            if user_input != 'yes':
                raise Exception(f"Data cleaning for update id: {update_id} cancelled by user")
            # Update table
            new_table = self.update_table(clean_data)
            # Update db dict
            self.update_db_dict(new_table, last_date)
            new_rows = new_table.shape[0]
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Successfully cleaned, saved, and updated league seasons table with new data!")
            print(f"In total, {new_rows - original_rows} rows of data were added")
            print(f"In total, this process took {elapsed_time:.4f} seconds to complete")
            return new_table
        except Exception as e:
            self.logger.error(f"Could not clean, save and update league seasons due to error: {e}")

    def check_data_vs_other_table(self, update_id, other_tables, foreign_keys):
        """
        Checks foreign keys in clean data vs other tables to see if they exist in other tables.
        Saves a dictionary to file which can be investigated further.

        Parameters
        ----------
        update_id : str
            id corresponding to update date and run number
        other_tables : list of str
            names of tables to check against
        foreign_keys : list of str
            foreign keys to check against list of other_tables

        Notes
        -----
        - Users can pass multiple tables for the same foreign key

        Returns
        -------
        dict
            Dict detailing unmatched keys for each table checked
        """
        unmatched_data = {}
        data = pd.read_csv(f"data/fbref/{self.name}_{update_id}.csv", index_col=0)
        for i, table in enumerate(other_tables):
            # Check vs both production table and latest update
            df_table = self.data_dict[table]
            df_updated = pd.read_csv(f"data/fbref/{table}/{table}_{update_id}.csv")
            # Add inner dictionary if not exists 
            if table not in unmatched_data.keys():
                unmatched_data[table] = {}
            # Check for mismatched keys
            unmatched_data[table][foreign_keys[i]] = {}
            unmatched1 = [x for x in list(data[foreign_keys[i]]) if x not in list(df_table[foreign_keys[i]])]
            unmatched2 = [x for x in list(data[foreign_keys[i]]) if x not in list(df_updated[foreign_keys[i]])]
            unmatched_data[table][foreign_keys[i]]['unmatched_full'] = unmatched1
            unmatched_data[table][foreign_keys[i]]['unmatched_latest'] = unmatched2
            print(f"There are {len(unmatched1)} ids unmatched with full table {table} for key {foreign_keys[i]}")
            print(f"There are {len(unmatched2)} ids unmatched with latest table {table} for key {foreign_keys[i]}")
        # Save Data
        file_path = f"data/fbref/{self.name}/temp/unmatched_data_{update_id}.json"
        self.save_data(file_path)
        return unmatched_data

    # Helper Methods
    def clean_data(self, *args, **kwargs):
        """
        This method is a placeholder for cleaning football reference data.

        Subclasses must implement this method to provide specific functionality for cleaning stats data.

        Parameters
        ----------
        *args : tuple
            Variable-length positional arguments. These arguments can be used to pass any additional parameters as required by subclasses.

        **kwargs : dict
            Arbitrary keyword arguments. These arguments can be used to pass any additional keyword parameters as required by subclasses.

        Raises
        ------
        NotImplementedError
            This method is not implemented in the base class and must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement clean_data")


    def save_data(self, data, file_path):
        """
        Generic save data function which handles saving data in pandas DataFrame or dictionary (JSON) format.

        Parameters
        ----------
        data : dict or pandas.DataFrame
            The data to be saved. Can be either a dictionary (JSON format) or a pandas DataFrame (CSV format).
        file_path : str
            The full file path and name where the data should be saved.
        """
        try:
            if isinstance(data, pd.DataFrame):
                # Data is a pandas DataFrame, save it to a CSV file
                data.to_csv(file_path)
                print(f"DataFrame saved to {file_path}")
                return True
            elif isinstance(data, dict):
                # Data is a dictionary, save it to a JSON file
                with open(file_path, 'w') as f:
                    f.write(json.dumps(data))
                print(f"Dictionary saved to {file_path}")
                return True
            else:
                # Unsupported data type
                print("Unsupported data type. Only pandas DataFrame or dictionary is supported.")
                return False
        except Exception as e:
            self.logger.error(f"Error saving data to {file_path}: {e}")
            return False

    def backup_table(self, update_id):
        """
        Backs up the production table to a specified fbref folder using the table name and update identifier for the file path.

        Parameters
        ----------
        update_id : str
            The identifier corresponding to the update date and run number.

        Raises
        ------
        Exception
            If the backup file already exists and the user chooses not to overwrite it.

        Notes
        -----
        This method performs the following operations:
        1. Constructs the file path for the backup file in the format "data/fbref/{self.name}/{self.name}_backup_{update_id}.csv".
        2. Checks if a backup file already exists at the specified file path and prompts the user to confirm overwriting it if it does.
        3. Saves the current production table (`self.db_table`) to the backup file.
        4. Prints a success message upon successful backup.
        5. Logs an error message if any exception occurs during the backup process.

        """
        # set file path
        file_path = f"data/fbref/{self.name}/{self.name}_backup_{update_id}.csv"
        # check if already exists and prompt user
        if os.path.exists(file_path):
            user_input = input(f"The backup file '{file_path}' already exists. Do you want to overwrite it? (yes/no): ").lower()
            if user_input != 'yes':
                print("Data was not backed up by user!")
                return None
        try:
            self.save_data(data=self.db_table, file_path=file_path)
            print(f"Successfully backed up data for table {self.name}; reference: {update_id}")
        except Exception as e:
            self.logger.error(f"Could not successfully back up data for table {self.name} caused by error: {e}!")
            

    def update_table(self, new_cleaned_data, replace_vals=False):
        """
        Concatenates new cleaned data to the existing database table and optionally overwrites existing data based on primary keys.

        Parameters
        ----------
        new_cleaned_data : pandas.DataFrame
            The new cleaned data to append to the existing database table.
        replace_vals : bool, optional
            If True, any data in the existing table with overlapping primary keys with `new_cleaned_data` will be overwritten by the new data.
            Defaults to False.

        Returns
        -------
        pandas.DataFrame
            The updated database table after concatenation and potential overwrite.

        Notes
        -----
        This method performs the following operations:
        1. Loads the current database table (`self.db_table`).
        2. Optionally drops rows from `self.db_table` that have overlapping primary keys with `new_cleaned_data` if `replace_vals` is True.
        3. Concatenates `new_cleaned_data` and `self.db_table`, drops any duplicate rows based on primary keys, and saves the result back to the database table file (`data/db_tables/{self.name}.csv`).
        4. Returns the updated database table (`new_table`).
        """
        # Load latest database table
        table = self.db_table
        # Drop rows in current data if in new data
        if replace_vals:
            table = table[~table[self.primary_key].isin(new_cleaned_data[self.primary_key])].reset_index(drop=True)
        # Concatenate old and new values, drop duplicates and save to production
        new_table = pd.concat([table, new_cleaned_data], axis=0).dropna(subset=[self.primary_key])
        new_table = new_table.drop_duplicates(subset=[self.primary_key]).reset_index(drop=True)
        file_path = f"data/db_tables/{self.name}.csv"

        self.save_data(new_table, file_path)
        return new_table

    def update_db_dict(self, new_table, last_date):
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
            # Update values
            db_dict[self.name]['update'] = last_date
            db_dict[self.name]['ids'] = list(new_table[self.primary_key])
            # Save to file
            write_db_dict(db_dict)
            print("Successfully updated db_dict!")
        except Exception as e:
            self.logger.error(f"Could not update db_dict file due to error: {e}")
            
    def load_raw_data(self, file_path):
        """
        Load raw data saved by scraper class from a CSV or JSON file.

        Parameters
        ----------
        file_path : str
            Path to the file containing the raw data. Supports .csv and .json file formats.

        Returns
        -------
        pd.DataFrame or dict or None
            The loaded data as a pandas DataFrame if the file is .csv,
            or as a dictionary if the file is .json. Returns None for unsupported file types.

        Notes
        -----
        - This function checks the file extension of the provided `file_path` to determine
        whether the data is stored in CSV or JSON format.
        - It then loads and returns the data accordingly.
        - If the file extension is unsupported, it returns None.
        """
        if ".csv" in file_path:
            # Data is a pandas DataFrame, load it from a CSV file
            raw_data = pd.read_csv(file_path, index_col=0)
            print(f"DataFrame loaded from {file_path}")
        elif ".json" in file_path:
            # Data is a dictionary, load it from a JSON file
            with open(file_path, 'r') as f:
                raw_data = json.load(f)
            print(f"Dictionary loaded from {file_path}")
        else:
            # Unsupported data type
            print("Unsupported data type. Only .csv or .json files supported.")
            raw_data = None
        
        return raw_data
    
    
        

    
    

    
        


    
    