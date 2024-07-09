import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime as dt
from requests.exceptions import HTTPError, RequestException
from utils.helpers import load_data
import json
from logging_config import logger
import re


class FbrefScraper():
    """
    Base class for scraping data from football reference websites.

    This class provides the basic structure and common methods for scraping data from 
    football reference websites. It should be inherited by more specific scraper classes 
    that target particular types of data, such as teams, players, matches, etc.

    Attributes
    ----------
    name : str; optional
        Name of the class data
    data_dict : dict
        contains data for all updated tables
    db_table : dataframe
        contains data for only class table

    Main Functionality Methods
    --------------------------
    scrape_data_html(self, url, index=None):
        Scrape data from website using pd.rea_html method
    scrape_data_requests(self, url):
        Scrape data using requests and BeautifulSoup libraries.
    save_raw_data(self, raw_data, file_path):
        Saves the scraped data to a file.
    """
    # Initialization Methods
    def __init__(self, name=None):
        self.logger = logger
        self.data_dict = load_data()
        self.name = name
        self.set_db_table() 
        
    def set_db_table(self):
        """
        Load most up-to-date database table for given 
        """
        self.db_table = self.data_dict[self.name]
    
    # Main Functionality Methods
    def scrape_data_html(self, url, index=None):
        """
        Scrape data from a web URL using pd.read_html method

        Parameters
        ----------
        url : str
            The URL from which to retrieve data.
        index : int, optional
            If provided, returns the dataframe at this index from the list of tables.

        Returns
        -------
        pd.DataFrame or list of pd.DataFrame
            The scraped data as a pandas DataFrame if index is provided,
            otherwise a list of DataFrames for all tables found in the HTML.

        Logs
        ----
        Logs any web request or pandas errors encountered during the process.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
        except RequestException as e:
            self.logger.error(f"Web request error fetching data from {url}: {e}")
            return None

        try:
            tables = pd.read_html(response.text)
            if index is not None:
                return tables[index]
            return tables
        except ValueError as e:
            self.logger.error(f"Pandas error reading HTML from {url}: {e}")
            return None
        except IndexError as e:
            self.logger.error(f"IndexError: {e}. The provided index {index} is out of range for tables found in {url}.")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching data from {url}: {e}")
            return None
    
    def scrape_data_requests(self, url):
        """
        Scrape data using requests and BeautifulSoup libraries.

        Parameters
        ----------
        url : str
            The URL to fetch data from.

        Returns
        -------
        BeautifulSoup or None
            A BeautifulSoup object containing the parsed HTML content if the request is successful.
            Returns None if the request fails.
        
        Logs
        ----
        Logs any web request or pandas errors encountered during the process.

        Raises
        ------
        requests.exceptions.HTTPError
            If the HTTP request returns a status code other than 200.
        requests.exceptions.RequestException
            If there is an issue with the HTTP request.

        Notes
        -----
        This function attempts to fetch the HTML content from the specified URL using the `requests` library.
        It parses the HTML content using BeautifulSoup and returns the BeautifulSoup object.
        If the request fails for any reason (e.g., network issues, invalid URL, etc.), the function logs an error message
        and returns None.
        """
        try:
            # Get content from url
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        except HTTPError as http_err:
            self.logger.error(f"HTTP error occurred while fetching data from {url}: {http_err}")
            return None
        except RequestException as req_err:
            self.logger.error(f"Request error occurred while fetching data from {url}: {req_err}")
            return None
        except Exception as err:
            self.logger.error(f"Unexpected error occurred while fetching data from {url}: {err}")
            return None

    def save_raw_data(self, raw_data, file_path):
        """
        Save raw data to a specific file.

        Parameters
        ----------
        raw_data : dict or pd.DataFrame
            The raw data to be saved. Can be a pandas DataFrame or a dictionary.
        file_path : str
            The path (including the file name) where the data should be saved.

        Returns
        -------
        bool
            True if the data was saved successfully, False otherwise.

        Logs
        ----
        Logs the process of saving data, including successes and any errors encountered.
        """
        try:
            if isinstance(raw_data, pd.DataFrame):
                # Data is a pandas DataFrame, save it to a CSV file
                raw_data.to_csv(file_path)
                self.logger.info(f"DataFrame saved to {file_path}")
                return True
            elif isinstance(raw_data, dict):
                # Data is a dictionary, save it to a JSON file
                with open(file_path, 'w') as f:
                    json.dump(raw_data, f)
                self.logger.info(f"Dictionary saved to {file_path}")
                return True
            else:
                # Unsupported data type
                self.logger.error("Unsupported data type. Only pandas DataFrame or dictionary is supported.")
                return False
        except Exception as e:
            self.logger.error(f"Error saving data to {file_path}: {e}")
            return False
    
    # Helper Methods
    def store_error(self, params, error_message):
        """
        Log and store error information for failed scrapes.

        Parameters
        ----------
        url : str
            The URL that was being scraped when the error occurred.
        params : dict
            The parameters used for the request.
        error_message : Exception
            The exception raised during the scraping attempt.

        Returns
        -------
        None

        Notes
        -----
        The error information includes the URL, parameters, error message, and a timestamp.
        Additionally, it logs the error message using the logging module.
        """
        error_info = {
            "params": params,
            "error_message": str(error_message),
            "timestamp": dt.datetime.now().isoformat()
        }
        logging.error(f"Failed to scrape url with params {params}: {error_message}")
        return error_info
    
    def scrape_error_items(self, stored_errors, base_scraping_function):
        pass

    def append_save_retried_scrapes(self):
        pass

    def add_suffix_to_repeat_headers(self, df):
        """
        Add suffixes to column headers if they are repeated.

        Parameters
        ----------
        headers : list of str
            A list containing column header strings.

        Returns
        -------
        list of str
            A list of header strings with suffixes added to repeated column headers.

        Notes
        -----
        This function iterates over each column header in the input list. If a column header is repeated, a numerical suffix is added
        to distinguish it from the previous occurrences. The modified list of headers is then returned.
        """
        # Keep track of header name occurrences
        col_count = {}
        new_cols = []
        for col in df.columns:
            # Add distrinct suffix to repeat headers
            if col in col_count:
                col_count[col] += 1
                new_cols.append(f"{col}{col_count[col] - 1}")
            else:
                col_count[col] = 1
                new_cols.append(col)
        df.columns = new_cols
        return df

    def extract_ids(self, regex_pattern, table):
        """
        Extract IDs from an HTML stats table object based on a regex pattern.

        Parameters
        ----------
        regex_pattern : str
            The regex pattern used to extract IDs from the links in the table.
        table : bs4.element.Tag
            An HTML table object from which to extract the IDs.

        Returns
        -------
        list of str
            A list of IDs extracted from the table.

        Notes
        -----
        This function searches for all anchor tags within the given HTML table and extracts the 'href' attributes.
        It then applies the given regex pattern to these 'href' attributes to find and collect the IDs.

        Example
        """
        # Find all tags with links
        links = table.find_all('a')
        link_list = [link.get('href') for link in links]
        # Iterate through links and find ids
        ids = []
        for link in link_list:
            _id = re.findall(regex_pattern, link)
            if len(_id) == 1:
                ids.append(_id[0])
            else:
                continue
        return ids

    

        
