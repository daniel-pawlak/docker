import mariadb
import json
import datetime
import time
import requests
import pandas as pd
from urllib.parse import urlparse
import logging
import schedule
import pytz
from time import perf_counter
import configparser
import os

warsaw_tz = pytz.timezone("Europe/Warsaw")

logging.basicConfig(level=logging.INFO)


def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = func(*args, **kwargs)
        end_time = perf_counter()
        run_time = end_time - start_time
        end_hour = datetime.datetime.now(warsaw_tz).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(
            f"Execution time of {func.__name__}: {run_time:.2f} seconds on {end_hour}, which is {time.strftime('%H:%M:%S', time.gmtime(run_time))}."
        )
        return result

    return wrapper


class GetData:
    def __init__(self) -> str:
        # Read config.ini file
        __location__ = os.path.realpath(
            os.path.join(os.getcwd(), os.path.dirname(__file__))
        )

        config_path = configparser.ConfigParser()
        config_path.read(os.path.join(__location__, "config.ini"))

        # Database credentials
        self.user_db = config_path["database"]["username"]
        self.password_db = config_path["database"]["password"]
        self.host_db = config_path["database"]["host"]
        self.port_db = int(config_path["database"]["port"])

        # Access credentials
        self.access_endpoint = config_path["access"]["endpoint"]
        self.access_username = config_path["access"]["username"]
        self.access_password = config_path["access"]["password"]

        # Authorization and access token
        self.authorization, self.access_token = self.get_access_token()

    def get_access_token(self):
        """
        This code defines a function called get_access_token that retrieves an access token for a user. The function takes three arguments: username
        and password which are the user's login credentials, and endpoint which is the API endpoint the user wants to access. The function first
        parses the given endpoint URL using the urlparse function from the urllib.parse module. It then extracts the domain name from the parsed
        URL using the netloc attribute. The extracted domain name is used to set various headers and payload values required for authentication.
        The function sets the required headers such as authority, accept, accept-encoding, accept-language, dnt, origin, referer, sec-ch-ua,
        sec-ch-ua-mobile, sec-ch-ua-platform, sec-fetch-dest, sec-fetch-mode, sec-fetch-site, Content-Type. It also sets the payload with grant_type,
        username, and password. The function then sends a POST request to /login endpoint using the given credentials and headers.
        If get_token.status_code is 200, the access token is extracted from the returned JSON using access_token key, Bearer authentication is
        prepended to this token in authorization, and then it is returned. If there is an error, the function prints an error message with the
        text of the response. The code makes use of the requests module to send HTTP requests.
        """
        parsed_url = urlparse(self.access_endpoint)

        domain_name = parsed_url.netloc

        headers = {
            "authority": domain_name,
            "accept": "application/json",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "dnt": "1",
            "origin": f"https://{domain_name}",
            "referer": f"https://{domain_name}/",
            "sec-ch-ua": '"Microsoft Edge";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "Content-Type": "application/json",
        }

        payload = {
            "grant_type": "password",
            "username": self.access_username,
            "password": self.access_password,
        }

        get_token = requests.post(
            url=self.access_endpoint + "/login", headers=headers, data=payload
        )

        if get_token.status_code == 200:
            access_token = get_token.json()["access_token"]
            authorization = f"Bearer {access_token}"
            return authorization, access_token
        else:
            print("Error obtaining access token:", get_token.text)

    @measure_execution_time
    def get_data_to_db(self):
        """
        This function is to download data and place it in database.
        """

        time = datetime.datetime.now(warsaw_tz).strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Function started at {time}")
        # Connect to database
        try:
            connection = mariadb.connect(
                user=self.user_db,
                password=self.password_db,
                host=self.host_db,
                port=self.port_db,
                database="your db name",
            )
            cursor = connection.cursor()
        except mariadb.Error as e:
            logging.warning(f"Error connecting to MariaDB Platform: {e} at {time}")

        # Define the SQL INSERT statement
        insert_row_sql = """
                INSERT INTO YourTableName (Col1, Col2, Col3, UpdatedAt, InsertedAt)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                Col1 = VALUES(Col1),
                Col2 = VALUES(Col2),
                Col3 = VALUES(Col3),
                UpdatedAt = NOW()
                """

        # Get the data for all stations
        # df = self.<name of your function to get data>

        df = pd.DataFrame(data=None) # sample df

        # Loop through all charge points andget its' details
        for index, row in df.iterrows():
            logging.info(f"Current iteration: {index}")

            col1 = row["Col1 value"]
            col2 = row["Col2 value"]
            col3 = row["Col3 value"]

            date_time = datetime.datetime.now(warsaw_tz).strftime("%Y-%m-%d %H:%M:%S")
            time = datetime.datetime.now(warsaw_tz).strftime("%H:%M:%S")

            data_to_insert = (
                col1,
                col2,
                col3,
                date_time,
                date_time,
            )

            try:
                # Execute the INSERT statement
                cursor.execute(insert_row_sql, data_to_insert)

                # Commit the transaction to save the changes
                connection.commit()

                logging.info(f"Row inserted successfully at {time}")

            except Exception as e:
                # Handle any errors
                logging.warning(f"Error {e} at {time}")
                print(data_to_insert)

        cursor.close()
        connection.close()


if __name__ == "__main__":
    current_time = datetime.datetime.now(warsaw_tz).strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Script started at {current_time}")

    # Create an instance of GetData
    data_fetcher = GetData()

    # Schedule the task to run once a week on Monday at 4:00 PM (adjust as needed)
    schedule.every().monday.at("16:00", tz=warsaw_tz).do(
        data_fetcher.get_data_to_db
    )

    while True:
        schedule.run_pending()
        time.sleep(1)
