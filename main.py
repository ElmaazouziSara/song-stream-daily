# --- IMPORTS.
import argparse
from datetime import datetime
import logging
import os
import pandas as pd
import schedule
import time


# --- INIT GLOBAL VARIABLES.
TODAY = datetime.now()

LOG_FOLDER = "logs/"
OUTPUT_FOLDER = "output/"

MONTH = '{:02d}'.format(TODAY.month)
DAY = '{:02d}'.format(TODAY.day)

LOG_FILE_NAME = f"listen-{TODAY.year}{MONTH}{DAY}.log"
TOP50_COUNTRY_OUTPUT_FILE_NAME = f"country_top50{TODAY.year}{MONTH}{DAY}.txt"
TOP50_USER_OUTPUT_FILE_NAME = f"user_top50{TODAY.year}{MONTH}{DAY}.txt"


# --- START DATA MANIPULATION FUNCTIONS.

def read_log_file(log_file: str) -> None:
    """
    Read data from the log file.

    :param log_file: A text file named listen-YYYYMMDD.log that contains the logs of all
    listening streams made on Deezer on that date. These logs are formatted as follows:
        - There is a row per stream (1 listening).
        - Each row is in the following format: sng_id|user_id|country.
            . sng_id: integer.
            . user_id: integer.
            . country: 2 characters string (Ex: FR, GB, BE, ...).
    """
    with open(log_file, 'r') as file:
        for line in file:
            try:
                song_id, user_id, country = line.strip().split('|')
                yield [int(song_id), int(user_id), country]
            except:
                logging.warning(f"An issue occurred while reading the line: {line}. "
                                f"Some data might go missing.")
                continue


def convert_into_pandas_dataframe(data_entry: list, columns: list) -> pd.core.frame.DataFrame:
    """
    Converts a raw list of data into a pandas DataFrame.
    :param data_entry: list of data. (EX: [[1233, 3425, DE], [5667, 7662, FR]])
    :param columns: list of columns to have in the dataframe.
    """
    return pd.DataFrame(data_entry, columns=columns)


def cleanup_streams_dataframe(streams_dataframe: pd.core.frame.DataFrame) -> None:
    """
    Cleans-up the given dataframe in the following order:
        1. Remove Nan values.
        2. Remove rows with country code length > 2 (EX: GBGBGB...).
        3. Remove rows with user_id < 0 (EX: -1...).
        3. Remove rows with song_id < 0 (EX: -1...).
    """
    streams_dataframe.dropna().reset_index(drop=True)
    streams_dataframe[streams_dataframe['country'].str.len() == 2].reset_index()
    streams_dataframe[streams_dataframe['user_id'] > 0].reset_index()
    streams_dataframe[streams_dataframe['song_id'] > 0].reset_index()


def generate_column_wise_top_50(streams_dataframe: pd.core.frame.DataFrame,
                                column_name: str) -> pd.core.frame.DataFrame:
    """
    Generates a column-wise ordered dataframe of given one.
    :param streams_dataframe: The actual dataframe to be processed.
    :param column_name: The column to use as a base for the aggregation.
    """
    top_songs_df = \
        streams_dataframe.groupby([column_name, "song_id"]).size().reset_index(name="count")

    return top_songs_df.groupby(column_name).apply(
        lambda x: x.nlargest(50, "count")).reset_index(drop=True)


def write_results_to_file(output_file_path: str, entity_name: str, top50_dataframe: pd.core.frame.DataFrame) -> None:
    """
    Writes the resulting top 50 list of a given entity name into a given file.
        1. Creates the file if not existing.
        2. Generates the TOP 50 list and loops through every line to format the reslts.
        3. Write the list into the given file.
    :param output_file_path: Path of the output file
    :param entity_name: Name of the entity to which the top 50 list is generated.
    :param top50_dataframe: The aggregated top 50 dataframe.
    """
    with open(output_file_path, "w+") as file:
        for entity in top50_dataframe[entity_name].unique():
            try:
                lines = []
                file.write(f"{entity}|")

                top_songs = top50_dataframe[top50_dataframe[entity_name] == entity].head(50)

                for _, row in top_songs.iterrows():
                    lines.append(f"{row['song_id']}:{row['count']}")

                file.write(f"{lines[0]},".join(lines[1:]))
                file.write("\n")
            except:
                logging.warning(f"An issue occured while writing the results of {entity} to the given file. "
                                f"Some data might go missing.")
                continue


def scheduler_function() -> None:
    """
    A scheduler function that would configure executing the script each day.
    """
    RUNNING_TIME = "00:00"
    # Schedule the function to run daily.
    schedule.every().day.at(RUNNING_TIME).do(main)

    while True:
        # This loop keeps the script running continuously.
        schedule.run_pending()
        time.sleep(1)

# --- END DATA MANIPULATION FUNCTIONS.


def main() -> None:
    """
    Run the complete program.
    """
    # 1: READ LOG FILE & CONVERT IT TO A DATAFRAME
    input_file_path = os.path.join(LOG_FOLDER, LOG_FILE_NAME)
    logs_data_entry = read_log_file(input_file_path)
    streams_dataframe = convert_into_pandas_dataframe(logs_data_entry, ["song_id", "user_id", "country"])

    # 2: AGGREGATE THE STREAMS
    cleanup_streams_dataframe(streams_dataframe)

    # 3: GENERATE COUNTRY-WISE TOP 50 SONGS & SAVE THEM TO A FILE
    top50_songs_per_country = generate_column_wise_top_50(streams_dataframe, "country")

    country_file_path = os.path.join(OUTPUT_FOLDER, TOP50_COUNTRY_OUTPUT_FILE_NAME)
    write_results_to_file(country_file_path, "country", top50_songs_per_country)

    # 4: GENERATE USER-WISE TOP 50 SONGS
    top50_songs_per_user = generate_column_wise_top_50(streams_dataframe, "user_id")

    user_file_path = os.path.join(OUTPUT_FOLDER, TOP50_USER_OUTPUT_FILE_NAME)
    write_results_to_file(user_file_path, "user_id", top50_songs_per_user)


if __name__ == "__main__":
    argParser = argparse.ArgumentParser()
    argParser.add_argument("--auto", help="Flag to run the script automatically everyday.", action='store_true')

    args = argParser.parse_args(['--auto'])

    if args.auto:
        scheduler_function()

    else:
        main()
