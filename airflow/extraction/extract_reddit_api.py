import configparser
import datetime
import pandas as pd
import pathlib
import praw
import sys
import os
import numpy as np
from validation import validate_input

# Read Configuration File
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "configuration.conf"
parser.read(f"{script_path}/{config_file}")

# Configuration Variables
SECRET = parser.get("reddit_config", "secret")  
CLIENT_ID = parser.get("reddit_config", "client_id")

# Options for extracting data from PRAW
SUBREDDIT = "dataengineering"
TIME_FILTER = "day"
LIMIT = None

# Fields that will be extracted from Reddit.
# Check PRAW documentation for additional fields.
# NOTE: if you change these, you'll need to update the create table
# sql query in the upload_aws_redshift.py file
POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
)

# TODO Improve error handling
# Use command line argument as output file
# name and also store as column value
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Error with file input. Error {e}")
    sys.exit(1)
date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")


def main():
    """Extract Reddit data and load to CSV"""
    validate_input(output_name)
    reddit_instance = api_connect()
    subreddit_posts_object = subreddit_posts(reddit_instance)
    extracted_data = extract_data(subreddit_posts_object)
    transformed_data = transform_basic(extracted_data)
    load_to_csv(transformed_data)


# TODO: Improve error handling
def api_connect():
    """Connect to Reddit API"""
    try:
        instance = praw.Reddit(
            client_id=CLIENT_ID, client_secret=SECRET, user_agent="My User Agent"
        )
        return instance
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")
        sys.exit(1)


# TODO: Improve error handling
def subreddit_posts(reddit_instance):
    """Create posts object for Reddit instance"""
    try:
        subreddit = reddit_instance.subreddit(SUBREDDIT)
        posts = subreddit.top(time_filter=TIME_FILTER, limit=LIMIT)
        return posts
    except Exception as e:
        print(f"There's been an issue. Error: {e}")
        sys.exit(1)


# TODO: Improve error handling
def extract_data(posts):
    """Extract Data to Pandas DataFrame object"""
    list_of_items = []
    try:
        for idx, submission in enumerate(posts):
            to_dict = vars(submission)

            # 🔍 In ra dữ liệu chi tiết của submission đầu tiên để xem API trả ra những gì
            # if idx == 0:
            #     print("🔎 Dữ liệu đầy đủ của submission đầu tiên:")
            #     for key, value in to_dict.items():
            #         print(f"{key}: {value}")

            sub_dict = {field: to_dict[field] for field in POST_FIELDS}
            list_of_items.append(sub_dict)
        extracted_data_df = pd.DataFrame(list_of_items)
    except Exception as e:
        print(f"There has been an issue. Error {e}")
        sys.exit(1)

    return extracted_data_df

 # TODO: Remove all but the edited line, as not necessary. For edited line, rather 
 # than force as boolean, keep date-time of last edit and set all else to None.
def transform_basic(df):
    """Some basic transformation of data. To be refactored at a later point."""

    # Convert epoch to UTC
    df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s")
    # Fields don't appear to return as booleans (e.g. False or Epoch time). Needs further investigation but forcing as False or True for now.
    df["over_18"] = np.where(
        (df["over_18"] == "False") | (df["over_18"] == False), False, True
    ).astype(bool)
    df["edited"] = np.where(
        (df["edited"] == "False") | (df["edited"] == False), False, True
    ).astype(bool)
    df["spoiler"] = np.where(
        (df["spoiler"] == "False") | (df["spoiler"] == False), False, True
    ).astype(bool)
    df["stickied"] = np.where(
        (df["stickied"] == "False") | (df["stickied"] == False), False, True
    ).astype(bool)
    return df


def load_to_csv(extracted_data_df):
    """Save extracted data to CSV file in /airflow/raw_data folder"""
    if is_local():
        # Nếu local thì lưu ra thư mục raw_data/
        output_dir = script_path.parent / "raw_data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{output_name}.csv"
    else:
        # Nếu trên server thì lưu vào /tmp/
        output_path = pathlib.Path("/tmp") / f"{output_name}.csv"

    extracted_data_df.to_csv(output_path, index=False)
    print(f"✅ Dữ liệu raw đã lưu tại: {output_path}")

def is_local():
    """Detect environment: Local hoặc Airflow"""
    # Nếu chạy từ thư mục có chứa "airflow", hoặc environment có biến 'AIRFLOW_HOME', thì không phải local
    return "AIRFLOW_HOME" not in os.environ

if __name__ == "__main__":
    main()
