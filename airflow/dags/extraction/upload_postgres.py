import psycopg2
import configparser
import pathlib
import sys
import pandas as pd
from validation import validate_input

"""
Part of DAG. Take Reddit data and upload to PostgreSQL. Takes one command line argument of format YYYYMMDD. 
This represents the file downloaded from Reddit, which will be in the /tmp folder.
"""

# Load DB credentials
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_path = script_path.parent.parent.parent / "configuration.conf"
parser.read(config_path)

POSTGRES_HOST = parser.get("postgres_config", "host")
POSTGRES_PORT = parser.get("postgres_config", "port")
POSTGRES_DB = parser.get("postgres_config", "database")
POSTGRES_USER = parser.get("postgres_config", "user")
POSTGRES_PASSWORD = parser.get("postgres_config", "password")

try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error {e}")
    sys.exit(1)

FILENAME = f"{output_name}.csv"
FILE_PATH = f"../raw_data/{FILENAME}"

def main():
    """Upload CSV file to PostgreSQL"""
    validate_input(output_name)
    df = load_csv_file(FILE_PATH)
    conn = connect_to_postgres()
    upload_dataframe_to_postgres(df, conn)
    conn.close()

def connect_to_postgres():
    """Connect to PostgreSQL Database"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Can't connect to PostgreSQL. Error: {e}")
        sys.exit(1)

def load_csv_file(filepath):
    """Load CSV file into a pandas DataFrame"""
    try:
        df = pd.read_csv(filepath)
        return df
    except Exception as e:
        print(f"Error loading CSV file. Error: {e}")
        sys.exit(1)

def upload_dataframe_to_postgres(df, conn):
    """Upload pandas DataFrame into PostgreSQL Table"""
    try:
        table_name = "reddit_data"
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS reddit_data (
            id TEXT ,
            title TEXT,
            score INTEGER,
            num_comments INTEGER,
            author TEXT,
            created_utc TIMESTAMP,
            url TEXT,
            upvote_ratio FLOAT,
            over_18 BOOLEAN,
            edited BOOLEAN,
            spoiler BOOLEAN,
            stickied BOOLEAN
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert dữ liệu vào bảng
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO reddit_data (
                id, title, score, num_comments, author,
                created_utc, url, upvote_ratio, over_18,
                edited, spoiler, stickied
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                row['id'], row['title'], row['score'], row['num_comments'], row['author'],
                row['created_utc'], row['url'], row['upvote_ratio'], row['over_18'],
                row['edited'], row['spoiler'], row['stickied']
            ))

        conn.commit()
        print(f"Upload completed successfully. {len(df)} rows inserted.")

    except Exception as e:
        print(f"Error uploading data to PostgreSQL. Error: {e}")
        conn.rollback()
        sys.exit(1)

if __name__ == "__main__":
    main()
