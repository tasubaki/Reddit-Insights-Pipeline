import psycopg2
import configparser
import pathlib
import sys
import pandas as pd
from validation import validate_input


# Load DB credentials
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "configuration.conf"
parser.read(f"{script_path}/{config_file}")

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
FILE_PATH = f"/tmp/{FILENAME}"

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
            id SERIAL PRIMARY KEY,
            post_id VARCHAR(50) UNIQUE,
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
            stickied BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert dữ liệu vào bảng
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO reddit_data (
                post_id, title, score, num_comments, author,
                created_utc, url, upvote_ratio, over_18,
                edited, spoiler, stickied
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (post_id) DO UPDATE SET
                title = EXCLUDED.title,
                score = EXCLUDED.score,
                num_comments = EXCLUDED.num_comments,
                author = EXCLUDED.author,
                created_utc = EXCLUDED.created_utc,
                url = EXCLUDED.url,
                upvote_ratio = EXCLUDED.upvote_ratio,
                over_18 = EXCLUDED.over_18,
                edited = EXCLUDED.edited,
                spoiler = EXCLUDED.spoiler,
                stickied = EXCLUDED.stickied,
                updated_at = CURRENT_TIMESTAMP;
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


