from src.DbConnector import DbConnector
from tabulate import tabulate
from sqlalchemy import create_engine, event


class DbUtils:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)

    def create_all_tables(self):
        drop_all_tables()
        try:
            self.create_coll("User")
            self.create_coll("Activity")
        except Exception as e:
            print("ERROR while accessing database", e)

    def insert_users(self, datapoints):
        for data in datapoints:
            query = "INSERT INTO %s (id, has_labels) VALUES ('%s', '%s')"
            self.cursor.execute(query % ("User", data[0], data[1]))
        self.db.commit()

    def insert_trackpoints(self, df):
        # Using a different engine to make bulk inserting of TrackPoints faster
        engine = create_engine("mysql+pymysql://root:password@127.0.0.1/db")

        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(cursor, executemany):
            if executemany:
                cursor.fast_executemany = True

        df.to_sql(name="TrackPoint", con=engine, if_exists="append", index=False)

    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        query = """INSERT INTO %s (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES("%s", "%s", "%s", "%s")
        """
        self.cursor.execute(query % ("Activity", user_id, transportation_mode, start_date_time, end_date_time))
        self.db.commit()
        return self.cursor.lastrowid


def _create_table_user(db, cursor):
    query = """CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(30) NOT NULL PRIMARY KEY,
    has_labels BOOL
    )
    """
    cursor.execute(query % 'User')
    db.commit()


def _create_table_activity(db, cursor):
    query = """ CREATE TABLE IF NOT EXISTS %s (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    user_id VARCHAR(30),
    transportation_mode VARCHAR(255),
    start_date_time DATETIME,
    end_date_time DATETIME,
    FOREIGN KEY(user_id) REFERENCES User(id)
    )        
    """
    cursor.execute(query % 'Activity')
    db.commit()


def _create_table_track_point(db, cursor):
    query = """ CREATE TABLE IF NOT EXISTS %s (
    id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    activity_id INT,
    lat DOUBLE,
    lon DOUBLE,
    altitude INT,
    date_days DOUBLE,
    date_time DATETIME,
    FOREIGN KEY(activity_id) REFERENCES Activity(id)
    )        
    """
    cursor.execute(query % 'TrackPoint')
    db.commit()


def show_tables(cursor):
    cursor.execute("SHOW TABLES")
    rows = cursor.fetchall()
    print(tabulate(rows, headers=cursor.column_names))


def drop_all_tables():
    connection = DbConnector()
    db = connection.db_connection
    cursor = connection.cursor

    query = "DROP TABLE IF EXISTS %s"
    cursor.execute(query % "TrackPoint")
    cursor.execute(query % "Activity")
    cursor.execute(query % "User")
    db.commit()

