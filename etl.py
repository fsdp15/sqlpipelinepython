import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ='series')
    df2 = df.loc[["song_id", "title", "artist_id", "year", "duration"]]
    df2 = df2.to_numpy()
    song_data = df2.tolist()

    # insert song record
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artist_data = artist_data.to_numpy().tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, typ='series', lines=True)
    df_bkp = df

    # filter by NextSong action
    for index, row in df.iteritems():
        if row["page"].lower() != "nextsong":
            df.drop(labels=index, axis =0, inplace=True)
    df.dropna(inplace=True)

    # convert timestamp column to datetime
    t = []
    for index, row in df.iteritems():
        t.append(row["ts"])
    td = pd.DataFrame (list(zip(t, t)),columns = ["time", "timestamp"])
    td["time"] = pd.to_datetime(td["time"])
    
    # insert time data records
    time_data = [td["timestamp"], td["time"].dt.hour, td["time"].dt.day, td["time"].dt.isocalendar().week, td["time"].dt.month, 
            td["time"].dt.year, td["time"].dt.weekday]

    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")

    time_df = pd.DataFrame(list(zip(time_data[0], time_data[1], time_data[2], time_data[3],
                           time_data[4], time_data[5], time_data[6])), columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.read_json(filepath, typ='series', lines=True)

    t = []
    for index, row in user_df.iteritems():
        s = []
        if row["userId"] == "":
            row["userId"] = None
        s.append(row["userId"])
        s.append(row["firstName"])
        s.append(row["lastName"])
        s.append(row["gender"])
        s.append(row["level"])
        t.append(s)

    user_df = pd.DataFrame (t, columns = ["userId", "firstName", "lastName", "gender", "level"])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_bkp.iteritems():

        s = []
        s.append(index)
        s.append(row["ts"])
        s.append(int(row["userId"]))
        s.append(row["level"])
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row["song"], row["artist"], row["length"]))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        s.append(songid)
        s.append(artistid)
        s.append(row["sessionId"])
        s.append(row["location"])
        s.append(row["userAgent"])

        cur.execute(songplay_table_insert, s)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=felipe password=Jwoa0RGD")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()