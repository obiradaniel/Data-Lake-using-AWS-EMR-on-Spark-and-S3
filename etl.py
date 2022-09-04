import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,\
    date_format, dayofweek, quarter
from pyspark.sql.types import LongType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file, Tested with a substet
    song_data = input_data + "song_data/A/A/*/*.json"

    song_data_full = input_data + "song_data/*/*/*/*.json"

    # read song data file
    startTime = datetime.now()
    print("Reading Song Data from S3..........")
    df = spark.read.json(song_data)
    diff = datetime.now() - startTime
    print("Reading Song Data Completed, {}; Time running.\n".format(diff))

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year',
                            'duration')
    songs_table = songs_table.distinct()

    # write songs table to parquet files partitioned by year and artist
    startTime = datetime.now()
    print("Writing Song Data to Parquet S3..........")
    filetime = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
    filename = output_data + 'Songs/' + filetime + '_Songs.parquet'
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite")\
        .parquet(filename)
    diff = datetime.now() - startTime
    print("Writing Song Data to S3 Complete.\n{}; Time running.".format(diff))
    print(filename, "Successfully written.\n")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude')
    artists_table = artists_table.distinct()

    # write artists table to parquet files
    startTime = datetime.now()
    print("Writing Artists Data to Parquet S3..........")
    filetime = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
    filename = output_data + 'Artists/' + filetime + '_Artists.parquet'
    artists_table.write.mode("overwrite").parquet(filename)
    diff = datetime.now() - startTime
    print("Writing Artists Data to S3 Complete.\n{}; Time running."
          .format(diff))
    print(filename, "Successfully written.\n")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    startTime = datetime.now()
    print("Reading Log Data from S3..........")
    df = spark.read.json(log_data)
    diff = datetime.now() - startTime
    print("Reading Log Data Completed, {}; Time running.\n".format(diff))

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level')
    users_table = users_table.distinct()

    # write users table to parquet files
    print("Writing Users Data to Parquet S3..........")
    startTime = datetime.now()
    filetime = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
    filename = output_data + 'Users/' + filetime + '_Users.parquet'
    users_table.write.mode("overwrite").parquet(filename)
    diff = datetime.now() - startTime
    print("Writing Users Data to S3 Complete.\n{}; Time running."
          .format(diff))
    print(filename, "Successfully written.\n")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda timestamp: timestamp/1000, LongType())
    df = df.withColumn("TimeStamp", get_timestamp("ts"))

    # create datetime column from original timestamp column
    # Add time columns year, month, e.t.c
    get_datetime = udf(lambda timestamp:
                       datetime.fromtimestamp(timestamp)
                       .strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("StartTime", get_datetime("TimeStamp"))

    df = df.withColumn("Hour", hour('StartTime'))
    df = df.withColumn("WeekDay", dayofweek('StartTime'))
    df = df.withColumn("DayOfMonth", dayofmonth('StartTime'))
    df = df.withColumn("WeekOfYear", weekofyear('StartTime'))
    df = df.withColumn("Month", month('StartTime'))
    df = df.withColumn("Quarter", quarter('StartTime'))
    df = df.withColumn("Year", year('StartTime'))

    # extract columns to create time table
    time_table = df.select('StartTime', 'Hour', 'WeekDay', 'DayOfMonth',
                           'WeekOfYear', 'Month', 'Quarter', 'Year')
    time_table = time_table.distinct()

    # write time table to parquet files partitioned by year and month
    print("Writing Time Data to Parquet S3..........")
    startTime = datetime.now()
    filetime = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
    filename = output_data + 'Time/' + filetime + '_Time.parquet'
    time_table.partitionBy("Year", "Month").write.mode("overwrite")\
        .parquet(filename)
    diff = datetime.now() - startTime
    print("Writing Time Data to S3 Complete.\n{}; Time running."
          .format(diff))
    print(filename, "Successfully written.\n")

    # read in song data to use for songplays table
    print("Reading Songs And Artists from previous S3 parquet..........")
    startTime = datetime.now()
    songs_parquet = "s3a://od-udacity-out/Songs/2022-09-04 13_56_05_Songs.parquet"
    songs = spark.read.parquet(songs_parquet)
    artists_parquet = "s3a://od-udacity-out/Artists/2022-09-04 12_03_29_Artists.parquet"
    artists = spark.read.parquet(artists_parquet)
    diff = datetime.now() - startTime
    print("Reading Song and Artists Data Completed, {}; Time running.\n"
          .format(diff))

    # extract columns from joined song and log datasets to create
    # songplays table
    print("Processing SongPlays Table")
    startTime = datetime.now()
    song_listens = df.select('StartTime', 'userId', 'level', 'length', 'song',
                             'artist', 'sessionId', 'location', 'userAgent')

    songs.createOrReplaceTempView("Songs")
    artists.createOrReplaceTempView("Artists")
    song_listens.createOrReplaceTempView("Listens")

    Query1 = """
                SELECT
                    S.song_id, S.title, S.artist_id, S.year, S.duration,
                    A.artist_name
                FROM
                    Songs S JOIN Artists A
                ON
                    S.artist_id = A.artist_id
    """

    Songs_Artist_Names = spark.sql(Query1)

    Songs_Artist_Names.createOrReplaceTempView("SongsArtists")
    Query2 = """
        SELECT
            *
        FROM
            SongsArtists JOIN Listens
        ON
            (SongsArtists.title = Listens.song
             AND
             SongsArtists.artist_name = Listens.artist)
        WHERE
            (ABS(SongsArtists.duration - Listens.length) < 0.5)
    """
    song_plays = spark.sql(Query2)

    songplays_table = song_plays.select('StartTime', 'userId', 'level',
                                        'song_id', 'artist_id', 'sessionId',
                                        'location', 'userAgent')
    songplays_table = songplays_table.withColumn("Month", hour('StartTime'))
    songplays_table = songplays_table.withColumn("Year", year('StartTime'))
    diff = datetime.now() - startTime
    print("Processing SongPlays Data Completed, {}; Time running.\n"
          .format(diff))

    # write songplays table to parquet files partitioned by year and month
    print("Writing SongPlays Data to Parquet S3..........")
    startTime = datetime.now()
    filetime = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
    filename = output_data + 'SongPlays/' + filetime + '_SongPlays.parquet'
    songplays_table.partitionBy("Year", "Month").write.mode("overwrite")\
        .parquet(filename)
    diff = datetime.now() - startTime
    print("Writing SongPlays Data to S3 Complete.\n{}; Time running."
          .format(diff))
    print(filename, "Successfully written.\n")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://od-udacity-out/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
