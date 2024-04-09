
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, VARCHAR, Time, ForeignKey, text
from sqlalchemy.exc import IntegrityError
import mysql.connector
import pandas as pd
import time
import datetime
import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st

# YOUR CHANNEL NAMES 
# 1 XXXXXX
# 2 XXXXXX
# 3 XXXXXX


# Database connection details

db_host = 'arjunx.cx8a862wca64.ap-south-1.rds.amazonawx.com:3306'
db_user = 'admin'
db_password = 'xxxxx'
db_name = 'YouTube'

# Connection URL with specifying the database
connection_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}"

# Create engine with the connection URL
engine = create_engine(connection_url)

# Establish a connection to the MySQL server
connection = engine.connect()

# SQL statement for creating the database if it doesn't exist
create_db_query = text(f"CREATE DATABASE IF NOT EXISTS {db_name}")

try:
    # Execute the SQL query to create the database
    connection.execute(create_db_query)

    print("Database created successfully!")
except Exception as e:
    print("An error occurred while creating the database:", str(e))
finally:
    # Close the connection
    connection.close()


# SQL statements for creating tables
metadata = MetaData()

# SQL statements for creating tables
create_channel_info_table = Table(
	'channel_info',metadata,
    Column('Channel_id', VARCHAR(255), primary_key=True),
    Column('Channel_Name',VARCHAR(255)),
    Column('Subscribers', Integer),
    Column('Total_videos',Integer),
    Column('Views_count', Integer),
    Column('Channel_Description', VARCHAR(255)),
    Column('Playlist_id', VARCHAR(255)),
    schema=db_name
)

create_video_info_table = Table(
	'video_info',metadata,
    Column('Channel_Id', VARCHAR(255),ForeignKey(f'{db_name}.channel_info.Channel_id')),
    Column('Channel_Name', VARCHAR(255)),
    Column('Comments',Integer),
    Column('Caption', VARCHAR(10)),
    Column('Description', VARCHAR(500)),
    Column('Definition', VARCHAR(10)),
    Column('Duration', VARCHAR(50)),
    Column('Fav_Count', Integer),
    Column('Published_date', DateTime),
    Column('Thumbnail', VARCHAR(500)),
    Column('Video_Id', VARCHAR(255), primary_key=True),
    Column('Video_Title', VARCHAR(255)),
    Column('Views', Integer),
    Column('Likes', Integer),
    Column('Dislikes', Integer),
    schema=db_name
)


create_comment_info_table = Table(
	'comment_info',metadata,
    Column('Comment_Id', VARCHAR(255), primary_key=True),
    Column('Video_id', VARCHAR(255), ForeignKey(f'{db_name}.video_info.Video_Id')),
    Column('Comment_TEXT', VARCHAR(500)),
    Column('Comment_Authour', VARCHAR(255)),
    Column('Published_Date', DateTime),
    schema=db_name
)


connection = engine.connect()
use_db_statement = text(f"USE {db_name}")
connection.execute(use_db_statement)

# Execute SQL statements to create tables
create_channel_info_table.create(engine, checkfirst=True)
create_video_info_table.create(engine, checkfirst=True)
create_comment_info_table.create(engine, checkfirst=True)
    # Check if index exists before attempting to add it

print("MySQL tables created successfully.")


# My API key's

# AIzaSyDl3W_xacI7VZbMkL4qghd9-vDny1kf3F8 # New Api key
# AIzaSyB3GMeoOMxWqVlfDIkP-vW65k1iijMskuE # old Api Key

api_key = "AIzaSyB3GMeoOMxWqVlfDIkP-vW65k1iijMskuE"  # Replace with your YouTube API key
# Build the YouTube service
youtube = build('youtube', 'v3', developerKey=api_key)

def channel_info(ch_name):
    request = youtube.search().list(
        part="snippet",
        q=ch_name,
    )
    response = request.execute()

    if not response['items']:
        print("No channel found with the given name.")
        return None

    channel_id = response['items'][0]['snippet']['channelId']

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for item in response['items']:
        ch_data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_id': item['id'],
            'Subscribers': item['statistics']['subscriberCount'],
            'Total_videos': item['statistics']['videoCount'],
            'Views_count': item['statistics']['viewCount'],
            'Channel_Description': item['snippet']['description'],
            'Playlist_id': item['contentDetails']['relatedPlaylists']['uploads']
        }
    return ch_data

def video_ids(ch_name):
    channel_data = channel_info(ch_name)
    if channel_data is None:
        return None

    ch_id = channel_data['Channel_id']

    video_ids = []
    response = youtube.channels().list(id=ch_id, part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response_vd = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response_vd['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        next_page_token = response_vd.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


def video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        retries = 3  # Number of retries
        for _ in range(retries):
            try:
                request = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=video_id
                )
                response = request.execute()

                for item in response['items']:
                    Video_Duration_ISO = item['contentDetails']['duration']
                    # Convert ISO 8601 duration to time format (HH:MM:SS)
                    duration_seconds = isodate.parse_duration(Video_Duration_ISO).total_seconds()
                    Video_Duration = str(datetime.timedelta(seconds=duration_seconds))

                    vd_data = {
                        'Channel_Id': item['snippet']['channelId'],
                        'Channel_Name': item['snippet']['channelTitle'],
                        'Comments': item.get('commentCount'),
                        'Caption': item['contentDetails']['caption'],
                        'Description': item.get('description'),
                        'Definition': item['contentDetails']['definition'],
                        'Duration': Video_Duration,
                        'Fav_Count': item['statistics'].get('favoriteCount'),
                        'Published_date': item['snippet']['publishedAt'],
                        'Likes': item['statistics'].get('likeCount'),
                        'Thumbnail': item['snippet']['thumbnails']['medium']['url'],  # Access medium-sized thumbnail URL Or Your Choice
                        'Video_Id': item['id'],
                        'Video_Title': item['snippet']['title'],
                        'Views': item['statistics'].get('viewCount')
                    }
                    video_data.append(vd_data)
                break  # Break out of the retry loop if successful
            except HttpError as e:
                if e.resp.status == 500:
                    print("Encountered 500 error. Retrying...")
                    time.sleep(1)  # Add a delay before retrying
                    continue  # Retry the request
                else:
                    raise  # Re-raise the exception if it's not a 500 error
    return video_data


def comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                C_data = {
                    'Comment_Id': item['snippet']['topLevelComment']['id'],
                    'Video_id': item['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_Text': item['snippet']['topLevelComment']['snippet'].get('textDisplay'),
                    'Comment_Authour': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Published_Date': item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                Comment_data.append(C_data)
    except Exception as e:
        print("An error occurred:", str(e))
    return Comment_data


    
st.write("Enter the YouTube channel name:")

# Get user input for channel name
channel_name = st.text_input("")

# Get channel information
channel_data = channel_info(channel_name)

# Get video IDs
video_ids_data = video_ids(channel_name)

# Get video details
video_details_data = video_info(video_ids_data)

# Get comment details
comment_details_data = comment_info(video_ids_data)


channel_df = pd.DataFrame([channel_data], columns=channel_data.keys())
video_details_df = pd.DataFrame(video_details_data, columns=video_details_data[0].keys())
comment_details_df = pd.DataFrame(comment_details_data, columns=comment_details_data[0].keys())

st.write("Channel Info:")
st.write(channel_df)

st.write("video_info:")
st.write(video_details_df)

st.write("comment_info:")
st.write(comment_details_df)

if st.button("Push Data to Database"):
     
    try:
         channel_df.to_sql('channel_info', con=engine, schema=db_name, if_exists='append', index=False)
         video_details_df.to_sql('video_info', con=engine, schema=db_name, if_exists='append', index=False)
         comment_details_df.to_sql('comment_info', con=engine, schema=db_name, if_exists='append', index=False) 
    

         print("Data pushed successfully to MySQL.")

    except Exception as e:

         print("An error occurred while pushing data to MySQL:", str(e))
         

# Streamlit Part

# Database connection details
db_host = 'arjunx.cx8a862wca64.ap-south-1.rds.amazonawx.com:3306'
db_user = 'admin'
db_password = 'xxx'
db_name = 'YouTube'


# Connection URL with specifying the database
connection_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"

# Create engine with the connection URL
engine = create_engine(connection_url)
        

# Function to execute SQL queries
def execute_query(query):
    connection = engine.connect()
    result = connection.execute(query).fetchall()
    connection.close()
    return result

# Increase sidebar width
st.markdown(
    """
    <style>
    .sidebar .sidebar-content {
        width: 350px;
    }
    </style>
    """,
    unsafe_allow_html=True
)


 # Define Streamlit app

st.sidebar.title(":movie_camera: YouTube Data Harvesting And Warehousing")
st.sidebar.subheader("Channel Information")
st.sidebar.caption("All Data Management using Python and MySQL")
st.sidebar.caption("Data Collection From YouTube")
st.sidebar.caption("API Integration")
st.sidebar.markdown("---")



# Select query

selected_query = st.sidebar.selectbox("Select a question", [
    "1. What are the names of all the videos and their corresponding channels?",
    "2. Which channels have the most number of videos, and how many videos do they have?",
    "3. What are the top 10 most viewed videos and their respective channels?",
    "4. How many comments were made on each video, and what are their corresponding video names?",
    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
    "8. What are the names of all the channels that have published videos in the year 2022?",
    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
])


if selected_query:
    if selected_query.startswith("1."):
        query = text("SELECT Video_Title, Channel_Name FROM video_info")
        column_names = ["Video Title", "Channel Name"]

    elif selected_query.startswith("2."):
        query = text("SELECT Channel_Name, COUNT(*) AS num_videos FROM video_info GROUP BY Channel_Name ORDER BY num_videos DESC")
        column_names = ["Channel Name", "Number of Videos"]

    elif selected_query.startswith("3."):
        query = text("SELECT Video_Title, Views, Channel_Name FROM video_info ORDER BY Views DESC LIMIT 10")
        column_names = ["Video Title", "Views", "Channel Name"]

    elif selected_query.startswith("4."):
        query = text("SELECT v.Video_Title, COUNT(*) AS num_comments FROM comment_info c JOIN video_info v ON c.Video_id = v.Video_id GROUP BY v.Video_Title;")
        column_names = ["Video Title", "Number of Comments"]

    elif selected_query.startswith("5."):
        query = text("SELECT Video_Title, Likes, Channel_Name FROM video_info ORDER BY Likes DESC LIMIT 10")
        column_names = ["Video Title", "Likes", "Channel Name"]

    elif selected_query.startswith("6."):
        query = text("SELECT Video_Title, SUM(Likes) AS total_likes, SUM(Dislikes) AS total_dislikes FROM video_info GROUP BY Video_Title")
        column_names = ["Video Title", "Total Likes", "Total Dislikes"]

    elif selected_query.startswith("7."):
        query = text("SELECT Channel_Name, SUM(Views) AS total_views FROM video_info GROUP BY Channel_Name")
        column_names = ["Channel Name", "Total Views"]

    elif selected_query.startswith("8."):
        query = text("SELECT DISTINCT Channel_Name FROM video_info WHERE YEAR(Published_date) = 2022")
        column_names = ["Channel Name"]

    elif selected_query.startswith("9."):
        query = text("SELECT b.Channel_Name AS Channel_Name,TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(a.Duration))), '%H:%i:%s') AS AVERAGE_VIDEO_DURATION FROM video_info a JOIN channel_info b ON a.Channel_Id = b.Channel_id GROUP BY Channel_Name ORDER BY AVERAGE_VIDEO_DURATION DESC;")
        column_names = ["Channel Name", "Average Duration"]

    elif selected_query.startswith("10."):
        query = text("SELECT Video_Title, Comments, Channel_Name FROM video_info ORDER BY Comments DESC LIMIT 10")
        column_names = ["Video Title", "Comments", "Channel Name"]

    # Execute the query
    result_data = execute_query(query)
    
    # Display the results
    if result_data:
        df = pd.DataFrame(result_data, columns=column_names)
        st.dataframe(df)
        
    else:
        st.warning("No data available for the selected query.")