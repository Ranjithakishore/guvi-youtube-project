import googleapiclient.discovery
import pandas as pd
import mysql.connector as sql
import re
from datetime import datetime, timedelta
import streamlit as st

# Reference data
# Channel_id=['UCaSm52FdLvuKOIWMuyIWE6w',
#             'UC_2OGS-RMUkccZwsby940oA',
#             'UCwR9fNuZ6h1tjgnPI1QHSMQ',
#             'UCsvA9blwi3td4O-MujAmsdA',
#             'UCtYIA8Wxbt-tvo9Ovyow6xg',
#             'UCmygEzOh7uDtHjzjFr8QFUA',
#             'UCEZvjCk6yoPOfDWDgiWjnVQ',
#             'UC18D1r09JkOO5chUmMkhlvQ',
#             'UCAwv_Uc3b9JOB1-Sbf32R0w',
#             'UCjvd2JmIWGsEWPmLifUS4PA']

#API key connection
def Api_connect(): 
  api="AIzaSyCkv1YpHnPrLKxR6r7ou0Mf042_VLHh22k"
  api_service_name = "youtube"
  api_version = "v3"
  youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=api)
  return youtube

youtube=Api_connect()

#get channels information
def get_channel_info(Channel_id):
    Channel_Details = []
    request = youtube.channels().list(
                                    part="statistics,snippet,contentDetails",
                                    id=Channel_id
                                    )
    response = request.execute()
    for i in response['items']:
        Channel_Name = {
            'Channel_Name':i['snippet']['localized']['title'],
            'Channel_Id':i['id'],
            'Subscription_Count':i['statistics']['subscriberCount'],
            'Channel_Views':i['statistics']['viewCount'],
            'Channel_Description':i['snippet']['description'],
            'Playlist_Id':i['contentDetails']['relatedPlaylists']['uploads']
        }
        Channel_Details.append(Channel_Name)
    return  Channel_Details   

#get playlist_ids
def get_playlist_ids(Channel_id):
  next_page_token=None
  All_data=[]
  while True:
    request = youtube.playlists().list(
                                        part="snippet,contentDetails",
                                        channelId=Channel_id,
                                        maxResults=50,
                                        pageToken=next_page_token)
    response1=request.execute()
    for item in response1['items']:
        Playlist_Id=item['id']
        All_data.append(Playlist_Id)
      
    next_page_token=response1.get('nextPageToken')
    if next_page_token is None:
        break
  return All_data

#get video_ids
def get_video_ids(playlist_id):
  next_page_token=None
  video_ids=[]
  
  while True:
    response2 = youtube.playlistItems().list(
                                        part="snippet",
                                        playlistId=playlist_id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
    for i in range(len(response2['items'])):
        video_ids.append(response2['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token=response2.get('nextPageToken')
    if next_page_token is None:
        break
  return video_ids

#get video information
def get_video_info(video_ids):
  video_data=[]
  try:
    for video_id in video_ids:
      request = youtube.videos().list(
        part='snippet,ContentDetails,statistics',
        id=video_id
        )
      response = request.execute()

      for item in response['items']:
          data=dict(channel_id=item['snippet']['channelId'],
                    channel_name=item['snippet']['channelTitle'],
                    Video_id=item['id'],
                    Video_Name=item['snippet']['title'],
                    Video_Discription=item['snippet'].get('description'),
                    Tags=item['snippet'].get('tags'),
                    Published_At=item['snippet']['publishedAt'],
                    View_Count=item['statistics'].get('viewCount'),
                    Like_Count=item['statistics'].get('likeCount'),
                    Dislike_Count=item.get('dislikeCount'),
                    Favourite_Count=item['statistics'].get('favouriteCount'),
                    Comment_Count=item['statistics'].get('commentCount'),
                    Duration=item['contentDetails']['duration'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Caption_Status=item['contentDetails']['caption']
                    )
          video_data.append(data)
    return video_data
  except Exception as e:
     print(e)

#get comment information
def get_comment_info(video_ids): 
    comment_data=[]
    try:
        for video_id in video_ids:
            try:
                request=youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=50
                )
                response=request.execute()
                for i in response['items']:
                    data=dict(Video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                                com_id=i['snippet']['topLevelComment']['id'],
                                com_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                com_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                com_published=i['snippet']['topLevelComment']['snippet']['publishedAt'].replace('Z',''))
                    comment_data.append(data)
            except:
                pass
    except Exception as e:
        print(e)
    
    return comment_data

# insert channel_details 
def insert_channel_details(channel_details):
    try:
        mydb = sql.connect(
                    host="127.0.0.1",
                    user="root",
                    password="root",
                    database="youtube"
                    )
        cur = mydb.cursor(buffered=True)
        create_channel_table='''create table if not exists channels (Channel_Name varchar(100),
                                                                            Channel_Id varchar(50) primary key,
                                                                            Channel_Description text,
                                                                            Channel_Views bigint,
                                                                            Subscription_Count bigint,
                                                                            Playlist_Id varchar(100))'''
        cur.execute(create_channel_table)
        mydb.commit()
        print('channels table created successfully')
    except sql.Error as err:
        print(f'Error: {err}')

    for index,row in channel_details.iterrows():
        channel_query='''insert into channels(Channel_Name,
                                        Channel_Id,
                                        Channel_Description,
                                        Channel_Views,
                                        Subscription_Count,
                                        Playlist_Id)

                                        values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Channel_Description'],
                row['Channel_Views'],
                row['Subscription_Count'],
                row['Playlist_Id'])

        cur.execute(channel_query,values)
        mydb.commit()

# reformatted duration in readable string
def parse_duration(duration):
    # Regex to match the ISO 8601 duration format
    pattern = re.compile(r'P(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)')
    match = pattern.match(duration)
    
    if not match:
        return '00:00:00'  # Return a default value if the format is incorrect

    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)

    total_seconds = timedelta(hours=hours, minutes=minutes, seconds=seconds).total_seconds()
    return str(timedelta(seconds=total_seconds))

# insert video details
def insert_video_details(video_details):
    try:
        mydb = sql.connect(
                host="127.0.0.1",
                user="root",
                password="root",
                database="youtube"
                )
        cur = mydb.cursor(buffered=True)
        create_video_table = '''CREATE TABLE IF NOT EXISTS videos (
                                channel_id VARCHAR(100),
                                channel_name VARCHAR(100),
                                Video_id VARCHAR(30),
                                Video_Name VARCHAR(150),
                                Video_Discription TEXT,
                                Tags TEXT,
                                Published_At TIMESTAMP,
                                View_Count BIGINT,
                                Like_Count BIGINT,
                                Dislike_Count BIGINT,
                                Favourite_Count INT,
                                Comment_Count INT,
                                Duration VARCHAR(50),
                                Thumbnail VARCHAR(200),
                                Caption_Status VARCHAR(50),
                                PRIMARY KEY (Video_id)
                                )'''
        cur.execute(create_video_table)
        mydb.commit()
        print('videos table created successfully')
    except sql.Error as err:
        print(f'Error: {err}')

    for index, row in video_details.iterrows():
        video_query = '''INSERT INTO videos (channel_id,
                                            channel_name,
                                            Video_id,
                                            Video_Name,
                                            Video_Discription,
                                            Tags,
                                            Published_At,
                                            View_Count,
                                            Like_Count,
                                            Dislike_Count,
                                            Favourite_Count,
                                            Comment_Count,
                                            Duration,
                                            Thumbnail,
                                            Caption_Status)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        # Convert list to comma-separated string if necessary
        tags = ', '.join(row['Tags']) if isinstance(row['Tags'], list) else row['Tags']

        # Convert Published_At to MySQL compatible datetime format
        published_at = datetime.strptime(row['Published_At'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        # Parse duration
        duration = parse_duration(row['Duration'])

        values = (row['channel_id'],
                row['channel_name'],
                row['Video_id'],
                row['Video_Name'],
                row['Video_Discription'],
                tags,
                published_at,
                row['View_Count'],
                row['Like_Count'],
                row['Dislike_Count'],
                row['Favourite_Count'],
                row['Comment_Count'],
                duration,
                row['Thumbnail'],
                row['Caption_Status']
                )

        try:
            cur.execute(video_query, values)
            mydb.commit()
        except sql.Error as err:
            print(f'Error: {err}')

# insert comment details
def insert_comment_details(comment_details):
    try:
        mydb = sql.connect(
                host="127.0.0.1",
                user="root",
                password="root",
                database="youtube"
            )
        cur = mydb.cursor(buffered=True)
        create_comment_table = '''CREATE TABLE IF NOT EXISTS comments (
                                  com_id VARCHAR(100) PRIMARY KEY,
                                  Video_id VARCHAR(50),
                                  com_text TEXT,
                                  com_author VARCHAR(150),
                                  com_published TIMESTAMP
                                  )'''
        cur.execute(create_comment_table)
        mydb.commit()
        print('comments table created successfully')
    except sql.Error as err:
        print(f'Error: {err}')

    for index, row in comment_details.iterrows():
        comment_query = '''INSERT INTO comments (
                           com_id,
                           Video_id,
                           com_text,
                           com_author,
                           com_published)
                           VALUES (%s, %s, %s, %s, %s)'''

        values = (
            row['com_id'],
            row['Video_id'],
            row['com_text'],
            row['com_author'],
            row['com_published']
        )

        try:
            cur.execute(comment_query, values)
            mydb.commit()
        except sql.Error as err:
            print(f'Error: {err}')

# Get all channel related details
def fetch_channel_details(st_channel_id):
  st_df_channel_details=[]
  st_df_video_details=[]
  st_df_comment_details=[]

  st_channel_details=get_channel_info(st_channel_id)
  st_df_channel_details = pd.DataFrame(st_channel_details)
  
  # Get playlist IDs
  st_playlist_ids = get_playlist_ids(st_channel_id)

  # Get video IDs
  st_video_ids = []
  for i in st_playlist_ids:
      st_video_details = get_video_ids(i)
      st_video_ids.extend(st_video_details)

  # Get video details
  st_video_details = get_video_info(st_video_ids)
  st_df_video_details = pd.DataFrame(st_video_details)

  # Get comment details
  st_comment_details=get_comment_info(st_video_ids)
  st_df_comment_details=pd.DataFrame(st_comment_details)

  return [st_df_channel_details, st_df_video_details, st_df_comment_details]

# Insert all data into SQL Tables
def insert_all_details(fetched_data):
  st_df_channel_details = fetched_data[0]
  st_df_video_details = fetched_data[1]
  st_df_comment_details = fetched_data[2]
  
  insert_channel_details(st_df_channel_details)
  insert_video_details(st_df_video_details)
  insert_comment_details(st_df_comment_details)
  st.success("Channel Details of the given channel is inserted successfully")

# streamlit part
def homePage():
    st.header('EXTRACT TRANSFORM')
    channel_id=st.text_input("Enter YouTube Channel_ID below :")
    st.caption('Hint: Go to channel\'s home page >> Right click >> View page source >> Find channel_id')

    # Initialize fetched_data in session state if not already
    if "fetched_data" not in st.session_state:
        st.session_state.fetched_data = []
    if "ch_ids" not in st.session_state:
        st.session_state.ch_ids = []
    
    if st.button("Extract Data"):
        if channel_id:
            st.session_state.fetched_data = fetch_channel_details(channel_id)
            st.success("Data fetched successfully!")
        else:
            st.warning("No channel Id provided")
        
    if st.button("Upload to SQL"):
        mydb = sql.connect(
                host="127.0.0.1",
                user="root",
                password="root",
                database="youtube"
            )
        cur = mydb.cursor(buffered=True)
        
        # Fetching channel IDs from DB
        query0 = '''SELECT Channel_Id FROM channels'''
        cur.execute(query0)
        ch_id_table = cur.fetchall()
        st.session_state.ch_ids = [t[0] for t in ch_id_table]
        
        if channel_id in st.session_state.ch_ids:
            st.success("Channel Details of the given channel already exists")
        else:
            insert_all_details(st.session_state.fetched_data)
            st.success("Channel Details of the given channel is inserted successfully")
        cur.close()
        mydb.close()

def viewPage():
    st.header('Select any question to get Insights')
    questions = [
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
    ]
    selected_question = st.selectbox("Questions", questions)

    #SQL Connection
    mydb = sql.connect(
            host="127.0.0.1",
            user="root",
            password="root",
            database="youtube",
            port="3306"
        )
    cur = mydb.cursor()

    if selected_question == "1. What are the names of all the videos and their corresponding channels?":
        query1 = '''SELECT Video_Name AS videos, channel_name AS channelname FROM videos'''
        cur.execute(query1)
        t1 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df = pd.DataFrame(t1, columns=["videos title", "channel name"])
        cur.close()
        mydb.close()
        st.write(df)

    elif selected_question == "2. Which channels have the most number of videos, and how many videos do they have?":
        query2 = '''SELECT channel_name, COUNT(*) AS video_count FROM videos GROUP BY channel_name 
                    ORDER BY video_count DESC'''
        cur.execute(query2)
        t2 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df2 = pd.DataFrame(t2, columns=["channel name","video_count"])
        st.write(df2)

    elif selected_question == "3. What are the top 10 most viewed videos and their respective channels?":
        query3 = '''SELECT View_Count AS views, channel_name AS channelname, Video_Name AS videoname FROM videos
                    WHERE View_Count IS NOT NULL ORDER BY View_Count DESC LIMIT 10'''
        cur.execute(query3)
        t3 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df3 = pd.DataFrame(t3, columns=column_names)
        st.write(df3)

    elif selected_question == "4. How many comments were made on each video, and what are their corresponding video names?":
        query4 = '''SELECT Video_Name, COUNT(Comment_Count) AS comment_count FROM videos 
                    GROUP BY Video_Name ORDER BY comment_count DESC'''
        cur.execute(query4)
        t4 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df4 = pd.DataFrame(t4, columns=column_names)
        st.write(df4)

    elif selected_question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5 = '''SELECT Video_Name, channel_name, Like_Count FROM videos ORDER BY Like_Count DESC'''
        cur.execute(query5)
        t5 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df5 = pd.DataFrame(t5, columns=column_names)
        st.write(df5)

    elif selected_question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6 = '''SELECT Like_Count AS likecount, Dislike_Count AS dislikecount, 
                    Video_Name AS videoname FROM videos'''
        cur.execute(query6)
        t6 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df6 = pd.DataFrame(t6, columns=column_names)
        st.write(df6)

    elif selected_question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        query7 = '''SELECT channel_name AS channelname, Channel_Views AS totalviews FROM channels'''
        cur.execute(query7)
        t7 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df7 = pd.DataFrame(t7, columns=column_names)
        st.write(df7)

    elif selected_question == "8. What are the names of all the channels that have published videos in the year 2022?":
        query8 = '''SELECT DISTINCT Video_Name AS videoname, channel_name AS channelname, 
                    Published_At AS publisheddate FROM videos WHERE YEAR(Published_At) = 2022'''
        cur.execute(query8)
        t8 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df8 = pd.DataFrame(t8, columns=column_names)
        st.write(df8)

    elif selected_question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9 = '''SELECT channel_name as channelname, AVG(TIME_TO_SEC(Duration)) 
                    AS avg_duration FROM videos GROUP BY channel_name'''
        cur.execute(query9)

        t9 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df9 = pd.DataFrame(t9, columns=column_names)

        T9 = []
        for index, row in df9.iterrows():
            channel_name = row['channelname']
            avg_seconds = row['avg_duration']
            
            # Convert decimal.Decimal to float
            avg_seconds_float = float(avg_seconds)
            
            # Convert seconds to hh:mm:ss format
            avg_duration_td = timedelta(seconds=avg_seconds_float)
            avg_duration_str = str(avg_duration_td)

            T9.append(dict(channelname=channel_name, avgduration=avg_duration_str))
        df1 = pd.DataFrame(T9)
        st.write(df1)

    elif selected_question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10 = '''SELECT Video_Name AS videoname,channel_name AS channelname,Comment_Count 
                    AS commentcount FROM videos WHERE Comment_Count IS NOT NULL ORDER BY comment_count DESC'''
        cur.execute(query10)
        t10 = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        df10 = pd.DataFrame(t10, columns=column_names)
        st.write(df10)

# Set up session state if not already set
if 'page' not in st.session_state:
    st.session_state.page = 'Page 1'

# Sidebar buttons for navigation
st.sidebar.subheader("Home")
if st.sidebar.button("Extract and Transform"):
    st.session_state.page = 'Page 1'
if st.sidebar.button("View"):
    st.session_state.page = 'Page 2'

# Display the current page based on session state
if st.session_state.page == 'Page 1':
    homePage()
elif st.session_state.page == 'Page 2':
    viewPage()
