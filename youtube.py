
#importing necessary libraries


from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import psycopg2
from streamlit_option_menu import option_menu
import json
import requests
from streamlit_lottie import st_lottie
from streamlit_lottie import st_lottie_spinner


#Establishing API connection

def YoutubeApi_key_connection():
    
    api_service_name = "youtube"
    api_version = "v3"

    api_key="your api key"

    youtube = build(api_service_name,api_version,developerKey=api_key)
    return youtube

youtube=YoutubeApi_key_connection()


#Scraping of Data
#Getting channel informations

def Get_channel_details(channel_id):
        request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
                )
        response= request.execute()

        channel_name=response['items'][0]['snippet']['title']
        channel_id=response['items'][0]['id']
        channel_description=response['items'][0]['snippet']['description']
        channel_subscription_count=response['items'][0][ 'statistics']['subscriberCount']
        channel_video_count=response['items'][0][ 'statistics']['videoCount']
        channel_views=response['items'][0]['statistics']['viewCount']
        playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        channel_info={
                    "channel_name":channel_name,
                    "channel_id":channel_id,
                    "channel_description":channel_description,
                    "channel_subscription_count":channel_subscription_count,
                    "channel_video_count":channel_video_count,
                    "channel_views":channel_views,
                    "playlist_id":playlist_id   

                }

                    
                
        return channel_info
    
        
     
#Getting video ids

def Get_video_ids(channel_id):
     video_ids=[]
     res=youtube.channels().list(
                        part="snippet,contentDetails,statistics",
                        id=channel_id
                    ).execute()

     playlist_id=res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

     next_page_token=None
     while True:
        res1=youtube.playlistItems().list(
                part = "snippet,contentDetails,statistics",
                playlistId = playlist_id, 
                maxResults = 500,
                pageToken=next_page_token).execute()
        for i in range(len(res1['items'])):
                video_ids.append(res1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=res1.get('nextPageToken')

        if next_page_token is None:
                break
     return video_ids


#Duration conversion


def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

#Getting  videos information

def Get_videos_information(video_ids):
    video_data=[]
    try:
        for video_id in video_ids:
            request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
             )
            response=request.execute()
            for i in  response['items']:
                data=dict(Channel_Name=i['snippet']['channelTitle'],
                  channelId=i['snippet']['channelId'],
                  videoid=i['id'],
                  video_title=i['snippet']['title'],
                  tags=i['snippet']['tags'],
                  thumbnail=i['snippet']['thumbnails']['default']['url'],
                  description=i['snippet']['description'],
                  published_at=i['snippet']['publishedAt'],
                  duration=time_duration(i['contentDetails']['duration']),
                  view_count=i['statistics']['viewCount'],
                  like_count=i['statistics']['likeCount'],
                  comment_count=i['statistics']['commentCount'],
                  favourite_count=i['statistics']['favoriteCount'],
                  caption_status=i['contentDetails']['caption']
                )
            video_data.append(data)
    except:
      pass 
  
    return video_data

#Getting comment Details

def Get_comment_data(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
           request=youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=50
           )
           response=request.execute()
           for comments in response['items']:
                comment_info=dict(comment_id=comments ['snippet']['topLevelComment']['id'],
                videoid=comments ['snippet']['videoId'],
                comment_text=comments ['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_author=comments['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_published_at=comments['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(comment_info)
    except:
       pass
    return comment_data



#Storing data in mongo db

import pymongo

my_client=pymongo.MongoClient('mongodb://localhost:27017/')
db=my_client["Youtube_Data"]




def channel_details(channel_id):
    import pymongo

    my_client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=my_client["Youtube_Data"]
   
    channel_det=Get_channel_details(channel_id)
    vid_id=Get_video_ids(channel_id)
    vid_info=Get_videos_information(vid_id)
    com_det=Get_comment_data(vid_id)

    collection=db["channel_details"]

    collection.insert_one({"channel_information":channel_det,"video_information":vid_info,"comment_information": com_det})
    return"uploaded successfully"





#creation  of channels,videos,comments tables in Postgresql  and importing data in those tables.

#Channels table

def channel_table():
    mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password='your password',
                            port="5432",
                            database="Youtube")
    cursor=mydb.cursor()
    drop_query = "drop table if exists channel_data"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''CREATE TABLE IF NOT EXISTS channel_data(channel_name VARCHAR(100),
                                                                channel_id VARCHAR(500) PRIMARY KEY,
                                                                channel_description TEXT,
                                                                channel_subscription_count INT,
                                                                channel_video_count INT,
                                                                channel_views INT,
                                                                playlist_id VARCHAR(300))'''

        cursor.execute(create_query)
        mydb.commit()
    except:
        st("Channel table already created")

    ch_list=[]

    my_client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=my_client["Youtube_Data"]
    collection=db["channel_details"]

    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)
    for index,row in df.iterrows():
        insert_query='''INSERT into channel_data( channel_name, 
                                                        channel_id, 
                                                        channel_description, 
                                                        channel_subscription_count, 
                                                        channel_video_count, 
                                                        channel_views, 
                                                        playlist_id ) 
                                                        
                                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
                
        values=(row['channel_name'],
                    row['channel_id'],
                    row['channel_description'],
                    row['channel_subscription_count'],
                    row['channel_video_count'],
                    row['channel_views'],
                    row['playlist_id'])
        try:
                cursor.execute(insert_query,values)
                mydb.commit()    
        except:
                st.write("Channel values are already inserted")

#Videos Table

def videos_table():
    mydb = psycopg2.connect(host='localhost',
        user='postgres',
        password='your password',
        port="5432",
        database="Youtube")
    cursor=mydb.cursor()
    drop_query = "drop table if exists videos_details"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''CREATE TABLE IF NOT EXISTS videos_details(Channel_Name VARCHAR(100),
                                        channelId VARCHAR(500) ,
                                        videoid VARCHAR(255) PRIMARY KEY,
                                        video_title VARCHAR(500),
                                        tags TEXT,
                                        thumbnail VARCHAR(255),
                                        published_at TIMESTAMP,
                                        duration INTERVAL,
                                        description TEXT,
                                        view_count BIGINT,
                                        like_count BIGINT,
                                        comment_count INT,
                                        favourite_count INT,
                                        caption_status VARCHAR(50)) '''
        cursor.execute(create_query)
        mydb.commit()


    except:
        st.write("videos_details  alrady created")

    vi_list=[]
    my_client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=my_client["Youtube_Data"]
    collection=db["channel_details"]

    for vi_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    DF2=pd.DataFrame(vi_list)

    for index, row in DF2.iterrows():
                    insert_query='''
                                INSERT INTO videos_details(Channel_Name ,channelId,videoid,video_title,tags,thumbnail,published_at,duration,description,view_count,like_count,comment_count,favourite_count,caption_status)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    values=(
                                row['Channel_Name'],
                                row['channelId'],
                                row['videoid'],
                                row['video_title'],
                                row['tags'],
                                row['thumbnail'],
                                row['published_at'],
                                row['duration'],
                                row['description'],
                                row['view_count'],
                                row['like_count'],
                                row['comment_count'],
                                row['favourite_count'],
                                row['caption_status'])
        
                    try:
                            cursor.execute(insert_query,values)
                            mydb.commit() 
                    except:
                            st.write("videos_details already inserted in the table")   



#Comments Table

def comment_table():
    mydb = psycopg2.connect(host='localhost',
                        user='postgres',
                        password='your password',
                        port="5432",
                        database="Youtube")
    cursor=mydb.cursor()
    drop_query = "drop table if exists comments_data"
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_query='''CREATE TABLE if not exists comments_data(comment_id VARCHAR(100) PRIMARY KEY,
                                                        videoid  VARCHAR(80),   
                                                        comment_text TEXT,
                                                        comment_author VARCHAR(150),
                                                        comment_published_at TIMESTAMP)'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("comments_data Table already created")


    com_list=[]
    my_client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=my_client["Youtube_Data"]
    collection=db["channel_details"]

    for com_data in collection.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    DF6=pd.DataFrame(com_list)

    for index, row in DF6.iterrows():
                        insert_query='''INSERT INTO comments_data(comment_id,videoid,comment_text,comment_author,comment_published_at) VALUES (%s, %s, %s, %s, %s)'''
                        values=(row['comment_id'],
                                row['videoid'],
                                row['comment_text'],
                                row['comment_author'],
                                row['comment_published_at'])
                        try:
                            cursor.execute(insert_query,values)
                            mydb.commit()
                        except:
                            st.write("this commnent already exist in comments table")

#Creation of tables in sql

def tables():
      channel_table()
      videos_table()
      comment_table()
      return "Data Transfered to SQL succesfully"
      

#Visualisation of Tables
#channel table

def show_channels_table():
    ch_list=[]
    my_client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=my_client["Youtube_Data"]
    collection=db["channel_details"]

    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table=st.dataframe(ch_list)
    return channels_table

#videos_table

def show_videos_table():
    vi_list=[]
    my_client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=my_client["Youtube_Data"]
    collection=db["channel_details"]

    for vi_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    videos_table=st.dataframe(vi_list)
    return videos_table

#Comment_table

def show_coments_table():
    com_list=[]
    my_client=pymongo.MongoClient('mongodb://localhost:27017/')
    db=my_client["Youtube_Data"]
    collection=db["channel_details"]

    for com_data in collection.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table=st.dataframe(com_list)
    return comments_table

#Streamlit setup

#Animation Setup

st.set_page_config(page_title="Youtube")
def load_lottiefile(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)


def load_lottieurl(url: str):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()


st.header(":red[**YOUTUBE DATA HARVESTING AND WAREHOUSING**]")


selected=option_menu(
        menu_title="Main Menu",
        options=["Home","Data Scraping and Storing","Migration and Storing","View Tables","Additional Information"],
        icons=["house-fill","box-fill","server","table","book-half"],
        menu_icon="youtube",
        orientation="horizontal")



#setup of Home page        

if selected=="Home":
    
     
     filepath=load_lottiefile("E:\data science\project 1\ds.json")
     st.lottie(filepath,speed=1,reverse=False,loop=True,height=350,width=1150,quality="highest")
     selected1=option_menu(
        menu_title="Technologies Used",
        options=["Youtube Data API","Python","MongoDB","SQL","Streamlit"],
        icons=["youtube","code","card-text","cloud-arrow-up-fill","play-fill"],
        menu_icon="distribute-vertical",
        )
     if selected1=="Python":
          st.markdown("Python is an interpreted, high-level, general-purpose programming language. Its design philosophy emphasizes code readability with its notable use of significant whitespace. Its language constructs and object-oriented approach aim to help programmers write clear, logical code for small and large-scale projects Python is dynamically typed and garbage-collected. It supports multiple programming paradigms, including structured particularly, procedural and functional programming, object-oriented, and concurrent programming.Python is widely used for web development, software development, data science, machine learning and artificial intelligence, and more. It is free and open-source software.")
     if selected1=="Youtube Data API":
          st.markdown("The YouTube Data API is an application programming interface  that allows you to add YouTube features to your website or application. You can use the API to: Upload videos Manage playlists and subscriptions Update channel settings Retrieve feeds related to videos, users, and playlists Manipulate feeds, such as creating new playlists, adding videos as favorites, and sending messages.")
     if selected1=="MongoDB":
          st.markdown("MongoDB is a NoSQL database program that's used to manage large amounts of data. It's a document-oriented database that uses collections of documents instead of tables of rows to store data. MongoDB is popular with developers because it's easy to use for storing structured or unstructured data.")    
     if selected1=="SQL":
          st.markdown("Structured query language (SQL) is a programming language for storing and processing information in a relational database. A relational database stores information in tabular form, with rows and columns representing different data attributes and the various relationships between the data values.")  
     if selected1=="Streamlit":
          st.markdown("Streamlit is an open-source app framework in python language. It helps us create beautiful web apps for data science and machine learning in a little time. It is compatible with major python libraries such as scikit-learn, keras, PyTorch, latex, numpy, pandas, matplotlib, etc.")
     
     st.subheader("**Instructions**")
     st.markdown("1. Get the :blue[**youtube channel ID**] from the channel for which you want to harvest the data.")
     st.markdown("2. Insert the channel ID in :blue[**Data Scraping**] zone .Here the data is getting scrapped from the youtube channel and it is getting stored in MongoDB  datalake.")
     st.markdown("3. The next step to convert the data into a structured data. To do that we have to go to :blue[**Migration and storing**]  zone. Now the data is converted in structured format and stored in SQL servers.")
     st.markdown("In this way the data is scrapped ,stored and displayed in streamlit application.")

#Setup of Data Scraping And storing page
         
if selected=="Data Scraping and Storing":
     
     st.header(":blue[Data Scraping]")
    
     channel_id=st.text_input(":white[Enter The Channel ID]")
     if st.button("Collect And Store Data In MongoDB"):
                with st.spinner("Scraping"):
                 ch_ids=[]
                 my_client=pymongo.MongoClient('mongodb://localhost:27017/')
                 db=my_client["Youtube_Data"]
                 collection=db["channel_details"]

                 for ch_data in collection.find({},{"_id":0,"channel_information":1}):
                     ch_ids.append(ch_data["channel_information"]["channel_id"])

                 if channel_id in ch_ids:
                     st.success("Channel Details of the given channel already exists")
                 else:
                     insert=channel_details(channel_id)
                     st.success(insert)
     file=load_lottiefile("E:\data science\project 1\A1.json")
     st.lottie(file,speed=0.40,reverse=False,loop=False,height=100,width=100,quality="highest") 

#Setup of  Migration and Storing page
           
if selected=="Migration and Storing":
     st.header(":blue[Transfer And Storing In SQL ]")
     
     
     if st.button("Transfer Data to SQL"):
        with st.spinner("uploading to sql"):
         Table=tables()
         st.success(Table)
     file=load_lottiefile("E:\data science\project 1\A1.json")
     st.lottie(file,speed=0.40,reverse=False,loop=False,height=100,width=100,quality="highest") 

#setup of data visualization table

if selected=="View Tables":
     selected=option_menu(
          menu_title="Tables",
          options=["Channels","Videos","Comments"],
          icons=["table","table","table"],
          orientation="horizontal"
     )

     if selected=="Channels":
       show_channels_table()

     if selected=="Videos":
       show_videos_table()

     if selected=="Comments":
       show_coments_table()
#Setup of Additional Information Page
#10 sql queries 
 
if selected=="Additional Information":
     

    mydb = psycopg2.connect(host='localhost',
                            user='postgres',
                            password='12345',
                            port="5432",
                            database="Youtube")
    cursor=mydb.cursor()
    questions=st.selectbox("Select Your Question",("1. Names of all the videos and Their corresponding channels",
                                                   "2. Channels who have the most number of videos, and number of videos",
                                                   "3. Top 10 Most Viewed videos and their  respective channels",
                                                   "4. Comments on Each video and their respective video",
                                                   "5. Videos have the highest number of likes and their corresponding channel names",
                                                   "6. Total number of likes  for each video  and their corresponding video names",
                                                   "7. Total number of views for each channel and their corresponding channel names",
                                                   "8. Names of the channels that published vidoes in year 2022",
                                                   "9. Average duration  of all videos  in each channel and their corresponding channel names",
                                                  "10. Vidoes with Highest number of comments  and their corresponding channel names"))
    if questions=="1. Names of all the videos and Their corresponding channels":
         query1='''select video_title as videotitle,channel_name as channelname from videos_details;'''
         cursor.execute(query1)
         mydb.commit()
         table1=cursor.fetchall()
         st.write(pd.DataFrame(table1,columns=["videotitle","channel name"]))

    elif questions=="2. Channels who have the most number of videos, and number of videos":
         query2='''select channel_name as channelname,channel_video_count as numberofvideos from channel_data order by channel_video_count desc;'''
         cursor.execute(query2)
         mydb.commit()
         table2=cursor.fetchall()
         st.write(pd.DataFrame(table2,columns=["channel name","number of videos"]))
    
    elif questions=="3. Top 10 Most Viewed videos and their  respective channels":
         query3='''select channel_name as channelname,video_title as videoname,view_count as views from videos_details where view_count is not null order by view_count desc limit 10;'''
         cursor.execute(query3)
         mydb.commit()
         table3=cursor.fetchall()
         st.write(pd.DataFrame(table3,columns=["channel name","video name","views"]))
    
    elif questions=="4. Comments on Each video and their respective video":
         query4='''select video_title as videoname,comment_count as numberofcomments from videos_details where comment_count is not null order by comment_count desc;'''
         cursor.execute(query4)
         mydb.commit()
         table4=cursor.fetchall()
         st.write(pd.DataFrame(table4,columns=["video name","number of comments"]))

    elif questions=="5. Videos have the highest number of likes and their corresponding channel names":
         query5=''' select channel_name as channelname,video_title as videoname,like_count as likes from videos_details where like_count is not null order by like_count desc limit 20;'''
         cursor.execute(query5)
         mydb.commit()
         table5=cursor.fetchall()
         st.write(pd.DataFrame(table5,columns=["channel name","video name","likes"]))

    elif questions=="6. Total number of likes  for each video  and their corresponding video names":
         query6='''select video_title as videoname,like_count as likes from  videos_details ;'''
         cursor.execute(query6)
         mydb.commit()
         table6=cursor.fetchall()
         st.write(pd.DataFrame(table6,columns=["video name","likes"]))

    elif questions=="7. Total number of views for each channel and their corresponding channel names":
         query7='''select channel_name as channelname,channel_views as viewcount from channel_data;'''
         cursor.execute(query7)
         mydb.commit()
         table7=cursor.fetchall()
         st.write(pd.DataFrame(table7,columns=["channel name","channel viewcount"]))
        
    elif questions=="8. Names of the channels that published vidoes in year 2022":
         query8='''select channel_name as channelname,video_title as videoname,published_at as publishedyear from videos_details where extract(year from published_at)=2022;'''
         cursor.execute(query8)
         mydb.commit()
         table8=cursor.fetchall()
         st.write(pd.DataFrame(table8,columns=["channel name","video name","published at"]))
    
    elif questions=="9. Average duration  of all videos  in each channel and their corresponding channel names":
         query9='''select channel_name as channelname, AVG(duration) as averageduration FROM videos_details GROUP BY channel_name;'''
         cursor.execute(query9)
         mydb.commit()
         table9=cursor.fetchall()
         df9=pd.DataFrame(table9,columns=["channelname","averageduration"])
         T9=[]
         for index,row in df9.iterrows():
            Channel_Name=row["channelname"]
            average_duration=row["averageduration"]
            average_duration_str=str(average_duration)
            T9.append(dict(channelname=Channel_Name,averageduration=average_duration_str))
         st.write(pd.DataFrame(T9))

    elif questions=="10. Vidoes with Highest number of comments  and their corresponding channel names":
         query10='''select channel_name as channelname ,video_title as videoname,comment_count as numberofcomments from videos_details where comment_count is not null order by comment_count  desc ;'''
         cursor.execute(query10)
         mydb.commit()
         table10=cursor.fetchall()
         st.write(pd.DataFrame(table10,columns=["channel name","video name","number of comments"]))
         
    
    

    
 
