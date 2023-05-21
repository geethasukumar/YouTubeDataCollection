######### YouTube Data Collection Youtube Capstone Project
######### Developed by Geetha Sukumar
######### Date: 21/05/2023

import googleapiclient.discovery
import YTD_configs as config
import json
import re
import pymongo
import pandas as pd
import mysql.connector
import streamlit as st

from datetime import datetime

class youtube_data:
    def __init__(self, chnl_id):
        self.chnl_id = chnl_id
        self.DEVELOPER_KEY = config.YT_API_KEY
        self.MONGODB_CLIENT = config.MONGODB_CLIENT
        self.MYSQLDB_config = config.MYSQLDB_config
        self.Questions = config.Questions
        self.Queries = config.Queries
        self.chnl_info = {}
        
    
    
    # get basic channel information
    def get_channel_info(self):
        if self.chnl_id != '' :
            try:
                self.api_service_name = "youtube"
                self.api_version = "v3"


                self.youtube = googleapiclient.discovery.build(
                    self.api_service_name, self.api_version, developerKey = self.DEVELOPER_KEY)

                chnl_request = self.youtube.channels().list(
                    part='snippet,contentDetails,statistics',
                    id=self.chnl_id
                )
                chnl_response = chnl_request.execute()
                self.chnl_info = {"Channel_Name": chnl_response['items'][0]['snippet']['title'],
                                  "Channel_Id" : self.chnl_id,
                                  "Subscription_Count" : chnl_response['items'][0]['statistics']['subscriberCount'],
                                  "Channel_Views" : chnl_response['items'][0]['statistics']['viewCount'],
                                  "Channel_Description" : chnl_response['items'][0]['snippet']['description'][0:50]
                                 }
            except:
                st.write('Error connecting to Youtube Channel using API Key')
            
        else:
            st.write('Please enter a YouTube Channel Id to fetch Channel info.')
        
        
    # get playlists information for the channel id                                  
    def get_playlists(self):
 
        pl_request = self.youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=self.chnl_id
        )

        pl_response = pl_request.execute()
        pl_items = {}
        for idx,item in enumerate(pl_response['items']):
            pl_id = item['id']
            pl_title = item['snippet']['title']
            pl_items.update({"Playlist_Id_"+str(idx+1): {'playlist_id': pl_id, 'playlist_name': pl_title}})
            
            #invoke playlist videos function to get playlist item videos for a playlist ID
            pl_videos = self.get_pl_videos(pl_id)
            
            pl_items["Playlist_Id_"+str(idx+1)]['videos'] = pl_videos
        
        
        return pl_items    
    
    
    
    
    
    # get video ids for the playlists
    def get_pl_videos(self,pl_id):
        
        pl_vid_request = self.youtube.playlistItems().list(
            part = "snippet,contentDetails,id,status",
            playlistId = pl_id,
            maxResults=100
        )
        
        pl_vid_response = pl_vid_request.execute()
        pl_videos={}
        for idx,item in enumerate(pl_vid_response['items']):
            
            #invoke video function to get video information for the video id
            video_info = self.get_video_info(item['contentDetails']["videoId"])
            
            # invoke comments function to get the comments for the video id
            cmt_info = {'Comments' : self.get_comments(item['contentDetails']["videoId"])}
            
            video_info.update(cmt_info)
            pl_videos["Video_Id_" + str(idx+1)] = video_info 
            
            
            
        return pl_videos
   



    # get video ids for the playlists
    def get_video_info(self,video_id):
        
        video_request = self.youtube.videos().list(
            part = "snippet,statistics,status,contentDetails",
            id = video_id          
        )
        video_response = video_request.execute()
        video_info={}
       
        if video_response['pageInfo']["totalResults"] > 0 : 
            
           
            video_info = {'Video_Id': video_id,
                          "Video_Name": video_response['items'][0]['snippet']['title'],
                          "Video_Description": video_response['items'][0]['snippet']['description'],
                          #"Tags": video_response['items'][0].get(['tags']),
                          "PublishedAt": video_response['items'][0]['snippet']['publishedAt'],
                          "View_Count": video_response['items'][0]['statistics'].get('viewCount',0),
                          "Like_Count": video_response['items'][0]['statistics'].get('likeCount',0),
                          "Dislike_Count": video_response['items'][0]['statistics'].get('dislikeCount',0),
                          "Favourite_Count": video_response['items'][0]['statistics'].get('favoriteCount',0),
                          "Comment_Count": video_response['items'][0]['statistics'].get('commentCount',0),
                          "Duration": self.process_duration(video_response['items'][0]['contentDetails']['duration']),
                          "Thumbnail" : video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                          "Caption_Status": video_response['items'][0]['status']['uploadStatus']
                         }
            
        return video_info

    #get comments details for the video
    def get_comments(self,video_id):
        try:
            cmt_request = self.youtube.commentThreads().list(
                                part="snippet",
                                videoId=video_id,
                                maxResults= 20   #get 20 comments
                                )

            cmt_response = cmt_request.execute()
            cmt_info={}


            for idx,item in enumerate(cmt_response['items']):

                cmt_info["Comment_Id_" + str(idx+1)] = {'Comment_Id': item["snippet"]["topLevelComment"]['id'],
                                                       'Comments_Text': item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                                       'Comments_Author': item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                                       'Comment_PublishedAt': item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                                                       }
            return cmt_info     
        except:
            cmt_info={}
            return(cmt_info)
                                               
                                                   
    
    
    # convert duration to HH:MM:SS format
    def process_duration(self,duration_inp_str):
        matches = re.search(r"^PT(([0-9]+)H)?(([0-9]+)M)?(([0-9]+)S)?$", duration_inp_str)
        duration=''
              
        if matches :
            hrs= matches.groups()[0]

            if hrs is not None:
                duration = duration + f'{int(matches.groups()[1]):02d}' + ':'
            else:
                duration = '00:'
            if matches.groups()[2] is not None:
                duration = duration + f'{int(matches.groups()[3]):02d}' + ':'
            else:
                duration = duration + '00:'
            if matches.groups()[4] is not None:
                duration = duration + f'{int(matches.groups()[5]):02d}'
            else:
                duration = duration + '00:'
        
        return duration
    
    
    ## DB connection definition
    def mongo_db_connect(self):
        collections=''
        try:
            client=pymongo.MongoClient(self.MONGODB_CLIENT)
            db = client.capstone_project
            self.collections=db.Youtube_ChannelData
            print("Successfully connected to DB - Youtube_ChannelData")
            
        except:
            st.write("Error: Function mongo_db_connect - Connecting to DB - Youtube_ChannelData failed!")



    ## connect to DB and save the youtube channel_data as documents in DB
    def mongo_db_save_yt_data(self,channel_data):
        # connect to DB
        self.mongo_db_connect()

        try:
            
            self.collections.insert_one(channel_data)
            st.write("Youtube Channel Data successfully stored in MongoDB") 
        except:
            st.write("Error: Function mongo_db_save_yt_data - Saving Youtube Channel Data to MongoDB failed!!")
     
    
    # get the channel names stored in the mongoDB        
    def get_channels(self):
         # connect to DB
        try:
            self.mongo_db_connect()
            self.chnl_title=[]
            
            # find all channel names
            for chnl in self.collections.find({},{'_id': 0, 'Channel.Channel_Name': 1}):
                
                json_chnl=chnl['Channel']
                self.chnl_title.append(json_chnl['Channel_Name'])
                
                # remove the duplicate channels from the list
                self.chnl_title = list(dict.fromkeys(self.chnl_title))
                
         
        except:
            st.write('Error: Function get_channels - Error retrieving Channel Info from MongoDB !!')
   


    # MySQL DB connect
    def mysql_db_connect(self):
        try:
          
            #self.db_mysql_cnx= mysql.connector.connect(**self.MYSQLDB_config)
            self.db_mysql_cnx= mysql.connector.connect(
                                host="localhost",
                                user="root",
                                
                                database="youtube_channeldata"
                            )
            mysql_cursor = self.db_mysql_cnx.cursor()
            return mysql_cursor
        except:
            st.write('Error mysql_db_connect - MySql DB connection failed!!')
        
    

    #Run the dashboard questions query based on the question no
    
    def dash_query_result(self, q_no):
       
        ## connect to MySQL DB
        
        try:        
            mysql_cursor = self.mysql_db_connect()
            
        except Exception as ex:   
             st.write(ex)
             
        question_q = self.Queries[q_no]
        mysql_cursor.execute(question_q)
        
        # Fetch all the rows returned by the query
        rows = mysql_cursor.fetchall()

        # Get the column names from the cursor description
        columns = [desc[0].upper().replace('_',' ') for desc in mysql_cursor.description]

        # Create a Pandas DataFrame from the query result
        df = pd.DataFrame(rows, columns=columns)

  
        # Print or process the DataFrame as needed
        #st.dataframe(df.assign(hack='').set_index('hack'))
        #st.markdown(df.style.hide(axis="index").to_html(), unsafe_allow_html=True)
        #st.table(df)
        st.dataframe(df)
        
        
        mysql_cursor.close()
        self.db_mysql_cnx.close()    
    
    # get data from MongoDB for a channel    
    def store_channel_data_mysql(self,chnl_name):
      
    ## connect to mongo DB
        try:
            self.mongo_db_connect()
        except:
            st.write('Error: Function get_DB_channel_data - Error connecting to MongoDB!!')
          
        
        ## connect to MySQL DB
        
        try:        
          
            mysql_cursor = self.mysql_db_connect()
            
        except Exception as ex:   
             st.write(ex)
        
                
        # get channel information and store in channel table    
        try:
            for data in self.collections.find({'Channel.Channel_Name':chnl_name}).sort("last_updated", pymongo.DESCENDING).limit(1):
                
                df=pd.DataFrame.from_dict(data['Channel'], orient="index")
                try:
                    ins_channel_q = "INSERT INTO channel (channel_id, channel_name, channel_views, channel_description) VALUES (%s, %s, %s, %s)" \
                                "ON DUPLICATE KEY UPDATE channel_id =VALUES(channel_id), channel_name =VALUES(channel_name), channel_views= VALUES(channel_views), channel_description = VALUES(channel_description)"
                    ins_channel_values = (str(df.loc["Channel_Id",0]), str(df.loc["Channel_Name",0])
                                      , str(df.loc['Channel_Views',0]), str(df.loc['Channel_Description',0]))
                
                
            
                    mysql_cursor.execute(ins_channel_q, ins_channel_values)
                except Exception as ex:
                    st.write("Error: Function get_DB_channel_data - Error storing Channel data to MySQL DB", ex);
               
            
            
                # get playlists information and store in playlists table    
           
                pl_dict = df.loc["Playlists"]
                
                for pl_k in pl_dict.items():
                    
                    for pl in pl_k[1].values():
                        try:  
                            ins_pl_q = "INSERT INTO playlist (playlist_id, channel_id, playlist_name) VALUES (%s, %s, %s)" \
                                    "ON DUPLICATE KEY UPDATE playlist_id = VALUES(playlist_id), channel_id =VALUES(channel_id), playlist_name= VALUES(playlist_name)"    
                            ins_pl_values = (pl['playlist_id'], str(df.loc["Channel_Id",0]), pl.get('playlist_name'))    
                        
                        
                            mysql_cursor.execute(ins_pl_q, ins_pl_values)
                        except Exception as ex:
                            st.write("Error: Function get_DB_channel_data - Error storing playlists data to MySQL DB", ex);
                      
                    
                        # get videos for the playlists and store in the mysql table videos
                        for vd_k in pl['videos'].values():
                            if vd_k.get('Video_Id') is not None:
                                
                                try:

                                    ins_vd_q = "INSERT INTO video (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favourite_count, comment_count,duration,thumbnail,caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                                        "ON DUPLICATE KEY UPDATE video_id = VALUES(video_id), playlist_id =VALUES(playlist_id), video_name= VALUES(video_name),video_description = values(video_description), published_date=values(published_date), view_count=values(view_count), like_count=values(like_count), dislike_count=values(dislike_count), favourite_count=values(favourite_count), comment_count = values(comment_count), duration=values(duration), thumbnail = values(thumbnail), caption_status=values(caption_status)"    

                                    ins_vd_values = (vd_k['Video_Id'], pl['playlist_id'], vd_k['Video_Name'], vd_k['Video_Description'], vd_k['PublishedAt'], vd_k['View_Count'], vd_k['Like_Count'], vd_k['Dislike_Count'], vd_k['Favourite_Count'], vd_k['Comment_Count'], vd_k['Duration'], vd_k['Thumbnail'], vd_k['Caption_Status'])   


                                    mysql_cursor.execute(ins_vd_q, ins_vd_values)
                                except Exception as ex:
                                    
                                    st.write("Error: Function get_DB_channel_data - Error storing videos to MySQL DB", ex);
                                
                                
                                # get videos for the playlists and store in the mysql table videos
                                for cmt_k in vd_k['Comments'].values():
                                   
                                    if cmt_k.get('Comment_Id') is not None:
                                    
                                        try:

                                            ins_cmt_q = "INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)" \
                                                "ON DUPLICATE KEY UPDATE comment_id = VALUES(comment_id), video_id =VALUES(video_id), comment_text= VALUES(comment_text),comment_author = values(comment_author), comment_published_date=values(comment_published_date)"    

                                            ins_cmt_values = (cmt_k['Comment_Id'], vd_k['Video_Id'], cmt_k.get('Comments_Text'), cmt_k.get('Comments_Author'), cmt_k.get('Comment_PublishedAt'))


                                            mysql_cursor.execute(ins_cmt_q, ins_cmt_values)
                                        except Exception as ex:

                                            st.write("Error: Function get_DB_channel_data - Error storing comments to MySQL DB", ex);
                                       
        except:
             st.write('Error: Function get_DB_channel_data - Error retrieving Channel data from MongoDB!!')
        
        self.db_mysql_cnx.commit()
        mysql_cursor.close()
        self.db_mysql_cnx.close()         
      
        st.write('Successfully migrated "' + chnl_name +'" channel data to MySQL DB !!');
            
                    
     

########### Main program Starts ###########            

def main():
    st.title ("YouTube Data Collection")
    st.markdown("By Geetha Sukumar")
    m = st.markdown("""
                    <style>
                    div.stButton > button:first-child {
                        background-color: #42c2f5;
                        color:#ffffff;
                    }


                    </style>""", unsafe_allow_html=True)
 
    #st.sidebar.title("YouTube Data Collection")

    youtube_id=st.sidebar.text_input("YouTube Channel ID")

    if "dc" not in st.session_state:
            st.session_state.dc = None
            st.session_state.json_channel_info = None

    
    if st.sidebar.button("Search YouTube"):
         with st.spinner():
            # Create object for class youtube_data for the channel id selected
            if youtube_id.strip() != '':
                dc = youtube_data(youtube_id)
                
                ####get channel information from youtube

                dc.get_channel_info()
           
                # get playlists info
                pl_items = dc.get_playlists()
                dc.chnl_info['Playlists'] = pl_items
                json_channel_info = {}
                json_channel_info['last_updated']=datetime.now()
                json_channel_info['Channel'] = dc.chnl_info

                st.session_state.dc = dc
                   
                st.write(pd.DataFrame.from_dict(json_channel_info))
                
                st.session_state.json_channel_info = json_channel_info
            else:
                st.write('Please enter a YouTube Channel Id to fetch Channel info.')
      
    if st.sidebar.button("Save in MongoDB"):
        if "dc" in st.session_state:
            dc = st.session_state.dc
            with st.spinner():        
                # Store the Channel data to Mongo DB
                dc.mongo_db_save_yt_data(st.session_state.json_channel_info)
        
               
        else:
            st.write("Search for a Youtube Channel Id to save")


    # Create object for class youtube_data for the channel id selected
    dc = youtube_data('')
    st.session_state.dc = dc

    # get the unique Channel names data saved in MongoDB
    dc.get_channels()
    option = st.sidebar.selectbox('Channels',dc.chnl_title)
    
              
    if st.sidebar.button("Migrate to MySqlDB"):
        if "dc" in st.session_state:
            dc = st.session_state.dc
            with st.spinner():        
              
                ## Retrieve Data from Mongo DB and migrate to mySQL
                dc.store_channel_data_mysql(option)
               
        else:
            st.write("Select a YouTube Channel to migrate !!")



    # Dashboard Questions
    # Displays the questions from ytdconfigs file
    db_quest = st.selectbox('Dashboard',options=dc.Questions.values())
             
    if st.button("Generate Report"):
        if "dc" in st.session_state:
            dc = st.session_state.dc
            
            with st.spinner():        
                q_no = next(idx for idx, value in dc.Questions.items() if value == db_quest)
                ## Retrieve Data from mySQL
                dc.dash_query_result(q_no)
               
        else:
            st.write("Select a YouTube Channel to migrate !!")



## Calling the main program
main()
