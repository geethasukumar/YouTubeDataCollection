YT_API_KEY="XXXXX" # update this API Key to use yours
MONGODB_CLIENT="mongodb+srv://xxx:xxxx@cluster0.eeit1pl.mongodb.net/?retryWrites=true&w=majority" # update the username and password for MongoDB atlas
MYSQLDB_config= {
  'user': 'root',
  'host': 'localhost',
  'database': 'youtube_channeldata',
  'raise_on_warnings': True
}
Questions = {
    1: 'What are the names of all the videos and their corresponding channels?',
    2: 'Which channels have the most number of videos, and how many videos do they have?',
    3: 'What are the top 10 most viewed videos and their respective channels?',
    4: 'How many comments were made on each video, and what are their corresponding video names?',
    5: 'Which videos have the highest number of likes, and what are their corresponding channel names?',
    6: 'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    7: 'What is the total number of views for each channel, and what are their corresponding channel names?',
    8: 'What are the names of all the channels that have published videos in the year 2022?',
    9: 'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    10: 'Which videos have the highest number of comments, and what are their corresponding channel names?'
}
Queries = {
1: 'SELECT ch.channel_name, vd.video_name from video vd inner join playlist pl on pl.playlist_id=vd.playlist_id inner join channel ch on ch.channel_id = pl.channel_id',
2: 'SELECT ch.channel_name, count(vd.video_id)  no_of_videos from video vd inner join playlist pl on pl.playlist_id=vd.playlist_id inner join channel ch on ch.channel_id = pl.channel_id group by ch.channel_name order by count(vd.video_id) desc',
3: 'SELECT ch.channel_name,vd.video_name,vd.view_count from video vd inner join playlist pl on pl.playlist_id=vd.playlist_id inner join channel ch on ch.channel_id = pl.channel_id order by vd.view_count desc limit 10',
4: 'select vd.video_name, count(cmt.comment_id) no_of_comments from comment cmt inner join video vd on vd.video_id=cmt.video_id group by vd.video_name order by count(cmt.comment_id) desc',
5: 'select ch.channel_name, vd.video_name, vd.like_count from video vd inner join playlist pl on pl.playlist_id=vd.playlist_id inner join channel ch on ch.channel_id=pl.channel_id group by vd.video_name order by vd.like_count desc',
6: 'select vd.video_name, vd.like_count, vd.dislike_count from video vd order by vd.like_count desc',
7: 'SELECT channel_name, channel_views FROM channel order by channel_views desc',
8: 'select distinct ch.channel_name from video vd inner join playlist pl on pl.playlist_id=vd.playlist_id inner join channel ch on ch.channel_id=pl.channel_id where year(published_date) = 2022',
9: 'select ch.channel_name,round((AVG(TIME_TO_SEC(vd.duration)))/60,2)  avg_duration_mins from video vd inner join playlist pl on pl.playlist_id = vd.playlist_id inner join channel ch on ch.channel_id=pl.channel_id group by channel_name order by channel_name asc',
10: 'select ch.channel_name, vd.video_name, count(comment_id)  no_of_comments from comment cmt inner join video vd on vd.video_id=cmt.video_id inner join playlist pl on pl.playlist_id = vd.playlist_id inner join channel ch on ch.channel_id = pl.channel_id group by vd.video_id order by count(comment_id) desc'
}











