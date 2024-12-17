# Udacity Project - Sparkify Data Warehouse

# Introduction (from udacity)

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

# Data Warehouse Design

Before starting this project, we need to see what data we have and what we are asked to find.  Data warehouses are built to facilitate answering business users' questions.  Some of these questions may be unknown to the user at the time of development.  Therefore, it's important to select the appropriate grain for our fact table and make sure that we have conforming dimension tables.  In this project, it is fairly straightforward since we are given a single fact of song plays. 

# Step 1 - Inspect the Data

The data reside in "s3://udacity-dend/song_data" (song_data) and "s3://udacity-dend/log_data" (log_data)

**Song Data**
(from udadicy) *The first dataset is a subset of real data from the Million Song Dataset(opens in a new tab). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.*

An example song is: {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

So, we have the following properties in song_data with my estimated data types:

num_songs - int
artist_id - varchar
artist_latitude - double
artist_longitude - double
artist_location - varchar
artist_name - varchar
song_id - varchar
title - varchar
duration - double
year - int

**Log Data**

*The second dataset consists of log files in JSON format generated by this event simulator(opens in a new tab) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.*

*The log files in the dataset you'll be working with are partitioned by year and month. For example, here are file paths to two files in this dataset.*
[!log_data.jpg]

artist - Varchar
auth - Varchar
firstName - Varchar
gender - Varchar
itemInSession - int
lastName - varchar
length - double
level - varchar
location - varchar
method - varchar
page - varchar
registration - bigint
sessionId - int
song - varchar
status - int
ts - datetime (unix timestamp with ms since JAN 1, 1970)
userAgent - varchar
userId - int
