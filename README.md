# Project: Data Warehouse on AWS


### Database
The purpose of the database is to enable the startup Sparkify to easily do song play analysis on their data from their music streaming app using data warehouse on AWS.

### Schema of the database
#### Fact table
1. **songplays** - records in log data associated with song plays
  - *user_id, first_name, last_name, gender, level*

#### Dimension tables
2. **users** - users in the app
  - *user_id, first_name, last_name, gender, level* 
3. **songs** - songs in music database
  - *song_id, title, artist_id, year, duration* 
4. **artists** - artists in music database
  - *artist_id, name, location, latitude, longitude* 
5. **time** - timestamps of records in **songplays** broken down into specific units
  - *start_time, hour, day, week, month, year, weekday* 

All the tables has been normalized which increases data integrity as the number of copies of the data has been reduced, which means data only needs to be added or updated in very few places. The schema is a star schema where the fact table, **songplays** is referencing all the dimnension tables.

### Files
- **create_tables.py** - used to connect to the sparkify database and drop any existing tables and create new ones with the above
- **etl.py** - used to stage all song data and log data files residing in S3 buckets into staging tables in redshift and then transform the data into fact and dimension tables
- **sql_queries** - contains SQL queries creating all the tables, copying into staging tables inserting into the tables and dropping the tables