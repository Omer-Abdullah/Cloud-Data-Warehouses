# Cloud-Data-Warehouses
data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each: NOTE: it is the same data used in the data modeling projects.

Song data: *s3://udacity-dend/song_data*

Log data: *s3://udacity-dend/log_data*

Log data json path: *s3://udacity-dend/log_json_path.json*

# Scripts description
1- create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.

2- etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

3- sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.

4- dwh.cfg is containing the S3 credintials, Redshift cluster details, IAM role and json files path.

5- test.ipnyp is containing some select statments used for testing purpose.

# Project Steps
Below are steps which will be implemented in order to finish this project.

# Create Table Schemas
1- Design schemas for your fact and dimension tables

2- Write a SQL CREATE statement for each of these tables in sql_queries.py

3- Complete the logic in create_tables.py to connect to the database and create these tables

4- Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.

5- Launch a redshift cluster and create an IAM role that has read access to S3.

6- Add redshift database and IAM role info to dwh.cfg.

7- Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.

# Build ETL Pipeline
1- Implement the logic in etl.py to load data from S3 to staging tables on Redshift.

2- Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.

3- Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.

4- Delete your redshift cluster when finished.

Note
The SERIAL command in Postgres is not supported in Redshift. The equivalent in redshift is IDENTITY(0,1), which you can read more on in the Redshift Create Table Docs.
