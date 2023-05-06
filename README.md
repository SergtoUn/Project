# Project: Data Warehouse with Chess games

## Introduction

<p>Chess still remains one of the most popular board games for the last several hundred years. Making progress in chess - just like any other game - has always been the the topic of great iterest among both chess amateurs and pros.</p>
<p>With time though different chess variants emerged, both for joy and for education. Chess960, Hoardes, Three Checks etc. become relatively popular and are often discussed as a way to progress in chess for those who want to do it faster than the classical way.</p>

<p>The goal of this current project is threefold.
First, it was planned to gain expertise with Amazon Redshift in managing big enough datasets.
Second, I've planned to practice working with the full cycle of ETL starting from API calls and finishing with DWH data.
Third, I wanted to understand the approaches to create the framework for the analysis of chess variants and how these variants affect the performance in classical chess.
The general idea is to compare the results of the players from the Kaggle dataset for November 2019 with the results of the following years. So far, only the games of the players from the original Kaggle dataset were collected. All the games are combined in one single table for partitions and analysis.
</p>
<p>The source data resides in S3 and needs to be processed in a data warehouse in Amazon Redshift. The source datasets consist of CSV file downloaded from Kaggle and several datasets as per the responses from Lichess API saved as CSV files as well.</p>

## Datasets

For this project, the following datasets were used:

>https://www.kaggle.com/datasets/timhanewich/5-million-chess-game-results-november-2019 - the dataset from Kaggle that contains the results of 5 million games from November 2019. Originally the dataset was manually copied to S3 from Kaggle to process it correctly;<br>
>Datasets created by me after getting the data from lichess API:
>- publicly accessible data about the games of all the players from the abovementioned Kaggle dataset played from November 2019 until March 2023. The data is received from Lichess API;
>- the publicly accessible data about all players from the Kaggle dataset. The data is received from Lichess API;
>- a small dataset with the data about all Lichess bot players. The data is received from Lichess API.<br>
>Testing subsets are available at **s3://chess-games-bucket/**. The project has been tested on the data from this bucket.

### Data Quantity of the Primary Dataset

The primary dataset for this project is https://www.kaggle.com/datasets/timhanewich/5-million-chess-game-results-november-2019. It's data were loaded into the stage table **staging_games2019_data**, it contains 5000995 records
 ![dataset!](dataset.png "dataset")

## Stage tables
<p>The stage tables are created to load the data from S3 to Amazon Redshift. All the data is loaded via SQL COPY statement based on the parameters provided. </p>
The following stage tables are created:
1. **staging_games2019_data** - this table is used to hold the data from the Kaggle dataset that contains the results of 5 million games from November 2019;
2. **staging_players_games_data** - the table that holds publicly accessible data about the games of the players from Kaggle dataset;
3. **staging_players** - this is the table that holds the data about the players derived from API calls to Lichess;
4. **staging_bots** - the table contains data bot players. This data is collected mainly to exclude bot players from the analysis. The secondary idea is to prepare the framework to analyze if the games with bot players are fruitful for the human palyer's performance.

## Fact and Dimension Tables
The analytics idea of the project is to understand the progress of the games in games. So far, the fact table is **games**, and it contains all the information about the games played by a certain player both from the Kaggle dataset and from the datasets created from API responses.
The following **dimension** tables have been created: 
1. **variants** - this table possesses variants of games with the corresponding codes used in the fact table. 14 game types (variants) are tracked there;
2. **results** - it has just 3 definite results - "white win", "black win" and "draw" (with stalemate also regarded as draw) - and the codes for them;
3. **speed** - this table possesses variants of time controls with the corresponding codes used in the fact table. 6 time controls are tracked there;
4. **players** - this table contains data about the players, including his/her first and last names, bio information which is optional, url of the data, player's country and location, whether the account is disabled or not, player's title, violation of TOS, all the ratings and player type (to track whether the player is a bot).

## The structure of the project

The project consisits of the following files:
1. **AWS.py** - the file that maintains the creation of Redshift cluster, getting the roles and permissions, and the coonections to the DB;
2. **create_tables.py** - this file runs the scripts that create all the schema for the project;
3. **etl.py** - the file that copies data from datasources like CSV files. Also the scripts provide transmission of the data from staging to DWH tables;
4. **sql_scripts.py** - this is the file with SQL scripts for schema creation, copy and insert of the data for *create_tables.py* and *etl.py*;
5. **get_data.py** - this file was used prior to all the work with the project. Its main idea is to create the staging tables in S3. Generally these tables were created from API responses;
6. **log_setup.py** - this small file is used only to set the logging parameters;
7. **dwh.cfg** - config file that contains the necessary information to connect with  S3, create the cluster, get permissions and roles etc.
8. **update_data.py** - the file run periodically in order to update the data. It requires changes in EPOCH_DATE set 
9. logfile.log - this file is not included in GitHub version of the project. The file is used to log all the information during project's execution.

## Steps to run

To run the project the following activities should be performed:
Preliminary steps:
- setup logging in *log_setup.py*;
- create datasets via get_data.py.
Project steps:
1. Edit *dwh.cfg* file, adding *key* and *secret*;
2. Run *AWS.py* to create the cluster, roles, and to connections;
3. run *create_tables.py* to create data schema;
4. run *etl.py* to copy data to staging tables and to insert data into data warehouse;
5. Run *clean_up_resources.py* to clean up the resources upon completion.

### The testing query to show the result of the data model

The following query is supposed to demonstrate the testingquery and its results to be used in the analysis
![wins!](wins.png "wins")
The same fact table with the dimensions joined:
![dimensions!](dimensions.png "dimensions")

## Data Quality Checks

In order to test correctness of the data quality the following checks are performed:
1. Check if the staging table **staging_games2019_data** contains any data;
2. Check if the staging table **staging_players_games_data** contains any data;
3. Check if the staging table **staging_players** contains any data;
4. Check if the staging table **staging_bots** contains any data;
5. Check if the data input in the **games** table is within the presupposed ranges.

## Logical Approach to Work with Potential Challenges

1. **The data was increased by 100x**. Unlike traditional databases, Redshift is designed to scale out by adding nodes to the cluster. Optimization comes from distribution styles of the columns over the clusters. So far, the following styles provide optimized performance:
   - besides the primary Kaggle dataset, all other data about games come from API calls. Chess games are approximately evenly distributed over the months, so the distribution key for the **games** is month;
   - the best key for the staging table **staging_players_games_data** is left to be automatically decided with the style **AUTO**;
   - small tables that take part in most of the processing - **variants** and **results** - are copied over the clusters with distribution style **ALL**.
2.  **The pipelines would be run on a daily basis by 7 am every day**. The project itself does not sort whether the data has been updated or daily we run all the same. Yet this can be sort out for **games** and **players** tables by either way, and the best solution is to be decided by the user:
   - create its temporary duplicate; add data from the datasets to the existing staging table; update the temporary table with just the newly received data; remove all the data from the staging table and put the one from the temporary one. After it the data to the DWH is updates as per the dates of the games (just the newly played games ar added). This approach requires solid amount of space exploited by AWS Redshift, because just a monthly data can cross 30Gb amount. Thus making duplicates can be pricey. Also the datasets are created via API calls, and getting most of the data that is already in the database takes enourmous and increasing amounts of time worthless;
   - daily the "folders" with datasets are updated, and with all the previous data removed and only newly received data left. So far it does not require significant changes in the DWH. Yet the get_data.py file needs to be updated daily to receive just new games.
   This second variant requires the following steps set up:
   a. *Deletion of data*. Can be performed by setting lifecycle configuration for the elements. It can consider the data management policies regarding both data transition an data removal. Further information on this topic is available here: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html;
   b. *Source updates*. The file to update the sources is update_data.py. It loads the data from lichess API to the corresponding "folders" inside the S3 bucket. The file is supposed to be started every Monday by the cron/job; 
   c. *ETL from S3 to DWH*. It is performed by etl.py file inside the project. It should be run as per the cron schedule as well;
   d. *Crontab*. Crontab is the file with the purpose to plan the execution of the task. The syntax to run the task, say, every Monday 7AM is 
   `0 7 * * 1`

3. **The database needed to be accessed by 100+ people.** This issue is not the limit in AWS Redshift as a cloud-based DWH. Concurrency scaling with up to 10 concurrency scaling clusters is available. Yet time- and resource-consuming activities like data updates are recommended to be performed during the time of minimum user activities. Also it is recommended to organize the users into the user groups.
   Recent limits of AWS Redshift are available here: http://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html



