# Project: Data Warehouse with Chess games

## Introduction

<p>Chess still remains one of the most popular board games for the last several hundred years. Making progress in chess - just like any other game - has always been the the topic of great iterest among both chess amateurs and pros.</p>

<p>With time though different chess variants emerged, both for joy and for education. Chess960, Hoardes, Three Checks etc. become relatively popular and are often discussed as a way to progress in chess for those who want to do it faster than the classical way.</p>

<p>The goal of this current project is threefold.
First, it was planned to gain expertise with Amazon Redshift in managing big enough datasets.
Second, I've planned to practice working with the full cycle of ETL starting from API calls and finishing with DWH data.
Third, I wanted to understand the approaches to create the framework for the analysis of chess variants and how these variants affect the performance in classical chess.
</p>

<p>The source data resides in S3 and needs to be processed in a data warehouse in Amazon Redshift. The source datasets consist of CSV files received from Kaggle and several datasets as per the responses from Lichess API.</p>

## Datasets

For this project, the following datasets were used:

>https://www.kaggle.com/datasets/timhanewich/5-million-chess-game-results-november-2019 - the dataset from Kaggle that contains the results of 5 million games from November 2019;<br>
>Datasets created by me after getting the data from lichess API:
>- the games of all the players from the abovementioned Kaggle dataset played from November 2019 until March 2023. The data is received from Lichess API;
>- the publicly accessible data about all players from the Kaggle dataset. The data is received from Lichess API;
>- a small dataset with the data about all Lichess bot players. The data is received from Lichess API.<br>
>Testing subsets are available at **s3://chess-games-bucket/**. The project has been tested on the data from this bucket.


### Stage tables
<p>The stage tables are created to load the data from S3 to Amazon Redshift. All the data is loaded via SQL COPY statement based on the parameters provided. </p>
<p>The following stage tables are created:
https://www.kaggle.com/datasets/timhanewich/5-million-chess-game-results-november-2019 - the dataset from Kaggle that contains the results of 5 million games from November 2019;<br>
Datasets created by me after getting the data from lichess API:
* the games of all the players from the abovementioned Kaggle dataset played from November 2019 until March 2023. The data is received from Lichess API;
 the publicly accessible data about all players from the Kaggle dataset. The data is received from Lichess API;
 a small dataset with the data about all Lichess bot players. The data is received from Lichess API.<br>
>Testing subsets are available at **s3://chess-games-bucket/**. The project has been tested on the data from this bucket.
</p>

### Fact and Dimension Tables
The analytics idea of the project is to understand the progress of the games in games. So far, the fact table is **games**, and it contains all the information about the games played by a certain player both from the Kaggle dataset and from the datasets created from API responses.
The following **dimension** tables have been created: 
1. **variants** - this table possesses variants of games with the corresponding codes used in the fact table. 17 game types (variants) are tracked there;
2. **results** - it has just 3 definite results - "white win", "black win" and "draw" (with stalemate also regarded as draw) - and the codes for them;
3. **players** - this table contains data about the players, including his/her first and last names, bio information which is optional, url of the data, player's country and location, whether the account is disabled or not, player's title, violation of TOS, all the ratings and player type (to track whether the player is a bot).


## The structure of the project

The project consisits of the following files:
1. **AWS.py** - the file that maintains the creation of Redshift cluster, getting the roles and permissions, and the coonections to the DB;
2. **create_tables.py** - this file runs the scripts that create all the schema for the project;
3. **etl.py** - the file that copies data from datasources like CSV files. Also the scripts provide transmission of the data from staging to DWH tables;
4. **sql_scripts.py** - this is the file with SQL scripts for schema creation, copy and insert of the data for *create_tables.py* and *etl.py*;
5. **get_data.py** - this file was used prior to all the work with the project. Its main idea is to create the staging tables in S3. Generally these tables were created from API responses;
6. **log_setup.py** - this small file is used only to set the logging parameters;
7. **dwh.cfg** - config file that contains the necessary information to connect with  S3, create the cluster, get permissions and roles etc.
8. logfile.log - this file is not included in GitHub version of the project. The file is used to log all the information during project's execution.

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