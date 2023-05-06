# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 02:48:01 2023

@author: Sergt
"""
import configparser
import psycopg2
import boto3
import requests
import pandas as pd
import awswrangler as wr 
import json
from io import StringIO
import logging
import time
import log_setup
import tenacity 
from tenacity import retry, stop_after_attempt, stop_after_delay

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
BUCKETNAME             = config.get('S3','bucketname')
BUCKETPATH             = ('s3://' + BUCKETNAME + '/')

aws_pd = pd.DataFrame({"Param": ["KEY", "SECRET"],
                       "Value": [KEY, SECRET]
                       })


session = boto3.session.Session(aws_access_key_id=KEY, 
                                aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

DbBucket = s3.Bucket(('{}').format(BUCKETNAME))
playergames = "/playergames/"
playersdata = "/playersdata/"

def get_last_update_date():
    '''
    Description: This function is used to get current date for data updates. Getting last Monday idea is taken from here: https://www.tutorialspoint.com/How-to-find-only-Monday-s-date-with-Python
    
    
    Purpose: The date is used to get current date in Unix epoch format. The date is inserted in API call to receive the most recent data updates.
    
    Arguments:
        None.
        
    Returns:
        EPOCH_DATE: Date in Unix epoch format.
    
    '''
    logging.message("Getting the current date/time in Unix epoch format...")
    
    try:
        
        import datetime
        
        # importing relativedelta, MO from dateutil
        from dateutil.relativedelta import relativedelta, MO

        # getting today's current local date
        todayDate = datetime.date.today()
        
        # Pass MO(-2) as an argument to relativedelta to set weekday as Monday and (-2) signifies last week's Monday. (-2) is used because the file is run on Mondays, so (-1) will capture the actual date of update
        lastMonday = todayDate + relativedelta(weekday=MO(-2))

        updateDatetime = datetime.datetime.combine(lastMonday, 
                          datetime.time(7, 0))
        
        # Getting epoch
        EPOCH_DATE = int((updateDatetime).timestamp())
        
        return (EPOCH_DATE)
    
    except Exception as e:
            
        logging.exception(e)
        
def get_players(cur, conn):
    '''
    Description: This function is used to get the list of players from the dataset. put it in S3 bucket as a csv file. 
    
    Purpose: The function files created by this function in S3 are to be copied to Redshift staging table.
    
    Arguments:
        cur: cursor
        conn: connection to the database
        
    Returns:
        players: the list of players
    
    '''
    
    
    try:
        logging.message("Getting players from the dataset...")
        
        cur.execute('SELECT pl.username FROM players pl;')
        
        players = cur.fetchall()
        
        return(players)
    
    except Exception as e:
            
        logging.exception(e)

def get_players_games_api(players):
    
    '''
        Description: This function is used to get player's games from lichess API and save them as a csv file. 
        
        Purpose: The files created by this function in S3 are to be copied to Redshift staging table.
        
        Details:
            For each player it: 
            1. Gets the API response from open lichess API is received in NDJson format. It is done by get_single_player_games_api() function;
            2. Processes the response as Pandas dataframe and saves as a CSV file in /playergames/. The filename is the username of the player. It is done by save_playergames_response()
            
        Arguments:
            players: a list of players whose games are to be received.
            
        Returns:
            None
    '''
    
    for p in players:
        save_playergames_response(get_single_player_games_api(p))          


@retry(reraise=True, stop=stop_after_delay(30) | stop_after_attempt(3))
def process_api_call(url):
    
    '''
        Description: 
            This function is used to process the request to API and the response from it. 
        
        Purpose: 
            Every response needs checked if it is possible to process it further. 
            For example, according to the lichess API documentation https://lichess.org/api#section/Introduction/Rate-limiting, 
                Only make one request at a time. 
                If you receive an HTTP response with a 429 status, please wait a full minute before resuming API usage.
        
        Details:
            The function checks request status code. 
            If the code is 2xx, the resopnse is returned to the caller function.
            If the status code is 429, the function waits for 70 seconds and retries the request.
            If the response is neither 429 nor 2xx, the function waits for 3 seconds and retries the request.
            If the response is not received for 30 seconds the retry fails.
            The function stops retrying after 3 attempts.
            
            
        Arguments:
            url: the url of the request.
            
        Returns:
            r: full response.
    '''
    
    try:
        
        EPOCH_DATE = get_last_update_date()
        
        r = requests.get(
            url,
            params={"since":EPOCH_DATE}, headers={"Accept": "application/x-ndjson"})

        if (r.status_code < 200) or (r.status_code > 299):
            logging.info("Status code is " + str(r.status_code))
            if r.status_code == 429:
                logging.warning('Status code is 429!!')
                time.sleep(70)
                raise tenacity.TryAgain
            
            else:
                logging.exception('Status code is not 2xx!')
                time.sleep(3)
                raise tenacity.TryAgain
            
        else:
            log_string = ("Status code is {}").format(str(r.status_code))
            logging.info(log_string)
            
            
            time.sleep(3)
            
            return(r)
            
    except Exception as e:
        raise
        logging.exception(e)
        
def get_single_player_games_api(p):
    
    '''
        Description: 
            This function is used to get player's games from lichess API. 
        
        Purpose: 
            The files created by this function in S3 are to be copied to Redshift staging table.
        
        Details:
            The function creates the URL for the player's data and calls process_api_call with the corresponding URL.             
            
        Arguments:
            p: player's ID.
            
        Returns:
            p_list: the list with the player's ID (p) and the response. Both parameters are passed to save_playergames_response() function.
    '''
        
    try:
        
        logging.info("Processing players' games")
        
        #Process each player 
        
            
        MESSAGE = ("Processing the games of player {}").format(str(p))
            
        logging.info(MESSAGE)
            
        url = ("https://www.lichess.org/api/games/user/{}").format(str(p))
            
        # 1. For each player the API response from open lichess API is received in NDJson format
        
        r_text = process_api_call(url).content.decode("utf-8")   
            
        
        
        return(list((p, r_text)))
            
    except Exception as e:
              
          logging.exception(e)
          pass
          
          
def save_playergames_response(p_list):
    
    '''
    Description: 
        This function gets a list with the name of the player and the response from API, and puts the players' games data as a csv file in S3 bucket. 
    
    Purpose: 
        The files created by this function and uploaded to S3 bucket are to be copied to Redshift staging table.
    
    Details:
        
        
    Arguments:
        p_list: consists of 2 items:
                p: player's name to save a file with the name;
                r_text: data to be saved as a csv file.
        
    Returns:
        None
        
    '''
        # 2. The response is processed as normalized Pandas dataframe and saved as a CSV file in /playergames/. The filename is the username of the player.
    try:        
                p = p_list[0]
                r_text = p_list[1]
                
                games = [json.loads(s) for s in r_text.split("\n")[:-1]]
                df = pd.json_normalize(games)
            
                csv_buffer = StringIO()
                df.to_csv(csv_buffer)
            
                s3.Object(('{}').format(BUCKETNAME), (playergames + "{}.csv").format(str(p))).put(Body=csv_buffer.getvalue())
            
                MESSAGE = ("{}.csv file is uploaded to the bucket successfully").format(str(p))
                logging.info(MESSAGE)
        
    except Exception as e:
            logging.exception(e)
            pass
    

def get_players_data(players):
    
    '''
        Description: This function is used to get player's data from lichess API and put it in S3 bucket as a csv file. It is done for each player from the list.
        
        Purpose: The files created by this function in S3 are to be copied to Redshift staging table.
        
        Details:
            1. For each player the API response (from the client) is received in Json format;
            2. The response is processed as a normalized Pandas dataframe;
            3. Each players' data are appended into the dataframe;
            4. The final dataframe is saved as a CSV file in /playerdata/. 
            
        Arguments:
            players: a list of players whose games are to be received.
            
        Returns:
            None
    '''
    
    try:
        
        logging.info("Processing players' data...")
        
        #myClient = lc.Client()

        df = pd.DataFrame()
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
    
        s3.Object(('{}').format(BUCKETNAME), (playersdata + "playersdata.csv")).put(Body=csv_buffer.getvalue())
        
        for p in players:
            
            MESSAGE = ("Beginning to get the data of player {}").format(str(p))
            logging.info(MESSAGE)
            
            df = wr.s3.read_csv(path=(BUCKETPATH + playersdata + "playersdata.csv"), index_col=False,
                                boto3_session=session,
                                sep=',')
            
            
            
        # 1. For each player the API response (from the client) is received in Json format;
        
            url = ("https://lichess.org/api/user/{}").format(str(p))
            
            player = process_api_call(url).content.decode("utf-8")
                        
            MESSAGE = ("Player {} data is received and are being proccessed").format(str(p))
            logging.info(MESSAGE)
            
        # 2. The response is processed as a normalized Pandas dataframe. 
            
            
            df_temp = pd.json_normalize(json.loads(player))
            logging.info('df_temp = df_temp.set_index("username")')
            
            
            df_temp["id"] = df_temp["id"].tolist()
            
            #print("df_temp" + str(df_temp))
            
        # 3. The response is added to the dataframe
            
            
            df = df.append(df_temp, True)
            df = df.drop(df.columns[[0]], axis=1)
            
            
            
            df["id"] = df["id"].tolist()
            df["id"].to_string(index=False)
            
            
            
            MESSAGE = ("Players' {} data have been appended").format(str(p))
            logging.info(MESSAGE)
            
        # 4.  The whole dataframe of the data of players is saved as a CSV file in /playerdata/
        
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            
            s3.Object(('{}').format(BUCKETNAME), (playersdata + "playersdata.csv")).put(Body=csv_buffer.getvalue())
        
    except Exception as e:
        logging.exception(e)
        
def preprocess_games_data():
    '''
        Description: This function is used to optimize the data that is to be loaded to the DWH.
        
        Purpose: Unnecessary messy columns are truncated. Unnecessary data are removed.
        
        Arguments:
            None
            
        Returns:
            None
    '''
    prefix_objs = DbBucket.objects.filter(Prefix=playergames)
    
    count = 0
    
    for object_summary in prefix_objs:
        
        if count > 0:
        
            
        
            logging.info("Loading " + object_summary.key)
        
            df = wr.s3.read_csv(path=BUCKETPATH + (object_summary.key), index_col=False,
                            boto3_session=session,
                            sep=',')
        
            df2 = df[["id", "rated", "variant", "speed", "perf", "createdAt", "lastMoveAt", "status", "winner", "moves", "players.white.user.name", "players.white.user.id", "players.white.rating", "players.white.ratingDiff", "players.black.user.name", "players.black.user.id", "players.black.rating", "players.black.ratingDiff"]]
        
        
        
            csv_buffer = StringIO()
            df2.to_csv(csv_buffer)
        
            s3.Object(('{}').format(BUCKETNAME), (object_summary.key)).put(Body=csv_buffer.getvalue())
        
            logging.info("Processing of " + object_summary.key + " has been finished")
            
        else:
            count += 1
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    pls = get_players(cur, conn)
    
    get_players_games_api(pls)
    
    get_players_data(pls)
    
    preprocess_games_data()

if __name__ == "__main__":
    main()