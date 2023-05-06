# 5 Million Chess Game Results - November 2019: https://www.kaggle.com/datasets/timhanewich/5-million-chess-game-results-november-2019

import configparser
from get_data import playergames, playersdata


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN           = config.get("DWH", "DWH_IAM_ROLE_ARN")
BUCKETNAME             = config.get('S3','bucketname')
BUCKETADDRESS          =  ("s3://{}/").format(BUCKETNAME)

# DROP TABLES

staging_data2019_table_drop = "DROP TABLE IF EXISTS staging_games2019_data"

staging_players_table_drop = "DROP TABLE IF EXISTS staging_players"

staging_players_games_data_table_drop = "DROP TABLE IF EXISTS staging_players_games_data"

staging_bots_table_drop = "DROP TABLE IF EXISTS staging_bots"


 
gameTypes_table_drop = "DROP TABLE IF EXISTS variants"

speed_table_drop = "DROP TABLE IF EXISTS speed"

results_table_drop = "DROP TABLE IF EXISTS results"
 
games_table_drop = "DROP TABLE IF EXISTS games"

players_table_drop = "DROP TABLE IF EXISTS players"
 
playerTypes_table_drop = "DROP TABLE IF EXISTS playerTypes"
 
# CREATE TABLES

## STAGING

# The staging table for the main dataset
staging_games2019_data_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_games2019_data(
Event            text,
Site             text,
Gamedate         text,
Round            text,
White            text,
Black            text,
Result           text,
UtcDateTime      text, 
WhiteElo         smallint,
BlackElo         smallint,
WhiteRatingDiff  int,
BlackRatingDiff  int,
ECO              text,
OpeningName      text,
TimeControl      text,
TimeIncrement    text,
Termination      text,
Move1            text,
Move2            text,
Move3            text,
Move4            text,
Move5            text,
Move6            text,
Move7            text,
Move8            text,
Move9            text,
Move10           text,
Move11           text,
Move12           text,
Move13           text,
Move14           text,
Move15           text,
Move16           text,
Move17           text,
Move18           text,
Move19           text,
Move20           text,
Move21           text,
Move22           text,
Move23           text,
Move24           text,
Move25           text,
Move26           text,
Move27           text,
Move28           text,
Move29           text,
Move30           text,
Move31           text,
Move32           text,
Move33           text,
Move34           text,
Move35           text,
Move36           text,
Move37           text,
Move38           text,
Move39           text,
Move40           text,
Move41           text,
Move42           text,
Move43           text,
Move44           text,
Move45           text,
Move46           text,
Move47           text,
Move48           text,
Move49           text,
Move50           text,
Move51           text,
Move52           text,
Move53           text,
Move54           text,
Move55           text,
Move56           text,
Move57           text,
Move58           text,
Move59           text,
Move60           text,
Move61           text,
Move62           text,
Move63           text,
Move64           text,
Move65           text,
Move66           text,
Move67           text,
Move68           text,
Move69           text,
Move70           text,
Move71           text,
Move72           text,
Move73           text,
Move74           text,
Move75           text,
Move76           text,
Move77           text,
Move78           text,
Move79           text,
Move80           text,
Move81           text,
Move82           text,
Move83           text,
Move84           text,
Move85           text,
Move86           text,
Move87           text,
Move88           text,
Move89           text,
Move90           text,
Move91           text,
Move92           text,
Move93           text,
Move94           text,
Move95           text,
Move96           text,
Move97           text,
Move98           text,
Move99           text,
Move100          text,
Move101          text,
Move102          text,
Move103          text,
Move104          text,
Move105          text,
Move106          text,
Move107          text,
Move108          text,
Move109          text,
Move110          text,
Move111          text,
Move112          text,
Move113          text,
Move114          text,
Move115          text,
Move116          text,
Move117          text,
Move118          text,
Move119          text,
Move120          text,
Move121          text,
Move122          text,
Move123          text,
Move124          text,
Move125          text,
Move126          text,
Move127          text,
Move128          text,
Move129          text,
Move130          text,
Move131          text,
Move132          text,
Move133          text,
Move134          text,
Move135          text,
Move136          text,
Move137          text,
Move138          text,
Move139          text,
Move140          text,
Move141          text,
Move142          text,
Move143          text,
Move144          text,
Move145          text,
Move146          text,
Move147          text,
Move148          text,
Move149          text,
Move150          text,
Move151          text,
Move152          text,
Move153          text,
Move154          text,
Move155          text,
Move156          text,
Move157          text,
Move158          text,
Move159          text,
Move160          text,
Move161          text,
Move162          text,
Move163          text,
Move164          text,
Move165          text,
Move166          text,
Move167          text,
Move168          text,
Move169          text,
Move170          text,
Move171          text,
Move172          text,
Move173          text,
Move174          text,
Move175          text,
Move176          text,
Move177          text,
Move178          text,
Move179          text,
Move180          text,
Move181          text,
Move182          text,
Move183          text,
Move184          text,
Move185          text,
Move186          text,
Move187          text,
Move188          text,
Move189          text,
Move190          text,
Move191          text,
Move192          text,
Move193          text,
Move194          text,
Move195          text,
Move196          text,
Move197          text,
Move198          text,
Move199          text,
Move200          text
)
""")

# Staging table for CSV player's data file 
staging_players_table_create = ("""CREATE TABLE IF NOT EXISTS staging_players (
indx                   int,
id                     text,
username               text,
createdAt              text,
seenAt                 text,
url                    text,
chess960_games         real,
chess960_rating        real,
chess960_rd            real,
chess960_prog          real,
chess960_prov          boolean,
antichess_games        real,
antichess_rating       real,
antichess_rd           real,
antichess_prog         real,
antichess_prov         boolean,
atomic_games           real,
atomic_rating          real,
atomic_rd              real,
atomic_prog            real,
atomic_prov            boolean,
ultraBullet_games      real,
ultraBullet_rating     real,
ultraBullet_rd         real,
ultraBullet_prog       real,
ultraBullet_prov       boolean,
blitz_games            real,
blitz_rating           real,
blitz_rd               real,
blitz_prog             real,
blitz_prov             boolean,  
crazyhouse_games       real,
crazyhouse_rating      real,
crazyhouse_rd          real,
crazyhouse_prog        real,
crazyhouse_prov        boolean,
bullet_games           real,
bullet_rating          real,
bullet_rd              real,
bullet_prog            real,
bullet_prov            boolean,
correspondence_games   real,
correspondence_rating  real,
correspondence_rd      real,
correspondence_prog    real,
correspondence_prov    boolean,
horde_games            real,
horde_rating           real,
horde_rd               real,
horde_prog             real,
horde_prov             boolean,
puzzle_games           real,
puzzle_rating          real,
puzzle_rd              real,
puzzle_prog            real,
puzzle_prov            boolean,
classical_games        real,
classical_rating       real,
classical_rd           real,
classical_prog         real,
classical_prov         boolean,
rapid_games            real,
rapid_rating           real,
rapid_rd               real,
rapid_prog             real,
rapid_prov             boolean,
playTime_total         real,
playTime_tv            real,
count_all	           real,
rated	               real,
ai	                   real,
draw                   real,
drawH                  real,
loss	               real,
lossH	               real,
win	                   real,
winH                   real,
bookmark	           real,
count_playing          real,
import	               real,
me	                   real,
threeCheck_games       real,
threeCheck_rating      real,
threeCheck_rd          real,
threeCheck_prog        real,
threeCheck_prov        boolean,
country                text,
location               text,
disabled               boolean,
kingOfTheHill_games    real,
kingOfTheHill_rating   real,
kingOfTheHill_rd       real,
kingOfTheHill_prog     real,
kingOfTheHill_prov     boolean,
streak_runs            real,
streak_score           real,
storm_runs             real,
storm_score            real,
firstName              text,
bio                    text,
lastName               text,
patron                 boolean,
racer_runs             real,
racer_score            real,
dsbRating              real,
fideRating             real,
ecfRating	           real,
uscfRating             real,
links                  text,
racingKings_games      real,
racingKings_rating     real,
racingKings_rd         real,
racingKings_prog       real,
racingKings_prov       boolean,
playing                text,
title                  text,
tosViolation           boolean,
cfcRating              real,
rcfRating              real
)
""")

# The table is used to upload each gamer's games into Redshift from S3
staging_players_games_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_players_games_data(
indx             int,
id               text,
rated            bool,
variant          text,
speed            text,
perf             text,
createdAt        bigint,
lastMoveAt       bigint,
status           text,
winner           text,
moves            VARCHAR(MAX),
white_username   text,
white_userid     text,
white_rating     text,
white_ratingDiff text,
black_username   text,
black_userid     text,
black_rating     text,
black_ratingDiff text
) 
DISTSTYLE AUTO;
""")

# Staging table for the bots from CSV file
staging_bots_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_bots(
playerId         text,
username         text,
rating           smallint
)
""")

## DWH tables

gameTypes_table_create = ("""
CREATE TABLE IF NOT EXISTS variants(
typeID           int IDENTITY (0,1) NOT NULL,
enumeration      smallint,
name             text
) diststyle all;
""")

speed_table_create = ("""
CREATE TABLE IF NOT EXISTS speed(
typeID           int IDENTITY (0,1) NOT NULL,
enumeration      smallint,
name             text
) diststyle all;
""")

results_table_create = ("""
CREATE TABLE IF NOT EXISTS results(
resultID         int IDENTITY (0,1) NOT NULL,
resultCode       int,
resultName       text
) diststyle all;
""")

games_table_create = ("""
CREATE TABLE IF NOT EXISTS games(
gameId           text, 
blackPlayer      text, 
bpRating         real,
bRatingDiff      real, 
whitePlayer      text,
wpRating         real,
wRatingDiff      real, 
result           int,
ECO              text,
moves            VARCHAR(MAX),
variant          int,
speed            int,
mth              smallint distkey,
yr               smallint,
PRIMARY KEY(gameId)
) SORTKEY AUTO;
""")

player_table_create = ("""CREATE TABLE IF NOT EXISTS players (
recordId               int IDENTITY (0,1) NOT NULL,
id                     text,
username               text,
firstName              text,
lastName               text,
bio                    text,
url                    text,
country                text,
location               text,
disabled               boolean,
patron                 boolean,
links                  text,
title                  text,
tosViolation           boolean,
fideRating             int,
uscfRating             int,
ecfRating	           int,
dsbRating              int,
cfcRating              int,
rcfRating              int,
playertype             int
) sortkey AUTO
""")

playerTypes_table_create = ("""CREATE TABLE IF NOT EXISTS playerTypes(
playerTypeId int IDENTITY (0,1) NOT NULL,
typeCode        int,
typeName        text
)
""")

# COPY INTO STAGING TABLES

staging_games2019_data_copy = ("""
COPY staging_games2019_data
FROM '{}' iam_role '{}'
CSV 
FILLRECORD
MAXERROR 50000;
""").format(BUCKETADDRESS + "November2019.csv", DWH_ROLE_ARN)

staging_players_games_data_copy = ("""
COPY staging_players_games_data
FROM '{}' iam_role '{}' 
CSV
IGNOREHEADER 1
FILLRECORD
MAXERROR 3500;
""").format(BUCKETADDRESS + playergames, DWH_ROLE_ARN)

staging_players_copy = ("""
COPY staging_players
FROM '{}' iam_role '{}' 
CSV
IGNOREHEADER 1
MAXERROR 3500
""").format(BUCKETADDRESS + playersdata + "playersdata.csv", DWH_ROLE_ARN)

staging_bots_table_copy = ("""
COPY staging_bots
FROM '{}' iam_role '{}'
CSV 
IGNOREHEADER 1
""").format(BUCKETADDRESS + "bots.csv", DWH_ROLE_ARN)                         


# POPULATE FINAL TABLES

insert_playerTypes = ("""
INSERT INTO playerTypes (typeCode, typeName)
VALUES 
(0, 'HUMAN'),
(1, 'BOT');
""")

insert_results = ("""
INSERT INTO results(resultCode, resultName)
VALUES 
(1,'white win'),
(0,'black win'),
(2,'draw'),
(3, 'other')
""")

insert_bots = ("""
INSERT INTO players (id, username, fideRating)
SELECT * FROM staging_bots;

INSERT INTO players (playerType)
VALUES(1) 
""")

insert_players_data = ("""
INSERT INTO players(
id, 
username,
firstName,
lastName,
bio,
url,
country,
location,
disabled,
patron,
links,
title,
tosViolation,
fideRating,
dsbRating,
ecfRating,
uscfRating,
cfcRating,
rcfRating
)

SELECT 
id, 
username,
firstName,
lastName,
bio,
url,
country,
location,
disabled,
patron,
links,
title,
tosViolation,
fideRating,
dsbRating,
ecfRating,
uscfRating,
cfcRating,
rcfRating
FROM staging_players sp
WHERE sp.title NOT LIKE 'BOT';

UPDATE players
SET playerType = 0
WHERE playerType IS NULL;
""")

gameType_table_insert = ("""
INSERT INTO variants (enumeration, name)
VALUES(0, 'standard');
INSERT INTO variants (enumeration, name)
VALUES(1, 'correspondence');
INSERT INTO variants (enumeration, name)
VALUES(2, 'chess960');
INSERT INTO variants (enumeration, name)
VALUES(3, 'crazyhouse');
INSERT INTO variants (enumeration, name)
VALUES(4, 'antichess');
INSERT INTO variants (enumeration, name)
VALUES(5, 'atomic');
INSERT INTO variants (enumeration, name)
VALUES(6, 'horde');
INSERT INTO variants (enumeration, name)
VALUES(7, 'kingOfTheHill');
INSERT INTO variants (enumeration, name)
VALUES(8, 'racingKings');
INSERT INTO variants (enumeration, name)
VALUES(9, 'threeCheck');
INSERT INTO variants (enumeration, name)
VALUES(10, 'puzzle');
INSERT INTO variants (enumeration, name)
VALUES(11, 'streak');
INSERT INTO variants (enumeration, name)
VALUES(12, 'storm');
INSERT INTO variants (enumeration, name)
VALUES(13, 'racer');
INSERT INTO variants (enumeration, name)
VALUES(14, 'other');
""")

speed_table_insert = ("""
INSERT INTO speed (enumeration, name)
VALUES(0, 'ultraBullet');
INSERT INTO speed (enumeration, name)
VALUES(1, 'bullet');
INSERT INTO speed (enumeration, name)
VALUES(2, 'blitz');
INSERT INTO speed (enumeration, name)
VALUES(3, 'rapid');
INSERT INTO speed (enumeration, name)
VALUES(4, 'classical');
INSERT INTO speed (enumeration, name)
VALUES(5, 'correspondence');
INSERT INTO speed (enumeration, name)
VALUES(6, 'other');
""")

games_table_staging_insert = ("""
INSERT INTO games(
gameId, 
whitePlayer,
wpRating,
wRatingDiff, 
blackPlayer,
bpRating,
bRatingDiff, 
result,
ECO,
variant,
speed,
mth,
yr
)
SELECT 
substring(sg.Site,21,8),
sg.White,
CAST(sg.WhiteElo AS real),
CAST(sg.WhiteRatingDiff AS real),
sg.Black,
CAST(sg.BlackElo AS real),
CAST(sg.BlackRatingDiff AS real),
CASE
    WHEN sg.Result LIKE '1-0' THEN 1
    WHEN sg.Result LIKE '0-1' THEN 0
    WHEN sg.Result LIKE '1/2-1/2' THEN 2
    ELSE 3
    end::int AS result,
sg.ECO,
CASE
    WHEN sg.Event ILIKE '%ultraBullet%' THEN 0
    WHEN sg.Event ILIKE '% Bullet%' THEN 1
    WHEN sg.Event ILIKE '%Blitz%' THEN 2
    WHEN sg.Event ILIKE '%Rapid%' THEN 3
    WHEN sg.Event ILIKE '%Classical%' THEN 4
    WHEN sg.Event ILIKE '%Correspondence%' THEN 5
    WHEN sg.Event ILIKE '%Chess960%' THEN 6
    WHEN sg.Event ILIKE '%Crazyhouse%' THEN 7
    WHEN sg.Event ILIKE '%Antichess%' THEN 8
    WHEN sg.Event ILIKE '%Atomic%' THEN 9
    WHEN sg.Event ILIKE '%Horde%' THEN 10
    WHEN sg.Event ILIKE '%KingOfTheHill%' THEN 11
    WHEN sg.Event ILIKE '%RacingKings%' THEN 12
    WHEN sg.Event ILIKE '%ThreeCheck%' THEN 13
    WHEN sg.Event ILIKE '%Puzzle%' THEN 14
    WHEN sg.Event ILIKE '%Streak%' THEN 15
    WHEN sg.Event ILIKE '%Storm%' THEN 16
    WHEN sg.Event ILIKE '%Racer%' THEN 17
    ELSE 18
    
    end::int AS variant,
    0,
extract('month' from CAST(sg.Gamedate AS timestamp)) as mth,
extract('year' from CAST(sg.Gamedate AS timestamp)) as yr
FROM staging_games2019_data as sg; 
""")

games_table_insert = ("""
INSERT INTO games(
gameId, 
whitePlayer,
wpRating,
wRatingDiff, 
blackPlayer,
bpRating,
bRatingDiff, 
moves,
variant,
speed,
result,
mth,
yr
)
SELECT
sd.id,
sd.white_username,
CASE
    WHEN sd.white_rating LIKE '%.%' THEN CAST(CAST(sd.white_rating AS real) AS int)    
    WHEN sd.white_rating SIMILAR TO '[0-9]{4}' THEN CAST(sd.white_rating AS int)
    ELSE 0
    end::int AS wpRating,
CASE
    WHEN sd.white_ratingDiff LIKE '%.%' THEN CAST(CAST(sd.white_ratingDiff AS real) AS int)
    WHEN sd.white_ratingDiff IS NULL THEN 0
    WHEN sd.white_ratingDiff SIMILAR TO '%[0-9]{4}' THEN CAST(sd.white_ratingDiff AS int)
    ELSE 0
    end::int AS wRatingDiff,    
sd.black_username,
CASE
    WHEN sd.black_rating LIKE '%.%' THEN CAST(CAST(sd.black_rating AS real) AS int)
    WHEN sd.black_rating SIMILAR TO '[0-9]{4}' THEN CAST(sd.black_rating AS int)
    ELSE 0
    end::int AS bpRating,
CASE
    WHEN sd.black_ratingDiff LIKE '%.%' THEN CAST(CAST(sd.black_ratingDiff AS real) AS int)
    WHEN sd.black_ratingDiff IS NULL THEN 0
    WHEN sd.black_ratingDiff SIMILAR TO '%[0-9]{4}' THEN CAST(sd.black_ratingDiff AS int)
    ELSE 0
    end::int AS bRatingDiff,     
sd.moves,
CASE
    WHEN sd.variant ILIKE 'standard%' THEN 0
    WHEN sd.variant ILIKE 'correspondence%' THEN 1
    WHEN sd.variant ILIKE 'chess960%' THEN 2
    WHEN sd.variant ILIKE 'crazyhouse%' THEN 3
    WHEN sd.variant ILIKE 'antichess%' THEN 4
    WHEN sd.variant ILIKE 'atomic%' THEN 5
    WHEN sd.variant ILIKE 'horde%' THEN 6
    WHEN sd.variant ILIKE 'kingofthehill%' THEN 7
    WHEN sd.variant ILIKE 'racingkings%' THEN 8
    WHEN sd.variant ILIKE 'threecheck%' THEN 9
    WHEN sd.variant ILIKE 'puzzle%' THEN 10
    WHEN sd.variant ILIKE 'streak%' THEN 11
    WHEN sd.variant ILIKE 'storm%' THEN 12
    WHEN sd.variant ILIKE 'racer%' THEN 13
    ELSE 14
    end::int AS variant,
CASE
    WHEN sd.speed ILIKE '%ultrabullet%' THEN 0
    WHEN sd.speed ILIKE 'bullet%' THEN 1
    WHEN sd.speed ILIKE 'blitz%' THEN 2
    WHEN sd.speed ILIKE 'rapid%' THEN 3
    WHEN sd.speed ILIKE 'classical%' THEN 4
    WHEN sd.speed ILIKE 'correspondence%' THEN 5
    ELSE 6
    end::int AS speed,
CASE
    WHEN sd.winner LIKE 'white' THEN 1
    WHEN sd.winner LIKE 'black' THEN 0
    WHEN sd.status LIKE 'draw' THEN 2
    WHEN sd.status LIKE 'stalemate' THEN 2
    ELSE 3
    end::INT as result,
extract('month' from (timestamp 'epoch' + CAST(sd.lastMoveAt AS BIGINT)/1000 * interval '1 second')) as mth,
extract('year' from (timestamp 'epoch' + CAST(sd.lastMoveAt AS BIGINT)/1000 * interval '1 second')) as yr    
FROM staging_players_games_data as sd
;
""")

DataQualityCheck1 = ("""
SELECT 
CASE
    WHEN COUNT(DISTINCT site) > 0 THEN True
    ELSE False
    end::BOOLEAN
FROM staging_games2019_data;
""")

DataQualityCheck2 = ("""
SELECT 
CASE
    WHEN COUNT(DISTINCT id) > 0 THEN True
    ELSE False
    end::BOOLEAN
FROM staging_players;
""")

DataQualityCheck3 = ("""
SELECT 
CASE
    WHEN COUNT(DISTINCT id) > 0 THEN True
    ELSE False
    end::BOOLEAN
FROM staging_players_games_data;
""")

DataQualityCheck4 = ("""
SELECT 
CASE
    WHEN COUNT(DISTINCT playerId) > 0 THEN True
    ELSE False
    end::BOOLEAN
FROM staging_bots;
""")

DataQualityCheck5 = ("""
SELECT 
CASE
    WHEN min(variant) < 0 OR max(variant) > 17 THEN 'FAILED. Incorrect variant value'
    WHEN min(result) < 0 OR MAX(result) > 2 THEN 'FAILED. Incorrect result value'
    WHEN MIN(yr) < 2019 OR MAX(mth) > 12 OR MIN(mth) < 1 THEN 'FAILED. Incorrect date values'
    ELSE 'PASSED'
    end::text
FROM   games
""")

# QUERY LISTS
drop_table_queries = [staging_data2019_table_drop, staging_players_table_drop, staging_players_games_data_table_drop, staging_bots_table_drop,  gameTypes_table_drop, games_table_drop, results_table_drop, players_table_drop, playerTypes_table_drop, speed_table_drop]
create_table_queries = [staging_games2019_data_table_create, staging_players_games_table_create, staging_bots_table_create, staging_players_table_create, results_table_create, games_table_create, player_table_create, playerTypes_table_create, gameTypes_table_create, speed_table_create]
copy_table_queries = [staging_games2019_data_copy, staging_players_games_data_copy, staging_bots_table_copy, staging_players_copy]
staging_data_quality_checks = [DataQualityCheck1, DataQualityCheck2, DataQualityCheck3, DataQualityCheck4]
insert_table_queries = [insert_playerTypes, games_table_staging_insert, insert_results, gameType_table_insert, insert_bots, insert_players_data, games_table_insert, speed_table_insert]
insert_data_quality_check = DataQualityCheck5
