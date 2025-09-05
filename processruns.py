import json
import csv
import math
from datetime import datetime
import mysql.connector as mariadb
import sys
import logging
import os
import sys
import time
from env import EXCLUDED_PLAYERS, DB, DEVEL

excludedPlayers = EXCLUDED_PLAYERS # Requested to be excluded

_log = logging.getLogger('SpeedStats-V3')

def collectGroups(path: str, test: bool):
    with open(path, 'r') as file:
        groups = json.load(file)
        print(len(groups))
        if len(groups) < 600000 and not test:
            _log.error("There aren't enough groups!")
            sys.exit(1)
        return groups

def findNumWRs(runs: list):
    reverseTime = runs[0]['isReverseTime']
    dateSortedRuns = sorted(runs, key=lambda run: (run['date'], run['dateSubmitted']))
    currentWR = -1
    numWRs = 0
    for run in dateSortedRuns:
        if run['date'] <= 0 or run['time'] == None: # date or time is null
            continue
        
        if currentWR == -1 or (reverseTime and run['time'] > currentWR) or (not reverseTime and run['time'] < currentWR):
            currentWR = run['time']
            numWRs += 1
    return numWRs

def buildLeaderboard(runs: list):  
    dateSortedRuns = sorted(runs, key=lambda run: (run['date'], run['dateSubmitted']))
    
    reverseTime = runs[0]['isReverseTime']
    fullySortedRuns = sorted(dateSortedRuns, reverse = reverseTime, key = lambda run: (run['time']))
    
    uniquePlayerNames = []
    leaderboard = []
    
    for run in fullySortedRuns:
        if run.get('playerNames') not in uniquePlayerNames:
            uniquePlayerNames.append(run.get('playerNames'))
            leaderboard.append(run)
    
    return leaderboard

def processGroups(groups: dict):
    leaderboards = []
    for groupName, runs in groups.items():
        
        leaderboard = buildLeaderboard(runs)
        numWRs = findNumWRs(runs)
        leaderboardRuns = len(leaderboard)
        totalRuns = len(runs)
        runLength = (leaderboard[(leaderboardRuns - 1) // 2]['time'] % 10000000.0) / 60.0
        lengthWeight = (1.1 - math.pow(1.01, -(runLength + 200)) - math.pow(2.4, -(runLength + 1.2)))
        WRValue = (math.log(totalRuns, 1.7) * numWRs + 120 * math.exp(-100 / totalRuns) + 0.04 * totalRuns) * (1 - (numWRs + 1) / (totalRuns + leaderboardRuns)) * lengthWeight
        sf = (math.log(leaderboardRuns, 10) / leaderboardRuns) + 0.001 if leaderboardRuns > 2 else 0.2

        i = 0
        while i < leaderboardRuns:
            run = leaderboard[i]
            nominalPlace = i + 1
            run['place'] = nominalPlace
            tiedRuns = [(run, nominalPlace)]
            
            while i < leaderboardRuns - 1 and run['time'] == leaderboard[i + 1]['time']:
                i += 1
                nominalPlace += 1
                tiedRun = leaderboard[i]
                tiedRun['place'] = run['place']
                tiedRuns.append((tiedRun, nominalPlace))

            totalValue = 0
            for run, nominalPlace in tiedRuns:
                top = sf * WRValue * (leaderboardRuns + 1 - nominalPlace)
                bottom = nominalPlace + (sf * leaderboardRuns - 1)
                value = top / bottom

                if run['isLevelRun']:
                    value *= 0.75

                run['groupName'] = groupName
                totalValue += value
            
            runValue = totalValue / len(tiedRuns)
            for run, _ in tiedRuns:
                run['value'] = runValue

            i += 1
        
        leaderboards.append(leaderboard)
    return leaderboards

def generateCSV(leaderboards: dict, csvPath: str):
    with open(csvPath, mode='w', encoding='utf-8', newline='\n') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_ALL, lineterminator='\n')
        for leaderboard in leaderboards:

            name = leaderboard[0].get('groupName').replace("\\","\\\\")
            series = leaderboard[0].get('seriesName') 
            series = series.replace("\\","\\\\").replace(",", ".") if series != None else "\\N"
            game = leaderboard[0].get('gameName').replace("\\","\\\\")
            creditedPlayers = []

            for run in leaderboard:
                
                platform = run.get('platformName') if run.get('platformName') != None else "\\N"
                date = datetime.fromtimestamp(run.get('date')).strftime("%Y-%m-%d") if run.get('date') > 0 else "\\N"
                valuePerPlayer = (run.get('value') / len(run.get('playerNames')))
                valuePerPlayer = "{:.3f}".format(valuePerPlayer)
                
                for player in run.get('playerNames'):
                    isGuest = player == None or player[:7] == "[Guest]"
                    # Only credits players for their best run in co-op categories
                    if player not in creditedPlayers and player not in excludedPlayers and not isGuest:
                        creditedPlayers.append(player)
                        params = [name, series, game, player, platform, run.get('place'), valuePerPlayer, date]
                        writer.writerow(params)

def exportToDatabase(absPath: str):
    try:
        conn = mariadb.connect(**DB)
    except mariadb.Error as e:
        _log.error(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE runs")
        cursor.execute(
            f"""
            LOAD DATA {"LOCAL " if DEVEL else ""}INFILE '{absPath}'
            INTO TABLE runs
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '\"' 
            ESCAPED BY '\"'
            LINES TERMINATED BY '\n'
            (Leaderboard, Series, Game, Player, Platform, Place, Value, Date);
            """)

        cursor.execute("TRUNCATE TABLE playerRanks")
        cursor.execute(
            f"""
            INSERT INTO playerRanks (Rank, Player, Points)
            SELECT ROW_NUMBER() OVER (ORDER BY Points DESC) AS Rank, t1.Player, t1.Points
            FROM (
                SELECT Player, SUM(GREATEST(Value * POWER(0.99, (PlayerRank - 1)), Value * 0.25)) AS Points
                FROM (
                    SELECT Player, Value, ROW_NUMBER() OVER (PARTITION BY Player ORDER BY Value DESC) AS PlayerRank
                    FROM runs
                ) AS rankedRuns
                GROUP BY Player
                ORDER BY Points DESC
            ) AS t1;
            """
        )
        conn.commit()
    except Exception as e:
        _log.error(e)

def processRuns(jsonPath: str, csvPath: str, test: bool):
    groups = collectGroups(jsonPath, test)
    leaderboards = processGroups(groups)
    generateCSV(leaderboards, csvPath)
    if not test:
        absPath = os.path.join(os.getcwd(), csvPath)
        if DEVEL:
            absPath = absPath.replace("\\", "/")
        exportToDatabase(absPath)
