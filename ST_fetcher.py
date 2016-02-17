#!/usr/bin/python

import pycurl
import json
import pymysql
import os

connection = pymysql.connect(user='seth',
                             password='cookies',
                             db='stocktwits')


# mysql_writer configured for specific data from Stocktwits
def mysql_writer():
    with connection.cursor() as cursor:

        # database relationships stored in Stock_Twits_DB_Model.mwb (open w/ MySQL Workbench)
        sql = 'CREATE TABLE IF NOT EXISTS messages (idMessages INT PRIMARY KEY, body VARCHAR(140), time_created DATETIME NOT NULL, User_idUser INT FOREIGN KEY, Source_idSource INT FOREIGN KEY, Symbols_idSymbols INT FOREIGN KEY)'
        cursor.execute(sql)
        sql = 'CREATE TABLE IF NOT EXISTS source (idSource INT PRIMARY KEY, Title VARCHAR(45), url VARCHAR(45))'
        cursor.execute(sql)
        sql = 'CREATE TABLE IF NOT EXISTS symbols (idSymbols INT PRIMARY KEY, symbol VARCHAR(45), title VARCHAR(45))'
        cursor.execute(sql)
        sql = 'CREATE TABLE IF NOT EXISTS users(idUser INT PRIMARY KEY, username VARCHAR(45), `name` VARCHAR(45), avatar_url VARCHAR(45), avatar_url_ssl VARCHAR(45), `identity` VARCHAR(45))'
        cursor.execute(sql)

    for message in data['messages']:  # TODO change vars to represent vars in JSON
        try:
            with connection.cursor() as cursor:
                sql = "INSERT INTO `messages` (`idMessages`, `body`, `time_created`, `user_idUser`, `Source_idSource`, `Symbols_idSymbols`) VALUES (%s, %s, %s, %s, %s)"

                cursor.execute(sql, (message['id'],
                                     message['body'],
                                     message['created_at'],
                                     message['user']['id'],
                                     message['source']['id'],
                                     message['symbols']['id']
                                     )
                               )
                for user in message['user']:
                    sql = "INSERT INTO `users`(`idUser`, `username`, `name`, `avatar_url`, `avatar_url_ssl`, `identity`) VALUES (%s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (user['id'],
                                         user['username'],
                                         user['name'],
                                         user['avatar_url'],
                                         user['avatar_url_ssl'],
                                         user['identity']
                                         )
                                   )
                for source in message['source']:
                    sql = "INSERT INTO `source`(`idSource`, `title`, `url`) VALUES (%s, %s, %s)"
                    cursor.execute(sql, (source['id'],
                                         source['title'],
                                         source['url']
                                         )
                                   )
                for symbol in message['symbols']:
                    sql = "INSERT INTO `symbols`(`idSymbols`, `symbol`, `title`) VALUES (%s, %s, %s)"
                    cursor.execute(sql, (symbol['id'],
                                         symbol['symbol'],
                                         symbol['title']
                                         )
                                   )

                connection.commit()
        finally:
            #           connection.close()
            pass


# write results from API call to out.json
with open('out.json', 'wb') as f:
    c = pycurl.Curl()
    # this API call returns the most recent tweets from a curated list of content
    # awaiting approval for more in-depth data streams
    c.setopt(c.URL, 'https://api.stocktwits.com/api/2/streams/suggested.json?access_token=1ad63859ca761495a7ea38c4cdc128369ad331f5')
    c.setopt(c.WRITEDATA, f)
    c.perform()
    c.close()

# read out.json into data
with open('out.json') as data_file:
    data = json.load(data_file)
    mysql_writer()

# remove out.json when complete
os.remove('out.json')
