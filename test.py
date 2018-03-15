# -*- coding: utf-8 -*-
__author__ = 'wangerxiong'

import tweepy
import time
import MySQLdb

def get_friends1(api, user):
    friend_list = []
    fw_error = open('error.txt', 'a')
    try:
        t_user= api.friends_ids(id=user)
        for i in t_user:
            friend_list.append(api.get_user(i).screen_name)
    except Exception, e:
        fw_error.write(str(e))
    fw_error.close()
    write_users(user,friend_list)

def get_friends(api, user):
    # friend_id_list=[]
    fw_error = open('error.txt', 'a')
    try:
        friend_id_list= api.followers_ids(id=user)
    except Exception, e:
        fw_error.write(str(e))
    fw_error.close()
    return friend_id_list

def write_users(user,users_list):
    f = open('./'+user.strip()+'.txt', 'w')
    print(user)
    # print(users_list)
    for users in users_list:
        f.write(users+'\n')
    f.flush()
    f.close()
    print("write :"+str(len(users_list)))
def insertotable(api,users_list):
    print(len(users_list))
    conn = MySQLdb.connect(host='localhost', user='root',passwd='wex')
    # conn = MySQLdb.connect(host='localhost', user='datamesh',passwd='Hello2016')
    # conn = MySQLdb.connect(host='47.89.43.140', user='datamesh',passwd='Hello2016')
    cursor = conn.cursor()
    conn.select_db('twitter_undp')
    sql=""
    for user in users_list:

        fw_error1 = open('error.txt', 'a')
        statement=""
        insertstr = "INSERT INTO twitter_user_followers (name)VALUES "
        #insertstr = "INSERT INTO twitter_user_gzing(name,has_timeline_crawled,last_crawl_time,crawl_times,latest_tweets_time)VALUES "
        users=api.get_user(user)
        print(users.screen_name)

        try:
               # statement=statement+"('"+str(users.screen_name).strip('\n')+"'),"
               statement=statement+"('"+str(users.screen_name)+"','"+str(users.created_at)+"','"+str(users.followers_count).encode('utf-8')+"','"+str(users.friends_count).encode('utf-8')+"','"+str(users.favourites_count).encode('utf-8')+"','"+str(users.statuses_count).encode('utf-8')+"','"+str(users.id).encode('utf-8')+"','"+str(users.lang).encode('utf-8')+"','"+str(users.location).encode('utf-8')+"','"+str(users.listed_count).encode('utf-8')+"'),"

               print(statement)
        except Exception, f:
            fw_error1.write(str(f))
        fw_error1.close()
        #statement=statement+"('"+str(line).strip('\n')+"','"+str(0)+"','"+str(0)+"','"+str(0)+"','"+str(0)+"'),"
        if statement[:-1]!="":
            sql=insertstr+statement[:-1]+";"
            print(sql)
            try:
                cursor.execute(sql)
                time.sleep(0.08)
                conn.commit()
            except:
                conn.rollback()
    conn.close()

def main():

    consumer_key = '3nVuSoBZnx6U4vzUxf5w'

    consumer_secret = 'Bcs59EFbbsdF6Sl9Ng71smgStWEGwXXKSjYvPVt7qys'

    access_token = '748039770564071424-3YxgsSAkyZdxDtGC6ecWf77b8pLnSpd'

    access_token_secret = 'rd8dkfgOySKx9VFj198MgQC0LzxRBYuTpii5WSlrHG84m'
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # api = tweepy.API(auth,proxy="http://proxy:proxy123456@10.27.44.211:8866")
    api = tweepy.API(auth,proxy="http://proxy:BTMM@bjbd.o2o.ac:58894")
    fr = open('get_friends.txt')
    for name in fr.readlines():
        friendis_list=get_friends(api, name)
        print(friendis_list)
        insertotable(api,friendis_list)
        time.sleep(1)
    print("Finished")
    #04,07,10,17,18,28 | 03,06
if __name__ == '__main__':
    main()

