#!venv/bin/python

import config
import psycopg2
import time
import datetime
import urllib2

from download_ssr import get_ssr_data
from load_schema_tables import load_schema_data
from ssr_2_postgis import ssr_2_postgis


def ensure_updated_dates_table_exists():
    connection = psycopg2.connect("dbname=" + config.database["dbname"] + " user=" + config.database["user"])
    cur = connection.cursor()
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('updated_dates',))
    exists = cur.fetchone()[0]

    if not exists:
        cur.execute("CREATE TABLE updated_dates (id serial PRIMARY KEY, date timestamp with time zone);")
        connection.commit()

    cur.close()
    connection.close()

def get_updated():

    ensure_updated_dates_table_exists()

    #query the db for the latest update
    connection = psycopg2.connect("dbname=" + config.database["dbname"] + " user=" + config.database["user"])
    cur = connection.cursor()
    cur.execute("SELECT date FROM updated_dates ORDER BY id LIMIT 1;")
    db_time =  cur.fetchone()

    cur.close()
    connection.close()

    if db_time:
        db_time = db_time[0]


    #get the latest updated string from web
    url = urllib2.urlopen(config.base_url + "sist_oppdatert.txt")
    latest = url.read().rstrip()
    web_time =  time.strptime(latest, '%a %b %d %H:%M:%S %Z %Y')
    web_time = datetime.datetime(*web_time[:6])


    if not db_time:
        return False, web_time
    else:
        return web_time >= db_time.replace(tzinfo=None), web_time

def update_newest(web_time):
    connection = psycopg2.connect("dbname=" + config.database["dbname"] + " user=" + config.database["user"])
    cur = connection.cursor()

    cur.execute("INSERT INTO updated_dates (date) VALUES (%s)", (web_time,))
    cur.close()
    connection.commit()
    connection.close()

is_updated, web_time= get_updated()

#go ahead if newer version found
if not is_updated:
    print "Server version is newer, updating"

    print "Downloading data.."
    json_file = get_ssr_data()

    print "Data downloaded"

    print "Creating supportive data"
    ssr_table = load_schema_data()

    num_features, elapsed_time = ssr_2_postgis(json_file, ssr_table)

    print "Finished writing %i features in %f seconds" % (num_features, elapsed_time)

    update_newest(web_time)


else:
    print "You have the latest version!"

print "Finished! Bye!"