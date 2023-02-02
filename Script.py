TIMEZONEDB_KEY = 'Z5ZDUWG4ZBMU'
TIMEZONEDB_API_ROOT = 'http://api.timezonedb.com/v2.1/'
TZDB_API_GET_LIST = TIMEZONEDB_API_ROOT+'list-time-zone'
TZDB_API_GET_TIMEZONE = TIMEZONEDB_API_ROOT+'get-time-zone'
DB_CONNECTION_STRING = "postgres://zjhpqlmr:oSLJD_OEBSrn-bUS8_KSTaOANjoE0WqU@fanny.db.elephantsql.com/zjhpqlmr"

import requests
import psycopg2
import time

dbDict={}

conn = psycopg2.connect(DB_CONNECTION_STRING)
with conn:
  with conn.cursor() as curs:
    try:
      # Establish params and pull zone list
      params = {
        "key":TIMEZONEDB_KEY,
        "format":"json",
        "by":"zone",
      }
      timezoneList = requests.get(TZDB_API_GET_LIST,params)
      
      # Clear existing data
      curs.execute('TRUNCATE TABLE tzdb_timezones')
      conn.commit()
      # Copy list to db
      for zone in timezoneList.json()['zones']:
        curs.execute(f"INSERT INTO tzdb_timezones VALUES ('{zone['countryCode']}','{zone['countryName']}','{zone['zoneName']}',{zone['gmtOffset']})")
        conn.commit()

      # Build db dictionary
      curs.execute('SELECT * FROM tzdb_zone_details')
      for record in curs:
        dbDict.update({record[2]:{
          "countryCode":record[0],
          "countryName":record[1],
          "zoneName":record[2],
          "gmtOffset":record[3],
          "dst":record[4],
          "zoneStart":record[5],
          "zoneEnd":record[6]
        }})

      # Check dictionary against pulled data
      # curs.execute('SELECT * FROM tzdb_timezones')
      for zone in timezoneList.json()['zones']:
        print(f"Checking {zone['zoneName']}, {zone['countryName']}")
        # If not there, add to db
        if zone['zoneName'] not in dbDict:
          time.sleep(2)
          print("Doesn't exist, pulling data and adding to db...")
          params.update({"zone":zone['zoneName']})
          timezoneDetails = requests.get(TZDB_API_GET_TIMEZONE,params)
          timezoneDetails = timezoneDetails.json()

          curs.execute(f"INSERT INTO tzdb_zone_details (countrycode, countryname, zonename, gmtoffset, dst, zonestart, zoneend) VALUES ('{timezoneDetails['countryCode']}','{timezoneDetails['countryName']}','{timezoneDetails['zoneName']}',{timezoneDetails['gmtOffset']},{timezoneDetails['dst']},{timezoneDetails['zoneStart']},{'null' if timezoneDetails['zoneEnd']==None else timezoneDetails['zoneEnd']})")
          conn.commit()
          print('Done')

    except Exception as err:
      print('Error', err)
      message = f'{err=}'
      message = message.replace("'","")
      curs.execute(f"insert into tzdb_error_log (error_message) values ('{message}')")
      conn.commit()