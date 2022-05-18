# Mikhail Nikitin Edits
# 5/18/2022
# Major Changes to be reverted for future integration:
    # Everything involving SQL
    # Reading from csv file
    # Everything line 77 to line 86
    # End code with AWS IOT things is commented out
    # Added timedelta in imports


# I don't have these imports:

#from awscrt import io, mqtt, auth, http
#from awsiot import mqtt_connection_builder
#import mysql.connector 


import time as t
import datetime as dt
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from datetime import timezone
import time

#MYSQL Credentials

#mydb = mysql.connector.connect(
#  host="10.16.26.175",
#  user="smartcity",
#  password="12TeCSAR_45",
#  database="SmartCity"
#)
#sleep
#print("hello!")

#mycursor = mydb.cursor()

fromtime = "2022-04-07 00:00:00"
totime = "2022-04-10 01:08:00"
#mycursor.execute(f"SELECT global_ID, camera_ID, record_time FROM t_Features WHERE record_time BETWEEN '{fromtime}' AND '{totime}';")

#table_rows = mycursor.fetchall()
#for x in mycursor:
  #print(x)
#----DO NOT EDIT-----#
#Initializations for results variables
#These are the variables you will update
id = "default"
## cameraname_timestamp(the time created)
elapsedTime = 0.0
## check for the output
initialTime = 0.0
createdTime = 0.0
description = "default"
cumulativePeople = 0
## to string
currentPeople = 0
## to string
### change all times to utc
#--------------------------#

#######DO YOUR SQL QUERIES HERE######
#Save the correct data to the Results Variables#


#ID = df.global_ID
#print(ID)
#df.eachmin = df.groupby('record_time')['global_ID'].nunique()

#Python didn't like line 78

#currentPeople = str(df.eachmin.iloc[-1])


#df.record_time = datetime.fromtimestamp(1571595618.0, tz=timezone.utc)
#   ******************** I'm not sure what this does or what it's for

#df['record_time']=pd.to_datetime(df['record_time'], dayfirst=True)
#id = "camera1_" , int(time.time()) 
# Assuming that this is more for real time application

#datetime.utcnow()
#print('camera_ID:', df.camera_ID.unique())












# Read Dataset
df = pd.read_csv (r'C:\Users\Mike\Downloads\Micheal.csv')

# Changes time index to proper format
df['record_time'] = pd.to_datetime(df['record_time'])
df.columns =['number', 'global_ID', 'camera_ID', 'record_time']

# CHANGE THIS VARIABLE TO CHANGE INTERVAL MEASURED IN SECONDS
MasterInterval = 5

# NEW
# Rounds to LOWEST threshold in seconds, in this case = (0:00, 0:05, 0:10, etc)
def Second_Rounder(ts):
    ts = ts - timedelta(seconds=ts.second % MasterInterval)
    return ts

""""
Testing Code for 5 second rounder
now = datetime.now().replace(microsecond=0)
print(now)
print(Second_Rounder(now))
"""

"""
Mikhail Nikitin Start Loop Code here
* Also made 5 second rounder function above
"""


# NOTE: This would be easier by just using my parsing code from before obviously, but
# I'm trying to simulate as if it is not in a set loop but running continuously every 5 seconds in a loop



# Making array of features to put into a csv later just for deliverable purposes
# Will append to this as the loop runs to "simulate" real time appending to a dataset
deliverables = ['createdTime', 'duration', 'startTime', 'endTime', 'currentPeople', 'totalPeople']
# createdTime is used as index
Output = pd.DataFrame(columns = deliverables, index = ['id'])

#Sets up output file
Output.to_csv('C:/Users/Mike/Downloads/Updater.csv')

print("Starting Loop!")

# Need a starting point for this application since we have a set dataset (This would change)
position = Second_Rounder(df.record_time.iloc[0])
# Position is also the "Current time" in the simulated "real time" as we go through the dataset

# start with 0
totalPeople = 0


# Loop actual start
while position < df.record_time.iloc[-1]:
    startTime = position
    endTime = position + timedelta(seconds=MasterInterval)
    idT = startTime
    
    elapsedTime = endTime - startTime
    
    #Actual current time
    createdTime = datetime.now().replace(microsecond=0)
    
    # Just finds the unique amount of people in the time interval, left inclusive
    currentPeople = df[(df.record_time >= startTime) & (df.record_time < endTime)].global_ID.nunique()
    
    # If you want current *unique people, need more code. Just need to check against previous global IDs here
    totalPeople += currentPeople
    
    data = pd.DataFrame([createdTime, elapsedTime, startTime, endTime, currentPeople, totalPeople]).T
    data.index = [idT]
    
    # Writing to output file, Appends in real time
    data.to_csv('C:/Users/Mike/Downloads/Updater.csv', mode = 'a', index=True, header = False)
    
    # Extra Code to see the code is going through the loop and updating
    # Prints every hour
    if (position.minute % 60 == 0) & (position.second == 0):
        print('Time: ', startTime)
        print('Current People', currentPeople, 'Total People', totalPeople)
    
    # Go 5 seconds further
    position += timedelta(seconds=MasterInterval)














"""
Mikhail Nikitin End Loop Code here
"""



description = "It works"

'''
whatever sql query 
id = camera1_timstamp
elapsedTime = 123445.2
.
.
.
currentPeople = 2

!!!!!!!!!!!!MAKE SURE YOU EDIT ALL THE RESULTS VARIABLES!!!!!!!!

'''
"""
#--------DO NOT EDIT--------#
results = {
  "id" : id,
    "__typename": "CameraData",
  "elapsedTime" : elapsedTime,
  "initialTime" : initialTime,
  "createdTime" : createdTime,
  "description" : description,
  "cumulativePeople" : cumulativePeople,
  "currentPeople": currentPeople,
    "updatedAt" : dt.datetime.now(),
    "createdAt": dt.datetime.now()
}

json_results = json.dumps(results, default = str)
#----------------------------#


#Define ENDPOINT, CLIENT_ID, PATH_TO_CERTIFICATE, PATH_TO_PRIVATE_KEY, PATH_TO_AMAZON_ROOT_CA_1, MESSAGE, TOPIC, and RANGE
'''
ENDPOINT = "a2du3i0gdkpf53-ats.iot.us-east-1.amazonaws.com"
CLIENT_ID = "testDevice"
'''
ENDPOINT = "a2du3i0gdkpf53-ats.iot.us-east-1.amazonaws.com"
CLIENT_ID = "testDevice"
#CERTIFICATES ARE ACTIVATED WITH THE IOT THING CALLED TEST_THING -- WILL NEED TO UPDATE IF USING A NEW IOT THING 
PATH_TO_CERTIFICATE = "/home/babak/53a33f1cb8-certificate.pem.crt"
PATH_TO_PRIVATE_KEY = "/home/babak/53a33f1cb8-private.pem.key"
PATH_TO_AMAZON_ROOT_CA_1 = "/home/babak/AmazonRootCA1.pem"

'''
PATH_TO_CERTIFICATE = "/home/babak/9631dc35aa43d8abb56e612f937f48ab5facbef75ed5d704a231fcd8799b1288-certificate.pem.crt"
PATH_TO_PRIVATE_KEY = "/home/babak/53a33f1cb8-private.pem.key"
PATH_TO_AMAZON_ROOT_CA_1 = "/home/babak/AmazonRootCA1.pem"
'''
#THE TOPIC TO BE SENT TO -- CAN CHANGE DEPENDING ON DATA
TOPIC = "test"


#------- SPIN UP MQTT RESOURCES --- DO NOT EDIT ---------#
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERTIFICATE,
            pri_key_filepath=PATH_TO_PRIVATE_KEY,
            client_bootstrap=client_bootstrap,
            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
            client_id=CLIENT_ID,
            clean_session=False,
            keep_alive_secs=6
            )
print("Connecting to {} with client ID '{}'...".format(
        ENDPOINT, CLIENT_ID))
# Make the connect() call
connect_future = mqtt_connection.connect()
# Future.result() waits until a result is available
connect_future.result()
print("Connected!")
#---------------------------------------------------------#

#Call the publish method
mqtt_connection.publish(topic=TOPIC, payload=json_results, qos=mqtt.QoS.AT_LEAST_ONCE)

print("Published: '" + json_results + "' to the topic: " + TOPIC)


#DISCONNECTING FROM AWS IOT CORE
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()
"""