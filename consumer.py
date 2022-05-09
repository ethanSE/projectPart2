#!/usr/bin/env python3
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
from confluent_kafka import Consumer
import json
import ccloud_lib
from datetime import datetime, timezone, timedelta
import time
import json
import psycopg2
import argparse
import re
import csv
import pandas as pd
import psycopg2.extras as extras
import pytz
from dateutil import parser

DBname = "postgres"
DBuser = "postgres"
DBpwd = "ethan"
TripTableName = 'Trip'
BreadCrumbTableName = 'BreadCrumb'

def process(messages):
	def createDataFrames(data):
		#load into dataframe
		df = pd.DataFrame(data)

		trips = df.copy()
		trips.drop(["latitude", "longitude", "direction", "tstamp",  "speed"], axis = 1, inplace = True)
		#drop duplicates
		trips.drop_duplicates(inplace=True)
		#change trips data types
		trips = trips.astype({'vehicle_id': 'int32', 'trip_id': 'int32'})

		#drop info that should not be in breadcrumb table
		breadCrumbs = df.drop("vehicle_id", axis = 1)
		#default direction for no direction
		breadCrumbs['direction'] = breadCrumbs['direction'].apply(lambda x: x if x != "" else "-1")	
		#default speed for no speed
		breadCrumbs['speed'] = breadCrumbs['speed'].apply(lambda x: x if x != "" else "-1")
		#lat, long to 32 bit float, default values of "0.0"
		breadCrumbs['latitude'] = breadCrumbs['latitude'].apply(lambda x: x if x != "" else "0.0")
		breadCrumbs['longitude'] = breadCrumbs['longitude'].apply(lambda x: x if x != "" else "0.0")

		#change breadcrumb data types
		breadCrumbs = breadCrumbs.astype({'trip_id': 'int32', 'longitude': 'float32', 'latitude': 'float32', 'speed': 'int32', 'direction': 'int32'})
	
		return(breadCrumbs, trips)

	def storeDFtoTable(df, conn, table):
		#could not have done this without this resource:
		#https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/
		# I wanted to do my validation in Pandas so I looked up how to go from pandas to postgresql
		# Create a list of tupples from the dataframe values
		tuples = list(df.itertuples(index=False))
		# Comma-separated dataframe columns
		cols = ','.join(list(df.columns))
		# SQL query to execute
		query  = "INSERT INTO %s(%s) VALUES %%s" % (table,cols)
		cursor = conn.cursor()
		try:
			extras.execute_values(cursor, query, tuples)
			conn.commit()
		except (Exception, psycopg2.DatabaseError) as error:
			print("Error: %s" % error)
			conn.rollback()
			cursor.close()
			return 1
		cursor.close()

	def messageTransformer(messages):
		def dateConverter(time,date):
			months = {
				"JAN": 1,
				"FEB": 2,
				"MAR": 3,
				"APR": 4,
				"MAY": 5,
				"JUN": 6,
				"JUL": 7,
				"AUG": 8,
				"SEP": 9,
				"OCT": 10,
				"NOV": 11,
				"DEC": 12
			}
			dateParts = date.split("-")
			dateParts.reverse()

			year = int(f"20{dateParts[0]}")
			month = int(months[dateParts[1]])
			day = int(dateParts[2])
			delta = timedelta(seconds=int(time))

			western = pytz.timezone('US/Pacific')
			localized_date = western.localize(datetime(year, month, day, 0, 0, 0))
			time = localized_date + delta
			fmt = '%Y-%m-%d %H:%M:%S'	
			return(time.strftime(fmt))

		rowlist = []

		#get into format of the data we are using
		for message in messages:
			item = {
				"tstamp": 		dateConverter(message["ACT_TIME"], message["OPD_DATE"]),
				"latitude": 	message["GPS_LATITUDE"],
				"longitude":	message["GPS_LONGITUDE"],
				"direction":	message["DIRECTION"],
				"speed":		message["VELOCITY"],
				"trip_id":		message["EVENT_NO_TRIP"],
				"vehicle_id":	message["VEHICLE_ID"]
			}
			rowlist.append(item)
		return rowlist
 
	def dbconnect():
		# connect to the database
		connection = psycopg2.connect(
			host="localhost",
			database=DBname,
			user=DBuser,
			password=DBpwd,
		)
		connection.autocommit = True
		return connection

	def validateTransform(breadCrumbs, trips):
		def assertTimeExists(time):
			date = parser.parse(time)
		breadCrumbs["tstamp"].apply(assertTimeExists)

		def validateDirection(dir):
			assert (-1 <= dir <= 359)
		breadCrumbs["direction"].apply(validateDirection)

		def breadCrumbForEachTrip():
			def existsBreadCrumb(trip_id):
				assert (breadCrumbs['trip_id'] == trip_id).any()
			trips['trip_id'].apply(existsBreadCrumb)
		breadCrumbForEachTrip()

		def tripForEachBreadCrumb():
			def existsTrip(trip_id):
				assert (trips['trip_id'] == trip_id).any()
			breadCrumbs['trip_id'].drop_duplicates().apply(existsTrip)
		tripForEachBreadCrumb()

		def averageSpeedCommuteTimes():
			# this assertion is probably not valid
			# defining commute times here as 7-8 am and 4:30-5:30 pm
			commuteCount = 0
			commuteTotalSpeed = 0
			nonCommuteCount = 0
			nonCommuteTotalSpeed = 0
			morningStartUTC = datetime.time(7)
			morningEndUTC = datetime.time(8)
			eveningStartUTC = datetime.time(16,30)
			eveningEndUTC = datetime.time(17,30)
				
			for tuple in breadCrumbs.itertuples():
				currentTime = parser.parse(tuple[6]).time()
				if morningStartUTC < currentTime < morningEndUTC or eveningStartUTC < currentTime < eveningEndUTC:
					commuteCount += 1
					commuteTotalSpeed += tuple[4]
				else:
					nonCommuteCount += 1
					nonCommuteTotalSpeed += tuple[4]
			
			commuteAvg = commuteTotalSpeed / commuteCount
			nonCommuteAvg = nonCommuteTotalSpeed / nonCommuteCount
			try:
				assert nonCommuteAvg > commuteAvg
			except:
				print("commute time assertion failed")

	conn = dbconnect()
	#read in data
	data = messageTransformer(messages)
	#create dataframes
	(breadCrumbs, trips) = createDataFrames(data)
	#perform validations
	validateTransform(breadCrumbs, trips)

	#store in database
	storeDFtoTable(breadCrumbs, conn, BreadCrumbTableName)
	storeDFtoTable(trips, conn, TripTableName)

def createTable(conn):
	with conn.cursor() as cursor:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {BreadCrumbTableName};
			CREATE TABLE {BreadCrumbTableName} (
				tstamp              TIMESTAMP,
				latitude            REAL,
				longitude           REAL,
				direction           INTEGER,
				speed              	REAL,
				trip_id             INTEGER
			);
		""")
		print(f"Created {BreadCrumbTableName}")
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TripTableName};
			DROP TYPE IF EXISTS DAY_TYPE;
			CREATE TYPE DAY_TYPE AS ENUM ('Weekday', 'Saturday', 'Sunday');
			CREATE TABLE {TripTableName} (
				trip_id             INTEGER,
				route_id            INTEGER,
				vehicle_id          INTEGER,
				service_key         DAY_TYPE,
				direction           INTEGER
			);	
			ALTER TABLE {TripTableName} ADD PRIMARY KEY (trip_id);
		""")
		print(f"Created {TripTableName}")


if __name__ == '__main__':

	# Read arguments and configurations and initialize
	#args = ccloud_lib.parse_args()
	#config_file = args.config_file
	config_file =  "/home/esam2/.confluent/librdkafka.config"
	topic = "ctran"
	conf = ccloud_lib.read_ccloud_config(config_file)

	# Create Consumer instance
	# 'auto.offset.reset=earliest' to start reading from the beginning of the
	#   topic if no committed offsets exist
	consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
	consumer_conf['group.id'] = 'python_example_group_1'
	consumer_conf['auto.offset.reset'] = 'earliest'
	consumer = Consumer(consumer_conf)

	# Subscribe to topic
	consumer.subscribe([topic])

	# Process messages
	try:
		messages = []
		while True:
			msg = consumer.poll(1.0)
			if msg is None:
				print("Waiting for message or event/error in poll()")
				continue
			elif msg.error():
				print('error: {}'.format(msg.error()))
			else:
				# Check for Kafka message			
				if msg.key().decode("utf-8") != "EOT":
					#add to current list
					messages.append(json.loads(msg.value()))
				else:
					#process current list
					process(messages)
					#empty current list
					messages = []
	except KeyboardInterrupt:
		pass
	finally:
		# Leave group and commit final offsets
		consumer.close()
