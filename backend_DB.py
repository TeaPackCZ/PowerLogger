import sqlite3
from datetime import datetime
import time
from math import floor
import numpy as np

import zmq

class myDB():
	def __init__(self, db_name):
		self.db_connection = sqlite3.connect(db_name)
		self.list_of_tables = ["TOTAL", "ACTUAL"]

	def init_table(self):
		message ='CREATE TABLE TOTAL ('
		message += 'UTC_TIME	INTEGER,'
		message += ' U_IN	INTEGER,'
		message += ' V_IN	INTEGER,'
		message += ' W_IN	INTEGER,'
		message += ' U_OUT	INTEGER,'
		message += ' V_OUT	INTEGER,'
		message += ' W_OUT	INTEGER,'
		message += ' ATLAS	INTEGER,'
		message += ' BUPI	INTEGER,'
		message += ' RENDER	INTEGER);'
		try:
			self.db_connection.execute(message)
		except:
			print("Table already exists")

		message ='CREATE TABLE ACTUAL ('
		message += 'UTC_TIME INTEGER,'
		message += ' U_IN FLOAT,'
		message += ' V_IN FLOAT,'
		message += ' W_IN FLOAT,'
		message += ' U_OUT FLOAT,'
		message += ' V_OUT FLOAT,'
		message += ' W_OUT FLOAT,'
		message += ' ATLAS FLOAT,'
		message += ' BUPI FLOAT,'
		message += ' RENDER FLOAT);'
		try:
			self.db_connection.execute(message)
		except:
			print("Table already exists")
		self.db_connection.commit()

	def insert_line(self, table, array, timestamp):

		if(table == "TOTAL"):
			values = ""
			for item in array:
				values += ", " + str(round(float(item)))
		elif(table == "ACTUAL"):
			values = ""
			for item in array:
				values += ", " + str(item)
		else:
			values = ",-1,-1,-1,-1,-1,-1,-1,-1,-1"

		message = "INSERT INTO " + table + " VALUES ("
		message += str(timestamp)
		message += values + ")"

		#print(message)

		self.db_connection.execute(message)
		self.db_connection.commit()

	def read_time_line(self, table, date_from, date_to, key_array):

		timestamp_from = myDB.generate_timestamp(date_from)
		timestamp_to = myDB.generate_timestamp(date_to)

		select = "SELECT "
		for item in key_array:
			select += str(item) + ","
		select = select[0:-1]
		select += " FROM " + table + " WHERE "
		select += "UTC_TIME > " + timestamp_from
		select += " AND UTC_TIME < " + timestamp_to

		print(select)
		data = self.db_connection.execute(select)
		return data

	def read_by_values(self, table, date_from, date_to, key, value_lo, value_hi):

		timestamp_from = myDB.generate_timestamp(date_from)
		timestamp_to = myDB.generate_timestamp(date_to)

		select = "SELECT UTC_TIME,"
		select += key
		select += " FROM " + table + " WHERE "
		select += "UTC_TIME > " + timestamp_from
		select += " AND UTC_TIME < " + timestamp_to
		select += " AND " + key + " > " + str(value_lo)
		select += " AND " + key + " < " + str(value_hi)

		#print(select)
		data = self.db_connection.execute(select)
		return data

	def generate_timestamp(time_str):
		try:
			time_strip = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
		except:
			try:
				time_strip = time.strptime(time_str, "%Y_%m_%d %H:%M:%S")
			except:
				print("Bad format of given time")
				time_strip = -1
		return str(int(time.mktime(time_strip)))

	def close(self):
		if(self.db_connection):
			self.db_connection.close()


def init_zmq(IP, ports_in, port_out):
	context = zmq.Context()
	subs = context.socket(zmq.SUB)
	subs.bind("tcp://" + str(IP) + ":" + str(ports_in[0]))
	subs.bind("tcp://" + str(IP) + ":" + str(ports_in[1]))
	subs.setsockopt(zmq.SUBSCRIBE, b"")

	publ = context.socket(zmq.PUB)
	publ.bind("tcp://" + str(IP) + ":" + str(port_out))

	return subs, publ

def runtime(db, socket_in, socket_out):
	while True:
		message = socket_in.recv_string()
		#print(message)
		type,data = message.split("&")
		if type == "INSERT_A":
			timestamp,actuals =  data.split(";")
			db.insert_line("ACTUAL", actuals.split(","),timestamp)
		elif type == "INSERT_T":
			timestamp,totals =  data.split(";")
			db.insert_line("TOTAL",totals.split(","),timestamp)
		elif(type == "READ_TIME"):
			table,date_from,date_to,keys = data.split(";")
			data = db.read_time_line(table, date_from, date_to, keys.split(","))
			message = ""
			for item in data:
				message += str(item) + ";"
			socket_out.send_string(message[0:-1])
		elif(type == "READ_VALUE"):
			table,date_from,date_to,key,key_val_lo,key_val_hi = data.split(";")
			data = db.read_by_values(table,date_from,date_to,key,key_val_lo,key_val_hi)
			message = ""
			for item in data:
				message += str(item) + ";"
			socket_out.send_string(message[0:-1])
		elif(type == "RAW"):
			try:
				reply = db.db_connection.execute(str(data))
				message = ""
				for item in reply:
					message += str(item)
				socket_out.send_string(message)
			except:
				socket_out.send_string("Bad format, or something got wrong: " + str(data))
		elif(type == "LOCAL"):
			try:
				reply = db.db_connection.execute(str(data))
				lines = []
				for item in reply:
					lines.append(np.array(item, dtype=np.double))
				matrix_rows = len(lines)
				matrix_columns = len(lines[0])
				matrix = np.ndarray((matrix_rows, matrix_columns), dtype= np.double)
				matrix = np.array(lines)
				np.save("export_matrix", matrix)
				socket_out.send_string("Local job done and saved into 'export_matrix'")
			except:
				socket_out.send_string("Bad format, or something got wrong: " + str(data))
		else:
			reply = "Unknown cmd: " + message
			socket_out.send_string(reply)

mdb = myDB("2025_01_01.db")
mdb.init_table()
subscriber,publisher = init_zmq("192.168.2.13", ["10000","10001"], "10010")

runtime(mdb, subscriber, publisher)

socket.close()
mdb.close()

