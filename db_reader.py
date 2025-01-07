import time
import zmq
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from math import floor

class db_comunication():
	def __init__(self):
		self.context = zmq.Context()
		self.subs = self.context.socket(zmq.SUB)
		self.subs.connect("tcp://192.168.2.13:10010")
		self.subs.setsockopt(zmq.SUBSCRIBE, b"")

		self.pubs = self.context.socket(zmq.PUB)
		self.pubs.connect("tcp://192.168.2.13:10001")

		time.sleep(5)

	def ask_db(self, msg):
		self.pubs.send_string(msg)
		reply = self.subs.recv_string()
		return reply

	def deinit(self):
		self.subs.close()
		self.pubs.close()
		self.context.term()

def to_matrix(msg):
	msg = msg.replace(")","").replace("(","").replace(" ","")
	lines = msg.split(";")
	matrix_rows = len(lines)
	matrix_columns = len(lines[0].split(","))
	matrix = np.ndarray((matrix_rows, matrix_columns), dtype= np.double)
	for i in range(matrix_rows):
		matrix[i,:] = np.array(lines[i].split(","), dtype= np.double )
	#matrix = np.asmatrix(msg, dtype = "i4,f4")
	return matrix

def create_plot(data, title="", legend=None, axis_x="", axis_y="", save=False, show=True):
	fig, ax = plt.subplots()

	timedate = []
	for timestamp in data[:,0]:
		timedate.append(datetime.utcfromtimestamp(timestamp))

	#print(np.shape(timedate), np.shape(data[:,0]), np.shape(data[:,1]))
	data_shape = np.shape(data)
	if(data_shape[1] == 2):
		ax.plot(timedate, data[:,1])
	else:
		for index in range(1,data_shape[1]):
			ax.scatter(timedate, data[:,index])


	ax.set(xlim=(timedate[0], timedate[-1]))
	#ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
	ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
	ax.xaxis.set_major_formatter(mdates.DateFormatter("%D %H:%M"))
	
	#ax.xaxis.set_minor_locator(mdates.MinuteLocator(interval=15))
	ax.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
	
	ax.set_title(title)
	ax.set_xlabel(axis_x)
	ax.set_ylabel(axis_y)
	ax.legend(legend)
	if(save):
		plt.savefig(str(title) + ".png")
	if(show):
		plt.show()


def get_daily_consuption(mydb, show_plot=False, month="01", day_from="15", day_to="31"):
	
	core_message = ["READ_TIME&TOTAL;2023-"," 01:00:00;2023-"," 23:00:00;UTC_TIME,BUPI"]
	for day in range(int(day_from), int(day_to)+1):
		message = core_message[0] + str(month) +"-"+ str(day) + core_message[1] +str(month) +"-"+ str(day) + core_message[2]
		data = mydb.ask_db(message)
		numbers = to_matrix(data)
		print("2023-"+ str(month) +"-"+ str(day) + " : " + str(max(numbers[:,1]) - min(numbers[:,1])) + " kWh")
	
	if(show_plot):
		core_message = ["READ_TIME&ACTUAL;2023-"," 01:00:00;2023-"," 23:00:00;UTC_TIME,BUPI"]
		for day in range(int(day_from), int(day_to)+1):
			message = core_message[0] + str(month) +"-"+ str(day) + core_message[1] + str(month) +"-"+ str(day) + core_message[2]
			data = mydb.ask_db(message)
			numbers = to_matrix(data)
			create_plot(numbers, "2023-" + str(month) +"-"+ str(day), ["BUPI"], "UTC Time", "P (kW)")


db_com = db_comunication()

get_daily_consuption(db_com, True, "02", "02", "20")

db_com.deinit()

