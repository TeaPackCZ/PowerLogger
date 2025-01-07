from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from time import sleep
from datetime import datetime
from math import floor

import zmq

def initialize_browser():
	  #object of Options class
	c = Options()
	c.add_argument("--headless")

	Browser = webdriver.Chrome(executable_path="/usr/lib/chromium-browser/chromedriver", options=c)
	Browser.implicitly_wait(5)
	Browser.get("http://192.168.2.12/login.htm")
	return Browser

def initialize_arrays():
	T = []
	A = []

	for i in range(9):
		T.append(0)
		A.append(0)

	return T,A

def refresh_total_data(Browser, Total_consuption):

	Total_consuption[0] = Browser.find_element(By.ID, "i1p7").text # U in
	Total_consuption[1] = Browser.find_element(By.ID, "i1p5").text # V in
	Total_consuption[2] = Browser.find_element(By.ID, "i1p3").text # W in
	Total_consuption[3] = Browser.find_element(By.ID, "i1p6").text # U out
	Total_consuption[4] = Browser.find_element(By.ID, "i1p4").text # V out
	Total_consuption[5] = Browser.find_element(By.ID, "i1p2").text # W out
	Total_consuption[6] = Browser.find_element(By.ID, "i1p1").text # ATLAS
	Total_consuption[7] = Browser.find_element(By.ID, "i1p0").text # BUPI
	Total_consuption[8] = Browser.find_element(By.ID, "i1p8").text # RENDER

	return Total_consuption

def refresh_actual_data(Browser, Actual_consuption):

	Actual_consuption[0] = Browser.find_element(By.ID, "i3p7").text # U in
	Actual_consuption[1] = Browser.find_element(By.ID, "i3p5").text # V in
	Actual_consuption[2] = Browser.find_element(By.ID, "i3p3").text # W in
	Actual_consuption[3] = Browser.find_element(By.ID, "i3p6").text # U out
	Actual_consuption[4] = Browser.find_element(By.ID, "i3p4").text # V out
	Actual_consuption[5] = Browser.find_element(By.ID, "i3p2").text # W out
	Actual_consuption[6] = Browser.find_element(By.ID, "i3p1").text # ATLAS
	Actual_consuption[7] = Browser.find_element(By.ID, "i3p0").text # BUPI
	Actual_consuption[8] = Browser.find_element(By.ID, "i3p8").text # Render

	return Actual_consuption

def sanitaze_inputs(array):
	for i in range(len(array)):
		if(array[i].find("á") > 0):
			array[i] = 0
		else:
			array[i] = float(array[i].split(" ")[0])
	return array

def check_n_login(Browser):
	try:
		login = Browser.find_element(By.NAME, "L")
	except:
		print("Already logged in")
		return Browser

	passwd = Browser.find_element(By.NAME, "passw")
	passwd.send_keys("test")

	login.click()
	Browser.get("http://192.168.2.12/s0.htm")
	print("Logged in")
	return Browser

def get_time_stamp():
	time_utc = datetime.utcnow().timestamp()
	return floor(time_utc)

def init_zmq():
	myZMQ = zmq.Context()
	zmq_socket = myZMQ.socket(zmq.PUB)

	return zmq_socket

def connect_zmq(socket, dest_ip, dest_port):
	socket.connect("tcp://" + str(dest_ip) + ":" + str(dest_port))
	return socket

def send_update_actual(socket,timestamp, actuals):
	message = "INSERT_A&" + str(timestamp) + ";"
	for item in actuals:
		message += str(item) + ","
	message = message[0:-1]
	#print(message)
	socket.send_string(message)

def send_update_total(socket,timestamp, totals):
	message = "INSERT_T&" + str(timestamp) + ";"
	for item in totals:
		message += str(item) + ","
	message = message[0:-1]
	#print(message)
	socket.send_string(message)

def get_reply(socket):
	message = socket.recv_string()
	print("received: %s" % message)

B = initialize_browser()
T,A = initialize_arrays()

B = check_n_login(B)

mZMQ = init_zmq()
mZMQ = connect_zmq(mZMQ, "192.168.2.13", "10000")
counter = 1

while True:
	ts = get_time_stamp()
	A = refresh_actual_data(B,A)
	A = sanitaze_inputs(A)

	send_update_actual(mZMQ, ts, A)

	if(counter == 1):
		T = refresh_total_data(B,T)
		T = sanitaze_inputs(T)
		send_update_total(mZMQ,ts,T)

	sleep(4)
	counter = (counter+1) % 60

B.close()
mZMQ.close()
