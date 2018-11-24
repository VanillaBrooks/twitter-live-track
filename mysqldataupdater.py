import pymysql
import time
import bs4
import requests
import re
import json
import datetime
from multiprocessing import Pool


with open('config.json', 'r') as f:
	config = json.load(f)
	database = config['database connection']
with open(config['track user path'], 'r') as f:
	baseusers = [i.strip('\n') for i in f.readlines()]


def getuserids(user):
	conn = pymysql.connect(host=database['host'], port=database['port'], user=database['user'], password=database['password'], database=database['database'])
	cursor = conn.cursor()

	sql = 'SELECT id FROM users where username=%s'
	cursor.execute(sql, user)
	idtuple =cursor.fetchall()

	if idtuple:
		for item in idtuple:
			userid = item[0]
	else:
		newuser = 'INSERT INTO `users` (username) VALUES (%s)'
		cursor.execute(newuser, user)
		conn.commit()
		print('New user has been added to the database: %s' % (user))
		cursor.execute(sql,user)
		idtuple = cursor.fetchall()

		for item in idtuple:
			userid = item[0]
	conn.commit()
	conn.close()
	return userid

def gettwitterdata(user):
	headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.0) Gecko/20100101 Firefox/60.0'}
	res = requests.get("https://twitter.com/" + user, headers=headers)

	try:
		soup = bs4.BeautifulSoup(res.text, 'html.parser')
	except AttributeError as e:
		return False
		print(e)

	twitterdata = soup.select('span[data-count]')

	regexsearch = re.compile(r'\d*\d')
	alldata = []

	for i in range(0,4):
		try:
			f = regexsearch.search(str(twitterdata[i]))
			allata.append(int(f.group()))
		except Exception:
			try:
				f = regexsearch.search(str(twitterdata[i]))
				alldata.append(int(f.group()))
			except Exception as e:
				# print('data for %s was corrupted' % (user,))
				alldata.append(False)

	return alldata



def filldata(filetype, userid, number, currentdate, currenttime, weekday, inc_dec, initial):
	conn = pymysql.connect(host=database['host'], port=database['port'], user=database['user'], password=database['password'], database=database['database'])
	cursor = conn.cursor()

	sql = 'INSERT INTO `' + filetype + '` (`user_id`, `num`, `datecol`, `timecol`, `weekday`, `inc_dec`, `initial`) VALUES (%s,%s,%s,%s,%s,%s,%s)'
	cursor.execute(sql, (userid, number, currentdate, currenttime, weekday, inc_dec, initial))
	conn.commit()
	conn.close()

def continuousupdates(userandlikepair):
	user, old_data, initial_value= userandlikepair

	current_data = gettwitterdata(user)
	typeofdata = ['tweet', 'follow', 'follower', 'favorite']

	if not current_data:
		print('there was an error getting the data for %s , continuing on' % (user,))
		return [user, old_data]
	for i in range(4):
		if (current_data[i] != old_data[i] and current_data[i] != False) or  (initial_value==1 and current_data[i] != False):
			if typeofdata[i] != 'follower':
				print ('updating %s for %s from %s to %s (%s | %s) at %s' % (typeofdata[i],user,old_data[i], current_data[i], current_data[i]-old_data[i],initial_value, datetime.datetime.now().time()))

			# assumes multiple likes in the same minute happened at the same time

			eventime = datetime.datetime.now()
			while current_data[i] != old_data[i]:
				if current_data[i] > old_data[i]:
					old_data[i] += 1
					inc_dec = 1
				else:
					old_data[i] -= 1
					inc_dec = -1

				currentdate = time.strftime('%Y-%m-%d')
				currenttime = time.strftime('%H:%M:%S')
				userid = getuserids(user)
				# datatowrite = [int(userid), int(old_data[i]), currentdate, currenttime, int(eventime.weekday()), inc_dec]

				filldata(typeofdata[i], userid, old_data[i], currentdate, currenttime, eventime.weekday(), inc_dec, initial_value)
	return [user, old_data]  # older like has now been updated to the same value as current like in the while loop


def basedata(user):
	# print('in base data with user', user)
	corrupted = []

	# initial like data collection
	twitterdata = gettwitterdata(user)
	for piece in twitterdata:
		if piece == False:
			return user, False
	return user, twitterdata


def multiprocess(info, name):
	#print('the length of info is: ', len(info))
	with Pool(config['pool size']) as p:
		if name == 'base':
			# info is a list of the usernames that need like data
			activeusers, corruptedusers = dictcombine(p.map(basedata, info))
			return activeusers, corruptedusers

		elif name == 'cont':
			#print('in cont multi with dict: ', info)
			# info is a dictionary of name : likedata pairs
														# the zero notates to NOT write some initial values
			listoftuples = p.map(continuousupdates, zip(info.keys(), info.values(), len(info.keys())*[0]))
			updatedict = dictcombine(listoftuples)
			#print('exiting cont with dict: ', updatedict)

			return updatedict[0]
		elif name == 'initial':
														# the one notates to write some initial values
			p.map(continuousupdates, zip(info.keys(), info.values(), len(info.keys())*[1]))
			return False


def dictcombine(nestedlists):
	likedata = {}
	corrupted = []
	# combine many rows of 2 item lists into one dictionary
	for row in nestedlists:
		# check if the item in the dicitonary is false (needs to not be in dict)
		index = 0
		if row[1] == False:
			corrupted.append(row[0])
		else:
			likedata[row[0]] = row[1]
	return likedata, corrupted




if __name__ == '__main__':
	print('Number of users being tracked: %s' % (len(baseusers)))
	incomplete = True
	while incomplete:
		try:
			print('getting base information')
			activeuserdict, corruptedusers = multiprocess(baseusers, 'base')
			print('corrupted users: ', corruptedusers)
			incomplete = False
		except Exception as e:
			print('there was an exception in base: ')
			print(e)
			incomplete = True
	counter = 0
	# continuous data collection
	while (True):
		counter += 1
		try:
			if corruptedusers:
				newusers, corruptedusers = multiprocess(corruptedusers, 'base')
				print('corrupted: ', corruptedusers)
				activeuserdict.update(newusers)

			activeuserdict = multiprocess(activeuserdict, 'cont')
		except Exception as e:
			print('end exception',e)

		if counter < 3:
			time.sleep(60)
		elif counter == 3:
			multiprocess(activeuserdict, 'initial')
			print('\n\nwrote some initial values\n\n')
		else:
			time.sleep(config['update frequency'])
	input()
