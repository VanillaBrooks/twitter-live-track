import pymysql
import pprint
import datetime
import math
import os
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter
import sys
import time

# this code takes twitter data from the database and:           -- DONE
# 1. Removes all the duplicate data                             -- DONE (could be algorithm implement)
# 2. Extrapolates for any missing values in the data            -- DONE
# 3. Generates graphs for that data
# 4. Writes a pdf report for that data
#
# TRANSITION EVERYTHING TO HTML AND BOKEH PLOTS
with open('config.json', 'r') as f:
	config = json.load(f)
	database = config['database connection']

def utfcon():
	conn = pymysql.connect(host=database['host'], port=database['port'], user=database['user'], password=database['password'], database=database['database'])
	cursor = conn.cursor()
	return conn, cursor

def sql_fetching(username, table):
	conn, cursor = utfcon()
	print('sql fetching with username %s ' % username)
	sql = 'SELECT id from users where username = %s'
	cursor.execute(sql, username)
	idtuple = cursor.fetchall()

	if idtuple:
		for item in idtuple:
			userid = item[0]
	else:
		sys.exit('the user was not in the database')

	cursor.execute('select num, inc_dec, id, datecol, timecol, weekday from ' + table + ' where user_id =%s and inc_dec=1', (userid))

	data = []
	for row in cursor.fetchall():
		shallow = []
		for i in row:
			# this handles datecol
			if type(i) == datetime.date:
				# if i == datetime.date(2018, 10, 15):
				#     print('breaking')
				#     break
				dt = i
			# this handles the timecol
			elif type(i) == datetime.timedelta:
				dt2 = datetime.datetime.strptime(str(i), '%H:%M:%S').time()
				t = datetime.datetime.combine(dt, dt2)
				shallow.append(t)
			else:
				shallow.append(i)
		data.append(shallow)

	# this function removes data for likes content that the user cant control
	# if table == 'tweet' or table == 'favorite':
	#     data_without_decrease = []
	#     for row in data:
	#         if row[1] == 1:
	#             data_without_decrease.append(row)
	#     data = data_without_decrease

	conn.commit()
	conn.close()
	return data


def fetch_data(username, content):

	if content == 'Favorites':
		table = 'favorite'
	elif content == 'Tweets':
		table = 'tweet'
	elif content == 'Follows':
		table = 'follow'
	elif content == 'Followers':
		table = 'follower'
	else:
		os.exit('the data was not found for content type %s' % content)

	if type(username) == list:
		data = []
		for u in username:
			data += sql_fetching(u, table)
	else:
		data = sql_fetching(username, table)

	# for row in data:
	#     print(row)

	return data

def remove_duplicate_data(row_data):
	confirmed_data = [] # this data is in row format

	for row in row_data:
		add_data = True


		num = row[0]  # this is the like number from the columar data that we are dealing with
		inc_dec = row[1]

		if len(confirmed_data) > 1:
			# print(confirmed_data[-1][0], confirmed_data[-2][0], confirmed_data[-3][0], confirmed_data[-4][0])
			if confirmed_data[-1][0] == num-1:
				if confirmed_data[-1][1] == inc_dec * -1:
					confirmed_data.append(row)
					continue

		if len(confirmed_data) > 4:
			if confirmed_data[-3][0] == num-1:
				if confirmed_data[-3][1] == inc_dec * -1:
					confirmed_data.append(row)
					continue

		for i in range(len(confirmed_data)):
			confirmed_num = confirmed_data[i][0]
			confirmed_inc_dec = confirmed_data[i][1]

			if num == confirmed_num:

				if confirmed_inc_dec == inc_dec:

					prev1 = confirmed_data[i-1][1]

					if (prev1 == -1* inc_dec):
						pass
					else:
						add_data = False
						break
		if add_data:
			confirmed_data.append(row)

	return confirmed_data


def fill_in_missing_data(row_data):
	prev_row = False
	new_row_data = []
	for row in row_data:
		# cant interpolate the first point
		if prev_row == False:
			prev_row = row  # hold the row for the next iter
			new_row_data.append(prev_row)
			continue
		num = row[0]
		old_num = prev_row[0]
		# print(row)
		# print(num, old_num, row[3], num-old_num)

		if  abs(num - old_num) != 1 and  abs(num-old_num) != 0:            # i is positive for an increase and negative for a decrease

			if num > old_num:
				inc_dec = 1
				row_iter = list(range(old_num, num+1))[1:]
				# print(row_iter)
			else:
				row_iter = list(range(num, old_num))[::-1]
				inc_dec = -1
				# print(row_iter)

			index = 0
			dates = difference(prev_row[3], row[3], abs(num-old_num))

			for i in row_iter:

				new_row_data.append([i, inc_dec, 0, dates[index], dates[index].weekday()])
				# print(len(new_row_data))
				index += 1

		else:
			# there is no skip in the data so we can move on
			new_row_data.append(row)
			# print('else', len(new_row_data))
		prev_row = new_row_data[-1]
# 55674 60311


	return new_row_data

def difference(start, end, intervals):
	diff = (end - start) / intervals

	dateobjlist = []
	for i in range(intervals):
		dateobjlist.append(start + (diff * i))
	return dateobjlist

def create_visuals(data, folderpath, user, type_of_content):
	def smooth(y, box_pts):
		box = np.ones(box_pts) / box_pts
		y_smooth = np.convolve(y, box, mode='same')
		return y_smooth

	def split_months(data_to_split, day_interval):
		nested_lists = []
		shallow = []
		base_time = data_to_split[0][3]
		delta = datetime.timedelta(days=day_interval)
		for row in data_to_split:
			if row[3] > base_time + delta:
				# print('more than %s days starting new list' % day_interval)
				nested_lists.append(shallow)
				shallow = []
				base_time = row[3]
			else:
				shallow.append(row)
		nested_lists.append(shallow)
		print('\n\nin nested lists with content %s' % type_of_content)
		for row in nested_lists:
			print(row)

		return nested_lists

	###########################################################################################
	#                                                                                         #
	# this function should behave differently for [follow / follower] vs [tweet / favorite]   #
	#                                                                                         #
	###########################################################################################
	def line_chart_over_time(period_data):
		pprint.pprint(period_data)
		# find the total dates we are looking for when they are increasing
		iteration = 0
		for batch in period_data:
			x_axis = []
			increase_decrease_list = []
			for row in batch:
				# if row[1] == 1:           # this used to make sure that it was just increaseing and not decreasing( we are using both right now)
				if row[1]:
					x_axis.append(row[3])
					increase_decrease_list.append(row[1])

			# find all the unique dates in the x_axis and then get the count of them
			overlapping_date = [i.date() for i in x_axis]
			unique_dates = []
			for date in overlapping_date:
				if date not in unique_dates:
					unique_dates.append(date)
			# y_axis  = smooth([overlapping_date.count(x) for x in unique_dates],2)     # this is legacy code that was used to smooth the data
			y_axis = [overlapping_date.count(x) for x in unique_dates]
			# new_unique = []
			#
			# if type_of_content == 'Follows' or type_of_content == 'Followers' or True:
			#     y_axis = []
			#     for f in range(len(unique_dates)):
			#         unique_day = unique_dates[f]
			#         total_change = 0
			#         print('!!!!!!!!!!!!!!!!!!!!!!!!!date being searched for is %s' % unique_day)
			#         for t in range(len(overlapping_date)):
			#             date = overlapping_date[t]
			#             increase_decrease = increase_decrease_list[t]
			#
			#
			#             if unique_day == date:
			#                 # print('the day is identical! %s is the same is %s\nthe increase_decrease is %s\n' % (unique_day, date, increase_decrease))
			#                 total_change += increase_decrease
			#         if  total_change > 0 or True:
			#             new_unique.append(unique_day)
			#             if len(y_axis) > 0:
			#                 print('the total change for this segment was %s, the previous change was %s' % (total_change, y_axis[-1]))
			#
			#                 y_axis.append(total_change + y_axis[-1])
			#                 # y_axis.append(total_change)
			#
			#             else:
			#                 y_axis.append(total_change)
			#
			# unique_dates = new_unique


			print('yaxis ', y_axis)
			# convert all the x axis to strings in day/ month / year
			x_axis = [x.strftime('%d/%m/%y') for x in unique_dates]

			xlabel, ylabel = 'date', 'occurances on date'

			new_x = []
			new_y = []
			for i in range(len(x_axis)):
				date = x_axis[i]
				count= y_axis[i]
				if count < 20:
					new_x.append(date)
					new_y.append(count)
			x_axis, y_axis = new_x, new_y

			# convert the lists into a dataframe
			d = {xlabel: x_axis, ylabel: y_axis}
			print('\n\n\n')
			print('about to create dataframe for content %s' % type_of_content)
			print('xlabel')
			print(d[xlabel])
			print('len xlabel %s' % len(d[xlabel]))

			print('ylabel')
			print(d[ylabel])
			print('len xlabel %s' % len(d[ylabel]))

			df = pd.DataFrame(d)

			print(df.head())
			# plot commands
			sns.set_style("darkgrid")
			f, ax = plt.subplots(figsize=(20,5))
			sns.lineplot(data=df, x=xlabel, y=ylabel)
			plt.xticks(rotation=45)
			ax.set_xticklabels(x_axis)
			plt.title('Number of %s over time' % type_of_content)
			# plt.show()

			lineplot_path = os.path.join(folderpath, type_of_content, 'lineplot')
			lineplot_path = os.path.join(folderpath, type_of_content)
			if not os.path.exists(lineplot_path):
				os.mkdir(lineplot_path)

			# plt.savefig(os.path.join(lineplot_path, str(iteration) + '.png'), bbox_inches='tight', dpi=400)
			plt.savefig(os.path.join(lineplot_path, 'lineplot' + '.png'), bbox_inches='tight', dpi=400)
			iteration += 1

			plt.cla()
			plt.clf()
			plt.close()

	def pie_charts(period_data):
		day_list = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
		iteration = 0

		for batch in period_data:
			week_days = list(zip(*batch))[-1]
			days_count = [week_days.count(day_num) for day_num in range(7)]

			f, ax = plt.subplots(figsize= (5.5,5)) # width, height
			ax.pie(days_count,labels=day_list,autopct='%1.1f%%',shadow=True, startangle=90)
			ax.axis('equal')

			# pie_path = os.path.join(folderpath, type_of_content, 'pie')
			pie_path = os.path.join(folderpath, type_of_content)
			if not os.path.exists(pie_path):
				os.mkdir(pie_path)
			plt.title('Percentage of %s for each weekday' % type_of_content)
			# plt.savefig(os.path.join(pie_path, str(iteration) + '.png'), bbox_inches='tight')

			plt.savefig(os.path.join(pie_path, 'pie chart' + '.png'), bbox_inches='tight')
			iteration += 1

			plt.cla()
			plt.clf()
			plt.close()

	def stacked_bar_day(period_data):    # stacked bar chart showing activity per hour of the day
		iteration = 0
		for batch in period_data:
			cols = list(zip(*batch))
			batch_weekdays = cols[-1]
			hour_parse = [i.hour for i in cols[3]]

			daycount_full = []

			for hour in range(24):
				shallow = []
				for weekday in range(7):
					for i in range(len(hour_parse)):
						h = hour_parse[i]
						w = batch_weekdays[i]

						if h == hour and weekday == w:
							shallow.append(weekday)

				daycount_full.append(shallow)


			# this is a list of the number of times that a weekday occured in each hour segment
			count_of_weekdays = []
			for row in daycount_full:
				shallow = []
				for k in range(7):
					shallow.append(row.count(k))
				count_of_weekdays.append(shallow)

			positions = np.arange(7)/8
			width = .1

			mylegend = ['12 AM']
			for i in range(1,12):
				mylegend.append(str(i) + ' AM')
			mylegend.append('12 PM')
			for i in range(1,12):
				mylegend.append(str(i) + ' PM')

			sns.set_style('darkgrid')

			f, ax = plt.subplots(figsize=(4, 6))  # width, height

			bot = np.cumsum(count_of_weekdays, axis=0)
			for i in range(len(count_of_weekdays)):
				row = count_of_weekdays[i]

				if i == 0:
					plt.bar(positions, row, width)
				else:
					ax.bar(positions, row, width, bottom = bot[i-1] )

			ax.legend(mylegend, loc='upper right')
			plt.xticks(positions, ('mon', 'tues', 'wed', 'thur', 'fri', 'sat', 'sun'))
			plt.xticks(rotation=45)

			box = ax.get_position()
			ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
			ax.legend(mylegend, loc='center left', bbox_to_anchor=(1, 0.5))



			# bar_path = os.path.join(folderpath, type_of_content, 'stacked_bar_day')
			bar_path = os.path.join(folderpath, type_of_content)
			if not os.path.exists(bar_path):
				os.mkdir(bar_path)
			plt.title('Number of likes on each day separated by hour')
			# plt.savefig(os.path.join(bar_path, str(iteration) + '.png'), bbox_inches='tight', dpi=170)
			plt.savefig(os.path.join(bar_path, 'stacked bar chart by day' + '.png'), bbox_inches='tight', dpi=170)

			plt.cla()
			plt.clf()
			iteration += 1

	def stacked_bar_hour(period_data):    # stacked bar chart showing activity per hour of the day
		iteration = 0
		# bar_path = os.path.join(folderpath, type_of_content, 'stacked_bar')
		bar_path = os.path.join(folderpath, type_of_content)
		if not os.path.exists(bar_path):
			os.mkdir(bar_path)
		for batch in period_data:
			cols = list(zip(*batch))
			batch_weekdays = cols[-1]
			hour_parse = [i.hour for i in cols[3]]

			daycount_full = []

			for weekday in range(7):
				shallow = []
				for hour in range(24):
					for i in range(len(hour_parse)):
						h = hour_parse[i]
						w = batch_weekdays[i]

						if h == hour and weekday == w:
							shallow.append(hour)

				daycount_full.append(shallow)

			# this is a list of the number of times that a weekday occured in each hour segment
			count_of_hours = []
			for row in daycount_full:
				shallow = []
				for k in range(24):
					shallow.append(row.count(k))
				count_of_hours.append(shallow)

			positions = np.arange(24)
			width = .7

			mylegend = ('mon', 'tues', 'wed', 'thur', 'fri', 'sat', 'sun')
			ticks = ['12 AM', '1 AM', '2 AM', '3 AM', '4 AM', '5 AM', '6 AM', '7 AM', '8 AM', '9 AM', '10 AM', '11 AM',
					 '12 PM', '1 PM', '2 PM', '3 PM', '4 PM', '5 PM', '6 PM', '7 PM', '8 PM', '9 PM', '10 PM', '11 PM']

			sns.set_style('darkgrid')

			f, ax = plt.subplots(figsize=(15, 6))  # width, height

			bot = np.cumsum(count_of_hours, axis=0)
			count_of_hours = np.array(count_of_hours)
			for i in range(len(count_of_hours)):
				row = count_of_hours[i]
				if i == 0:
					ax.bar(positions, row, width)
				else:
					ax.bar(positions, row, width, bottom=bot[i-1])

			plt.xticks(positions, ticks)
			plt.xticks(rotation=90)

			box = ax.get_position()
			ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
			ax.legend(mylegend, loc='center left', bbox_to_anchor=(1, 0.5))

			plt.title('Number of likes on each day separated by hour')

			# plt.savefig(os.path.join(bar_path, str(iteration) + '.png'), bbox_inches='tight', dpi=170)
			plt.savefig(os.path.join(bar_path, 'stacked barchart by hour' + '.png'), bbox_inches='tight', dpi=170)

			plt.cla()
			plt.clf()
			iteration +=1

	# if len(data) > 100:
	#     split_data = split_months(data, 45)
	# else:
	#     split_data = [data]

	print('the length of daya is %s' % len(data))

	# line_chart_over_time(split_data)
	# pie_charts(split_data)
	# stacked_bar_hour(split_data)
	# stacked_bar_day(split_data)

	line_chart_over_time([data])
	pie_charts([data])
	stacked_bar_hour([data])
	stacked_bar_day([data])

def main():
	folder = r'C:\Users\Brooks\Desktop\twitter reports'

	NAME = 'jordanswan_'

	if type(NAME) == list:
		FOLDERNAME = NAME[0]
	else:
		FOLDERNAME = NAME
	user_folder = os.path.join(folder, FOLDERNAME)

	content_types = ['Tweets', 'Favorites', 'Follows', 'Followers']

	# content_types = ['Favorites']

	# create data folders
	if not os.path.exists(user_folder):
		os.mkdir(user_folder)
	for i in content_types:
		if not os.path.exists(os.path.join(user_folder, i)):
			os.mkdir(os.path.join(user_folder, i))


	for content in content_types:
		print('doing content %s' % content)
		# if the data for the user was not generated then we should generate it
		pickle_path = os.path.join(user_folder, FOLDERNAME + '_' + content + '.pickle')

		if not os.path.exists(pickle_path):
			# generate data that has duplicates removed and interpolate the rest of it
			data = fetch_data(NAME, content)
			# data = fill_in_missing_data(data)
			# data = remove_duplicate_data(data)

			#store the data in a pickle se we dont need to reformat later
			with open(pickle_path, 'wb') as file:
				pickle.dump(data, file)
			print('dumped pickle file')
		# if the data was already generated we just pull it
		else:
			with open (pickle_path, 'rb') as file:
				data = pickle.load(file)
				pprint.pprint(data)
				print('\n\n\n\n\n\n\n\n\n')

		create_visuals(data, user_folder, FOLDERNAME, content)



if __name__ == '__main__':
	main()

	# sample =  np.linspace(0,.5,100)
	# x = savgol_filter(sample, int(len(sample)*3/4), 3)
	# print(x)
