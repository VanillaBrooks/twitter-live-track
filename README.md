# twitter-live-track
Collect time of new tweet / favorite / follow / follower from twitter for list of users and store in mysql db


## Usage

A list of users in `users.txt` are scraped every 5 minutes to determine any change in account status. If any of the four basic user stats have changed, the current date and time are inserted into a MySQL table to be used in report generation. In order for information to be collected, `mysqlupdater.py` must be running. While extrapolation is an option in report generation, it quickly becomes innacurate over long periods of time


## Project Goal

The goal of this repository is to produce meaningful representations of social media use over time for a user. For example, the frequency of tweets (and retweets on each tweet) can be mapped against the total number of followers gained. In the future I would like to study the content of each tweet to be mapped, but this is currently outside the scope of the project. 

## TODO:

1. Render all graphs as `bokeh` plots to be used more dynamically on website instead of the current static state. 

2. Fix tick mark issues with graph generation

3. Section graphs into more meaningful periods of activity

## Example Graphs

![Favorite frequncy by hour](https://i.imgur.com/JP5wjvE.png)

![Number of daily likes over time](https://i.imgur.com/IbCtoQL.png)

![Activity by day of the week](https://i.imgur.com/HiMK2yX.png)

![Number of followers gained per day](https://i.imgur.com/1lsXmpk.png)

