import requests
import pandas as pd
from urllib.parse import urlsplit, urlunsplit
import datetime
from bs4 import BeautifulSoup
import re
import os

class RainDataCrawler():
	def __init__(self, url="", saveDir=""):
		self.url = url
		self.saveDir = saveDir

	def crawlTemp(self,reportString):
		reportStrings = re.split('\s{2,}',reportString)
		reportList = re.sub("[^\w]", " ",  reportStrings[1]).split()
		tempIdx = reportList.index("DEGREES")-1

		tempDict = {}
		tempDict['HONG KONG OBSERVATORY'] = int(reportList[tempIdx])

		reportStrings = re.split('\s{2,}|\n',reportString)
	
		crawl = False
		location = ""
		for string in reportStrings: 
			if string == "THE AIR TEMPERATURES AT OTHER PLACES WERE:":
				crawl = True 
			if "DEGREES." in string:
				crawl = False
			if crawl and ("DEGREES" in string):
				tempDict[location] = int(re.sub("[^\w]", " ",  string).split()[0])
			location = string

			if "DISPATCHED BY" in string:
				time = string.split(" ")[6]
				date = string.split(" ")[9]

				dateTime = datetime.datetime.strptime(date + " " + time,"%d.%m.%Y %H:%M")
				tempDict['DATE'] = dateTime.strftime("%Y-%m-%d")
				tempDict['TIME'] = dateTime.strftime("%H:%M")
			
		return tempDict

	def crawlRain(self,reportString):
		reportStrings = re.split('\s{2,}|\n',reportString)
		
		locations = ['NORTH DISTRICT',\
			'YUEN LONG', \
			'TUEN MUN',\
			'ISLANDS DISTRICT',\
			'TSUEN WAN',\
			'KWAI TSING',\
			'SHA TIN',\
			'TAI PO',\
			'WONG TAI SIN',\
			'SAI KUNG',\
			'CENTRAL & WESTERN DISTRICT',\
			'YAU TSIM MONG',\
			'SHAM SHUI PO',\
			'KOWLOON CITY',\
			'KWUN TONG',\
			'EASTERN DISTRICT',\
			'SOUTHERN DISTRICT',\
			'WAN CHAI']
		rainDict = {}

		for location in locations:
			rainDict[location] = [0,0]

		rainDict['RAINSTORM SIGNAL ISSUED'] = 'NO'

		crawl = False
		location = ""
		for string in reportStrings: 
			# print(string)
			if "VARIOUS REGIONS WERE:" in string:
				crawl = True 
			if "MM." in string:
				crawl = False
			if crawl and ("MM" in string):
				rainRange = [int(s) for s in string.split() if s.isdigit()]
				if len(rainRange) == 1:
					rainRange.append(rainRange[0])
				rainDict[location] = rainRange
			location = string

			if "DISPATCHED BY" in string:
				time = string.split(" ")[6]
				date = string.split(" ")[9]

				dateTime = datetime.datetime.strptime(date + " " + time,"%d.%m.%Y %H:%M")
				rainDict['DATE'] = dateTime.strftime("%Y-%m-%d")
				rainDict['TIME'] = dateTime.strftime("%H:%M")

			if "RAINSTORM WARNING SIGNAL" in string:
				rainDict['RAINSTORM SIGNAL ISSUED'] = string.split(" ")[1]
		return rainDict

	def crawlHourlyReadings(self,url):
		# print(url)
		resp = requests.get(url)
		soup = BeautifulSoup(resp.text,"lxml")
		report = soup.find_all("div",id="weather_report")[0]
		for br in soup.find_all("br"):
			br.replace_with("\n")

		reportString = str(report.text)
		tempDict = self.crawlTemp(reportString)
		rainDict = self.crawlRain(reportString)

		# save data


		# print(tempDict)
		print(rainDict)



		return

	def crawlRainstormWarningSignal(self,url):
		return

	def crawl(self):
		# create save folder

		if not os.path.exists(os.path.join(self.saveDir,'temperature')):
			os.makedirs(os.path.join(self.saveDir,'temperature'))
		if not os.path.exists(os.path.join(self.saveDir,'rainfall')):
			os.makedirs(os.path.join(self.saveDir,'rainfall'))
		if not os.path.exists(os.path.join(self.saveDir,'rainstorm')):
			os.makedirs(os.path.join(self.saveDir,'rainstorm'))

		resp = requests.get(self.url)
		soup = BeautifulSoup(resp.text, "lxml")
		ul = soup.find_all('ul', class_="list fontSize1")[0]
		for li in ul.find_all('li'):
			if  "HOURLY READINGS" in li.text:
				hourUrl = list(urlsplit(self.url))
				hourUrl[2] = li.a.get('href')
				hourUrl = urlunsplit(hourUrl)
				self.crawlHourlyReadings(hourUrl)
				exit()
			elif "RAINSTORM WARNING SIGNAL" in li.text:
				signalUrl = list(urlsplit(self.url))
				signalUrl[2] = li.a.get('href')
				signalUrl = urlunsplit(signalUrl)
				self.crawlRainstormWarningSignal(signalUrl)

def main():
	# data url
	url = "http://www.info.gov.hk/gia/wr/ym/d.htm"

	# output directory
	saveDir = "./data"

	startDate = "2018-08-27" #%Y-%m-%d %H, up to date is enough, start date should be at least one date earlier than end date
	endDate = "Now" #%Y-%m-%d %H or "Now"

	startDateTime = datetime.datetime.strptime(startDate,"%Y-%m-%d")
	if endDate == "Now":
		endDateTime = datetime.datetime.now().replace(microsecond=0)
	else:
		endDateTime = datetime.datetime.strptime(endDate,"%Y-%m-%d")

	parsed = list(urlsplit(url))

	dateTime = startDateTime

	crawler = RainDataCrawler()
	crawler.saveDir = saveDir

	while dateTime.strftime("%Y-%m-%d") != endDateTime.strftime("%Y-%m-%d"): # better to compare with strings
		path = "gia/wr/"+dateTime.strftime("%Y%m")+"/"+dateTime.strftime("%d") + ".htm"
		parsed[2] = path
		currentUrl = urlunsplit(parsed)

		# actual crawl the data
		crawler.url = currentUrl
		crawler.crawl()
		dateTime = dateTime+datetime.timedelta(days=1)

if __name__=="__main__":
	main()