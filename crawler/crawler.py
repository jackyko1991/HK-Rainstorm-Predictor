import requests
import pandas as pd
from urllib.parse import urlsplit, urlunsplit
import datetime
from bs4 import BeautifulSoup
import re
import csv
import os

class RainDataCrawler():
	def __init__(self, url="", saveDir=""):
		self.url = url
		self.saveDir = saveDir
		self.crawlTemp = True
		self.crawlRain = True
		self.crawlSignal = True

	def crawlTempWorker(self,reportString):
		reportStrings = re.split('\s{2,}',reportString)
		tempDict = {}

		crawl = False
		location = ""
		for string in reportStrings: 
			if "HONG KONG OBSERVATORY" in string:
				reportList = re.sub("[^\w]", " ",  string).split()
				if reportList[2] == "A":
					time = int(reportList[1])
				elif reportList[2] == "P":
					time = int(reportList[1])+12
				elif reportList[1] == "NOON":
					time = 12
				elif reportList[1] == "MIDNIGHT":
					time = 0
				tempDict['TIME'] = time

				tempIdx = reportList.index("DEGREES")-1
				tempDict['HONG KONG OBSERVATORY'] = int(reportList[tempIdx])

			if string == "THE AIR TEMPERATURES AT OTHER PLACES WERE:":
				crawl = True 
			if "DEGREES." in string:
				crawl = False
			if crawl and ("DEGREES" in string):
				tempDict[location] = int(re.sub("[^\w]", " ",  string).split()[0])
			location = string
			
		return tempDict

	def crawlRainWorker(self,reportString):
		reportStrings = re.split('\s{2,}|\n',reportString)
		rainDict = {}
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
			'SOUTHERN DIS8R15T',\
			'WAN CHAI']
		# 
		rainDict['RAINSTORM SIGNAL ISSUING AT DISPATCH'] = 'NO'

		for location in locations:
			rainDict[location+" MIN"] = 0
			rainDict[location+" MAX"] = 0

		crawl = False
		location = ""
		for string in reportStrings: 
			if "HONG KONG OBSERVATORY" in string:
				reportList = re.sub("[^\w]", " ", string).split()
				if reportList[2] == "A":
					time = int(reportList[1])
				elif reportList[2] == "P":
					time = int(reportList[1])+12
				elif reportList[1] == "NOON":
					time = 12
				elif reportList[1] == "MIDNIGHT":
					time = 0
				rainDict['TIME'] = time

			if "VARIOUS REGIONS WERE:" in string:
				crawl = True 
			if "MM." in string:
				crawl = False
			if crawl and ("MM" in string):
				rainRange = [int(s) for s in string.split() if s.isdigit()]
				if len(rainRange) == 1:
					rainRange.append(rainRange[0])
				rainDict[location + " MIN"] = rainRange[0]
				rainDict[location + " MAX"] = rainRange[1]
			location = string

			if "RAINSTORM WARNING SIGNAL" in string:
				rainDict['RAINSTORM SIGNAL ISSUING AT DISPATCH'] = string.split(" ")[1]
		return rainDict

	def crawlHourlyReadings(self,url):
		resp = requests.get(url)
		soup = BeautifulSoup(resp.text,"lxml")
		report = soup.find_all("div",id="weather_report")[0]
		for br in soup.find_all("br"):
			br.replace_with("\n")

		reportString = str(report.text)

		if self.crawlTemp:
			tempDict = self.crawlTempWorker(reportString)
			tempDict['DATE'] = datetime.datetime.strptime(url.split("/")[5]+url.split("/")[6],"%Y%m%d").strftime("%Y-%m-%d")
			fieldnames = ['DATE', 'TIME']
			for key,value in tempDict.items():
				if (key!= 'DATE' and key!='TIME'):
					fieldnames.append(key)
			# save data
			if not os.path.exists(os.path.join(self.saveDir,'temperature',tempDict['DATE'][:7] + '.csv')):
				with open(os.path.join(self.saveDir,'temperature',tempDict['DATE'][:7] + '.csv'), 'w',newline='') as csv_file:
					writer = csv.DictWriter(csv_file,fieldnames=fieldnames)
					writer.writeheader()
					writer.writerow(tempDict)
			else:
				df = pd.read_csv(os.path.join(self.saveDir,'temperature',tempDict['DATE'][:7] + '.csv'))
				tempDf = pd.DataFrame(tempDict, index=[0])
				df = pd.concat([df,tempDf])
				df.drop_duplicates(subset=["DATE", "TIME"],inplace=True)
				df = df[fieldnames]
				df.to_csv(os.path.join(self.saveDir,'temperature',tempDict['DATE'][:7] + '.csv'), index=False)
		
		if self.crawlRain:
			rainDict = self.crawlRainWorker(reportString)

			rainDict['DATE'] = datetime.datetime.strptime(url.split("/")[5]+url.split("/")[6],"%Y%m%d").strftime("%Y-%m-%d")
			fieldnames = ['DATE', 'TIME']
			for key,value in rainDict.items():
				if (key!= 'DATE' and key!='TIME'):
					fieldnames.append(key)
			# save data
			if not os.path.exists(os.path.join(self.saveDir,'rainfall',rainDict['DATE'][:7] + '.csv')):
				with open(os.path.join(self.saveDir,'rainfall',rainDict['DATE'][:7] + '.csv'), 'w',newline='') as csv_file:
					writer = csv.DictWriter(csv_file,fieldnames=fieldnames)
					writer.writeheader()
					writer.writerow(rainDict)
			else:
				df = pd.read_csv(os.path.join(self.saveDir,'rainfall',rainDict['DATE'][:7] + '.csv'))
				rainDf = pd.DataFrame(rainDict, index=[0])
				df = pd.concat([df,rainDf])
				df.drop_duplicates(subset=["DATE", "TIME"],inplace=True)
				df = df[fieldnames]
				df.to_csv(os.path.join(self.saveDir,'rainfall',rainDict['DATE'][:7] + '.csv'), index=False)

		return

	def crawlRainstormWarningSignal(self,url):
		return

	def crawl(self):
		# create save folder
		if (not os.path.exists(os.path.join(self.saveDir,'temperature')) and self.crawlTemp):
			os.makedirs(os.path.join(self.saveDir,'temperature'))
		if (not os.path.exists(os.path.join(self.saveDir,'rainfall')) and self.crawlRain):
			os.makedirs(os.path.join(self.saveDir,'rainfall'))
		if (not os.path.exists(os.path.join(self.saveDir,'rainstorm')) and self.crawlSignal):
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
				# exit()
			elif "RAINSTORM WARNING SIGNAL" in li.text:
				signalUrl = list(urlsplit(self.url))
				signalUrl[2] = li.a.get('href')
				signalUrl = urlunsplit(signalUrl)
				if self.crawlSignal:
					self.crawlRainstormWarningSignal(signalUrl)
				else:
					continue

def main():
	# data url
	url = "http://www.info.gov.hk/gia/wr/ym/d.htm"

	# output directory
	saveDir = "./data"

	startDate = "2018-07-01" #%Y-%m-%d, up to date is enough, latest start date should be the end date
	# endDate = "2018-08-15" #%Y-%m-%d, up to date is enough, latest start date should be the end date
	endDate = "Today" #%Y-%m-%d or "Today"

	startDateTime = datetime.datetime.strptime(startDate,"%Y-%m-%d")
	if endDate == "Today":
		endDateTime = datetime.datetime.now().replace(microsecond=0)
	else:
		endDateTime = datetime.datetime.strptime(endDate,"%Y-%m-%d")

	parsed = list(urlsplit(url))

	dateTime = endDateTime

	crawler = RainDataCrawler()
	crawler.saveDir = saveDir

	dayCount = 0;
	while dateTime.strftime("%Y-%m-%d") != (startDateTime-datetime.timedelta(days=1)).strftime("%Y-%m-%d"): # better to compare with strings
		print("Crawling temperature and rainfall data in progress: %d/%d (%s)"%(dayCount+1,(endDateTime-startDateTime).days+1,dateTime.strftime("%Y-%m-%d")))
		# exit()

		path = "gia/wr/"+dateTime.strftime("%Y%m")+"/"+dateTime.strftime("%d") + ".htm"
		parsed[2] = path
		currentUrl = urlunsplit(parsed)

		# actual crawl the data
		crawler.url = currentUrl
		crawler.crawlTemp = False
		crawler.crawlRain = True
		crawler.crawlSignal = False
		crawler.crawl()
		dateTime = dateTime-datetime.timedelta(days=1)
		dayCount = dayCount+1

if __name__=="__main__":
	main()