import requests
import pandas as pd
from urllib.parse import urlsplit, urlunsplit
import datetime
from bs4 import BeautifulSoup
import re

class RainDataCrawler():
	def __init__(self, url="", saveDir=""):
		self.url = url
		self.saveDir = saveDir

	def crawlHourlyReadings(self,url):
		print(url)
		resp = requests.get(url)
		soup = BeautifulSoup(resp.text,"lxml")
		report = soup.find_all("div",id="weather_report")[0]
		for br in soup.find_all("br"):
			br.replace_with("\n")

		reportString = str(report.text)
		reportStrings = re.split('\s{4,}',reportString)

		for string in reportStrings:
			print(string)
		return

	def crawlRainstormWarningSignal(self,url):
		return

	def crawl(self):
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