# Hong Kong Hourly Temperature and Rainfall Crawler

The program downloads hourly reading and rainstorm signals issuing reports from http://www.info.gov.hk/gia/wr/201808/28.htm.
Data will be exported in csv format and saved in `./data`, and sorted to folders with associated physical quantity.

## Usage
In the main function of `./crawler.py`, you may specify following parameters:

- `saveDir`, directory to export, e.g. :`"./data"`
- `startDate` = Start date of the dataset you want to crawl, e.g.:`"2018-08-26"`.  Latest start date should be the `endDate`
- `endDate` = Last date of the dataset you want to crawl, e.g.: `"Today"`,`"2018-08-28"` 

## Points to Note
- Hourly reading may have issued more than one time every hour. By observation temperature and rainfall data doesn't change for report within same hour. Instead duration of thunderstorm signal maybe extended.
- Rainfall record time is from xx:45 to yy:45 every hour, where xx and yy are consecutive hours. The time delay of report dispatch may differ from instant measurements.