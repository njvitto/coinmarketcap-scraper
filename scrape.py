""" Core scraper for coinmarketcap.com. """
import db
import logging
import coinmarketcap
import sys
import time
import traceback

# Configuration
timestamp_0 = 1367174841000
timestamp_1 = int(round(time.time() * 1000))
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s %(levelname)s: %(message)s',
	datefmt='%m/%d/%Y %I:%M:%S %p')

database = db.Database()


def scrapeCurrencyList():
    """Scrape currency list."""
    html = coinmarketcap.requestCurrencyList('all')
    data = coinmarketcap.parseCurrencyList(html)
    return data


def scrapeMarketCap(slug, timestamp_0, timestamp_1):
    """Scrape market cap for the specified currency slug."""
    jsonDump = coinmarketcap.requestMarketCap(slug, timestamp_0, timestamp_1)
    result = coinmarketcap.parseMarketCap(jsonDump, slug)
    database.batch_entry(result)


logging.info("Attempting to scrape currency list...")
currencies = scrapeCurrencyList()
logging.info("Finished scraping currency list. Starting on currencies...")
for currency in currencies:
	logging.info("> Starting scrape of currency {0}...".format(currency['slug']))
	try:
		scrapeMarketCap(currency['slug'], timestamp_0, timestamp_1)
	except Exception as e:
		print '-'*60
		print "Could not scrape currency {0}.".format(currency['slug'])
		print traceback.format_exc()
		print '-'*60
		logging.info(">> Could not scrape {0}. Skipping.".format(currency['slug']))
		continue
logging.info("Finished scraping currencies. All done.")
logging.info("Made {0} requests in total.".format(coinmarketcap.countRequested))