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
    html = coinmarketcap.requestList('currencies', 'all')
    data = coinmarketcap.parseList(html, 'currencies')
    return data


def scrapeAssetList():
	"""Scrape asset list."""
	html = coinmarketcap.requestList('assets', 'all')
	data = coinmarketcap.parseList(html, 'assets')
	return data


def scrapeMarketCap(slug, name, type):
    """Scrape market cap for the specified currency slug."""
    jsonDump = coinmarketcap.requestMarketCap(slug)
    result = coinmarketcap.parseMarketCap(jsonDump, slug)
    database.batch_entry(result, name, type)


logging.info("Attempting to scrape currency list...")
currencies = scrapeCurrencyList()
assets = scrapeAssetList()
logging.info("Finished scraping currency list. Starting on currencies...")
for asset in assets:
	logging.info("> Starting scrape of asset {0}...".format(asset['slug']))
	try:
		scrapeMarketCap(asset['slug'], asset['name'], 'asset')
	except Exception as e:
		print '-'*60
		print "Could not scrape asset {0}.".format(asset['slug'])
		print traceback.format_exc()
		print '-'*60
		logging.info(">> Could not scrape {0}. Skipping.".format(asset['slug']))
		continue
for currency in currencies:
	logging.info("> Starting scrape of currency {0}...".format(currency['slug']))
	try:
		scrapeMarketCap(currency['slug'], currency['name'], 'currency')
	except Exception as e:
		print '-'*60
		print "Could not scrape currency {0}.".format(currency['slug'])
		print traceback.format_exc()
		print '-'*60
		logging.info(">> Could not scrape {0}. Skipping.".format(currency['slug']))
		continue
logging.info("Finished scraping currencies and assets. All done.")
logging.info("Made {0} requests in total.".format(coinmarketcap.countRequested))