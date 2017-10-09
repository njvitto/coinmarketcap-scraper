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


def scrapeCoinList():
    """Scrape coin list."""
    html = coinmarketcap.requestList('coins', 'all')
    data = coinmarketcap.parseList(html, 'currencies')
    return data


def scrapeTokenList():
    """Scrape token list."""
    html = coinmarketcap.requestList('tokens', 'all')
    data = coinmarketcap.parseList(html, 'assets')
    return data


def scrapeMarketCap(slug, name, type):
    """Scrape market cap for the specified coin or token slug."""
    jsonDump = coinmarketcap.requestMarketCap(slug)
    result = coinmarketcap.parseMarketCap(jsonDump, slug)
    database.batch_entry(result, name, type)

logging.info("Attempting to scrape token list...")
tokens = scrapeTokenList()
logging.info("Finished scraping token list. Starting on tokens...")
for token in tokens:
    logging.info("> Starting scrape of token {0}...".format(token['slug']))
    try:
        scrapeMarketCap(token['slug'], token['name'], 'token')
    except Exception as e:
        print '-'*60
        print "Could not scrape token {0}.".format(token['slug'])
        print traceback.format_exc()
        print '-'*60
        logging.info(">> Could not scrape {0}. Skipping.".format(token['slug']))
        continue
logging.info("Attempting to scrape coin list...")
coins = scrapeCoinList()
logging.info("Finished scraping coin list. Starting on coins...")
for coin in coins:
    logging.info("> Starting scrape of coin {0}...".format(coin['slug']))
    try:
        scrapeMarketCap(coin['slug'], coin['name'], 'coin')
    except Exception as e:
        print '-'*60
        print "Could not scrape coin {0}.".format(coin['slug'])
        print traceback.format_exc()
        print '-'*60
        logging.info(">> Could not scrape {0}. Skipping.".format(coin['slug']))
        continue
logging.info("Finished scraping tokens and coins. All done.")
logging.info("Made {0} requests in total.".format(coinmarketcap.countRequested))
