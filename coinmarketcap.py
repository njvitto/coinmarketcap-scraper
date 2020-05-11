""" Module for requesting data from coinmarketcap.org and parsing it. """
from datetime import datetime
import json
import logging
import lxml.html
from random import random
import requests
import time
from future.utils import iteritems
from bs4 import BeautifulSoup

baseUrl = "https://coinmarketcap.com"
graphBaseUrl = "https://graphs2.coinmarketcap.com" #Coinmarket cap endpoint changed from graphs to graphs2

countRequested = 0
interReqTime = 20
lastReqTime = None


def _request(target):
    """Private method for requesting an arbitrary query string."""
    global countRequested
    global lastReqTime
    if lastReqTime is not None and time.time() - lastReqTime < interReqTime:
        timeToSleep = random()*(interReqTime-time.time()+lastReqTime)*2
        logging.info("Sleeping for {0} seconds before request.".format(timeToSleep))
        time.sleep(timeToSleep)
    logging.info("Issuing request for the following target: {0}".format(target))
    r = requests.get(target)
    lastReqTime = time.time()
    countRequested += 1
    if r.status_code == requests.codes.ok:
        return r.text
    else:
        raise Exception("Could not process request. \
            Received status code {0}.".format(r.status_code))


def requestList(type, view):
    """Request a list of all coins or tokens."""
    assert(type == "tokens" or type == "coins",
        "Can only request tokens or coins")
    return _request("{0}/{1}/views/{2}/".format(
        baseUrl,
        type,
        view))


def requestMarketCap(slug):
    """Request market cap data for a given coin slug."""
    return _request("{0}/currencies/{1}/".format(
        graphBaseUrl, slug))

def parseList(html, type):
    """Parse the information returned by requestList for view 'all'."""

    data = []

    docRoot = lxml.html.fromstring(html)

    rows = docRoot.cssselect("table > tbody > tr")

    for row in rows:
        datum = {}
        fields = row.cssselect("td")

        # Ranking
        datum['ranking'] = fields[0].text_content().strip()

        # Name and slug
        nameField = fields[1].cssselect("a")[0]

        datum['name'] = nameField.text_content().strip()

        datum['slug'] = nameField.attrib['href'].replace(
            '/currencies/', '').replace('/', '').strip()

        # Symbol
        datum['symbol'] = fields[2].text_content().strip()

        # Explorer link
        supplyFieldPossible = fields[5].cssselect("a")
        if len(supplyFieldPossible) > 0:
            datum['explorer_link'] = supplyFieldPossible[0].attrib['href']
        else:
            datum['explorer_link'] = ''

        data.append(datum)

    return data

def gatherHistoricalDataFor(coin, start_date, end_date):
    historicaldata = []
    request_string = "https://coinmarketcap.com/currencies/{0}/historical-data/?start={1}&end={2}".format(coin['slug'], start_date, end_date)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    logging.info(request_string)
    r  = requests.get(request_string, headers=headers)
    
    #logging.debug(r.text)

    soup = BeautifulSoup(r.text, "lxml")
    #unfortunately there are different div with some other css classes and also cmc-table__table-wrapper-outer
    #table_div = soup.find('div', attrs={ "class" : "cmc-table__table-wrapper-outer"})
    #if the result contains all the divs with "also" cmc-table__table-wrapper-outer the right table as of now is the 2nd
    #table = table_div.find_all('table')[2] #much better to point directly to the table

    #a bit cleaner solution, selecting exactly the div with just that class cointaining only the table that we need
    table = soup.select("div[class='cmc-table__table-wrapper-outer']")[0].table

    #Add table header to list
    if len(historicaldata) == 0:
        headers = [header.text for header in table.thead.find_all('th')]
        headers.insert(0, "Coin")

    for row in table.tbody.find_all('tr'):
        currentrow = [val.text for val in row.find_all('td')]
        if(len(currentrow) != 0):
            currentrow.insert(0, coin)
        historicaldata.append(currentrow)

    return headers, historicaldata

def parseMarketCap(jsonDump, slug):
    """ """
    data = []
    rawData = json.loads(jsonDump)

    # Covert data in document to wide format
    dataIntermediate = {}
    targetFields = [str(key.replace('_data', '')) for key in rawData.keys()]
    for field, fieldData in iteritems(rawData):
        for row in fieldData:
            time = int(row[0]/1000)
            if time not in dataIntermediate:
                dataIntermediate[time] = dict(zip(targetFields, [None]*len(targetFields)))
            dataIntermediate[time][field] = row[1]

    # Generate derived data & alter format
    times = sorted(dataIntermediate.keys())
    for time in times:
        datum = dataIntermediate[time]
        datum['slug'] = slug
        datum['time'] = datetime.utcfromtimestamp(time)

        if (datum['market_cap_by_available_supply'] is not None
            and datum['price_usd'] is not None
            and datum['price_usd'] is not 0):
            datum['est_available_supply'] = float(datum['market_cap_by_available_supply'] / datum['price_usd'])
        else:
            datum['est_available_supply'] = None

        data.append(datum)

    return data
