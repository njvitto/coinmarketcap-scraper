""" Module for requesting data from coinmarketcap.org and parsing it. """
from datetime import datetime
import json
import logging
import lxml.html
from random import random
import requests
import time

baseUrl = "http://coinmarketcap.com"
graphBaseUrl = "http://graphs.coinmarketcap.com"

countRequested = 0
interReqTime = 1
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
    """Request a list of all currencies or assets."""
    assert(type == "assets" or type == "currencies",
    	   "Can only request assets or currencies")
    return _request("{0}/{1}/views/{2}/".format(
		baseUrl,
		type,
		view))
		

def requestMarketCap(slug):
	"""Request market cap data for a given currency slug."""
	#return _request("{0}/v1/datapoints/{1}/{2}/{3}/".format(
	#	graphBaseUrl,
	#	slug,
	#	timestamp_0,
	#	timestamp_1))
	return _request("{0}/v1/datapoints/{1}/".format(
		graphBaseUrl, slug))


def parseList(html, type):
    """Parse the information returned by requestList for view 'all'."""
    assert(type == "assets" or type == "currencies",
    	   "Can only parse assets or currencies")
    
    data = []

    docRoot = lxml.html.fromstring(html)
    rows = docRoot.cssselect(
        "table#{0}-all > tbody > tr".format(type))
        
    for row in rows:
        datum = {}
        fields = row.cssselect("td")
        
        # Name and slug
        nameField = fields[1].cssselect("a")[0]
        datum['name'] = nameField.text_content().strip()
        datum['slug'] = nameField.attrib['href'].replace(
            '/{0}/'.format(type), '').replace('/', '').strip()
		
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


def parseMarketCap(jsonDump, slug):
	""" """
	data = []
	rawData = json.loads(jsonDump)
		
	# Covert data in document to wide format
	dataIntermediate = {}
	targetFields = [str(key.replace('_data', '')) for key in rawData.keys()]
	for field, fieldData in rawData.iteritems():
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
	