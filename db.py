"""
Database operations
"""

import sqlite3
import time
import datetime

class Database:

	_path = 'database.db'
	_conn = None
	_c = None


	def __init__(self):
		self._conn = sqlite3.connect(self._path)
		self._c = self._conn.cursor()
		self._create_tables()


	def _create_tables(self):
		"""Creates the tables.

		Creates the tables coin and vals unless they already exist.
		"""
		self._c.execute("CREATE TABLE IF NOT EXISTS coin ( "
			  			"id INTEGER PRIMARY KEY, "
			  			"name CHAR(50) NOT NULL, "
			  			"type CHAR(50) NOT NULL, "
			  			"slug CHAR(50) UNIQUE NOT NULL)")
		self._c.execute("CREATE TABLE IF NOT EXISTS vals ( "
			  			"id INTEGER PRIMARY KEY, "
			  			"price_usd REAL NOT NULL, "
			  			"price_btc REAL NOT NULL, "
			  			"volume_usd REAL NOT NULL, "
			  			"market_cap_usd REAL NOT NULL, "
			  			"available_supply REAL NOT NULL, "
			  			"datetime TEXT NOT NULL, "
			  			"coin_slug CHAR(50) NOT NULL, "
			  			"FOREIGN KEY(coin_slug) REFERENCES coin(slug) "
			  			"ON DELETE CASCADE ON UPDATE CASCADE)")
		self._conn.commit()


	def _val_entry(self, name, slug, type, price_usd, price_btc, volume_usd,
				   market_cap_usd, available_supply, datetime):
		"""Enter new data into the database."""
		# If coin does not exist yet, insert entry
		self._c.execute("SELECT * FROM coin WHERE slug = ?", (slug,))
		if (self._c.fetchone() is None):
			self._c.execute("INSERT INTO coin (name, slug, type) VALUES (?, ?, ?)",
				(name, slug, type))
			self._conn.commit()
		# Insert values
		self._c.execute("INSERT INTO vals ("
						"price_usd, price_btc, volume_usd, market_cap_usd, "
						"available_supply, datetime, coin_slug) "
		  				"VALUES(?, ?, ?, ?, ?, ?, ?)", (
		  					price_usd, price_btc, volume_usd, market_cap_usd,
		  					available_supply, datetime, slug))
		self._conn.commit()


	def batch_entry(self, data, name, type):
		for entry in data:
			self._val_entry(name,
							entry['slug'],
							type,
							entry['price_usd'],
							entry['price_btc'],
							entry['volume_usd'],
							entry['market_cap_by_available_supply'],
							entry['est_available_supply'],
							entry['time'])


	def __del__(self):
		self._conn.close()
