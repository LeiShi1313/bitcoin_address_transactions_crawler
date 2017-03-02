# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WalletAddressCrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class AddressItem(scrapy.Item):
    address = scrapy.Field()
    wallet_id = scrapy.Field()

class AddrTxItem(scrapy.Item):
    more = scrapy.Field()
    n_tx = scrapy.Field()
    total_received = scrapy.Field()
    total_sent = scrapy.Field()
    _id = scrapy.Field()
    txs = scrapy.Field()
    wallet = scrapy.Field()
    service = scrapy.Field()
    t = scrapy.Field()
    index = scrapy.Field()
