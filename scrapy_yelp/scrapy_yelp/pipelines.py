# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
from scrapy_yelp.items import RestaurantItem, CommentItem, UserItem


class ScrapyYelpPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('DATABASE')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):

        self.client.close()
    
    def process_item(self, item, spider):
        if isinstance(item, RestaurantItem):
            self.db[item.table_name].update({'rest_url': item.get('rest_url')}, {'$set': dict(item)}, True)
            print("restaurant+1")
            return item
        elif isinstance(item,CommentItem):
            self.db[item.table_name].update({'review_id':item.get('review_id')},{'$set':dict(item)},True)
            print("review count+1")
            return item
        elif isinstance(item, UserItem):
            self.db[item.table_name].update({'user_id':item.get('user_id'),},{'$set':dict(item)},True)
            print("user count+1")

        return item
