# -*- coding: utf-8 -*-
import json
import random
import pandas as pd
from scrapy import Spider,Request
#from yelp.models import RestaurantItem, UserItem, CommentItem
from scrapy_yelp.items import RestaurantItem, CommentItem, UserItem
import re
from multiprocessing import Pool
import pymongo
class YelpSpider(Spider):
    name = 'scrapy_yelp'
    #allowed_domains = ['https://www.yelp.com']
    base_url = 'https://www.yelp.com'

    def __init__(self, *args, **kwargs):
        # We are going to pass these args from our django view.
        # To make everything dynamic, we need to override them inside __init__ method
        self.url = kwargs.get('url')
        self.domain = kwargs.get('domain')
        self.start_url = self.url
        self.allowed_domains = self.domain

        super(YelpSpider, self).__init__(*args, **kwargs)

    #myclient = pymongo.MongoClient("mongodb://10.244.10.6:27017/")
    #db = myclient.url#database
    #collection = db.url_table#collection
    #url_list = pd.DataFrame(list(collection.find()))['url'].tolist()
    #start_url = url_list[-1]
    #myclient.close()

    def start_requests(self):
        #delete database first
        myclient = pymongo.MongoClient("mongodb://10.244.10.6:27017/")
        dblist = myclient.list_database_names()
        if "django_yelp" in dblist:
            mydb = myclient["django_yelp"]
            mydb['commentTable'].drop()
            mydb['restaurantTable'].drop()
            mydb['userTable'].drop()
        yield Request(self.start_url, meta={'restaurant': 'Freemans'}, callback=self.parse_detail, dont_filter=True)
        yield Request(url=self.start_url, meta={'restaurant': 'Freemans'}, callback=self.parse_comment,dont_filter=True)

    # 解析餐厅信息
    def parse_detail(self,response):
        restaurant = response.meta['restaurant']
        rest_url = response.url

        score = str(response.xpath('//div[@class="rating-info clearfix"]/div[@class="biz-rating biz-rating-very-large clearfix"]/div//@title').extract_first())[0:3]
        total_reviews = response.xpath('//span[@class="review-count rating-qualifier"]//text()').extract_first().strip()
        category = response.xpath('//div[@class="biz-main-info embossed-text-white"]/div[@class="price-category"]/span[@class="category-str-list"]/a//text()').extract()
        address = response.xpath('//strong[@class="street-address"]/address//text()').extract_first().strip()
        phone = response.xpath('//span[@class="biz-phone"]//text()').extract_first().strip()
        items = RestaurantItem()

        for field in items.fields:
            try:
                items[field] = eval(field)
            except NameError:
                self.logger.debug("Field not Defined" + field)
        yield items

    # 解析评论信息
    def parse_comment(self, response):
        reviews = response.xpath('//div[@class="review-list"]/ul/li/div[@class="review review--with-sidebar"]')
        restaurant = response.meta['restaurant']
        rest_url = response.url
        for review in reviews:
            restaurant = restaurant
            rest_url = rest_url
            # review-sidebar

            user_name = review.xpath('.//div[@class="review-sidebar"]/div/div/div[@class="media-story"]/ul[@class="user-passport-info"]/li/a//text()').extract_first()
            user_url = review.xpath('.//div[@class="review-sidebar"]/div/div/div[@class="media-story"]/ul[@class="user-passport-info"]/li/a//@href').extract_first()
            user_url = self.base_url+str(user_url)
            user_id = review.xpath('.//@data-signup-object').extract_first()[8:]
            print("why not crawl people？",user_url)
            yield Request(url=user_url,meta={'user_id':user_id},callback=self.parse_user,dont_filter=True)

            # review-wrapper
            review_id = review.xpath('.//@data-review-id').extract_first()
            score = review.xpath('.//div[@class="review-wrapper"]/div/div/div[@class="biz-rating__stars"]/div//@title').re_first("(.*?) star rating")
            date = review.xpath('.//div[@class="review-wrapper"]/div/div/span[@class="rating-qualifier"]//text()').extract_first().strip()
            comment = review.xpath('.//div[@class="review-wrapper"]/div[@class="review-content"]/p//text()').extract_first()
            have_pic = (1 if review.xpath('.//div[@class="review-wrapper"]//ul[@class="photo-box-grid clearfix js-content-expandable lightbox-media-parent"]') else 0)
            useful = (review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"useful")]/span[@class="count"]//text()').extract_first() if review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"useful")]/span[@class="count"]//text()') else 0)
            funny = (review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"funny")]/span[@class="count"]//text()').extract_first() if review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"funny")]/span[@class="count"]//text()') else 0)
            cool = (review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"cool")]/span[@class="count"]//text()').extract_first() if review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"cool")]/span[@class="count"]//text()') else 0)
            label = 1
            items = CommentItem()
            for field in items.fields:
                try:
                    items[field] = eval(field)
                except NameError:
                    self.logger.debug("Field not Defined" + field)
            yield items

        # 判断是否还有下一页
        next = bool(response.xpath('//span[@class="pagination-label responsive-hidden-small pagination-links_anchor"]').extract_first())
        if next:
            url = response.xpath('//a[@class="u-decoration-none next pagination-links_anchor"]//@href').extract_first()
            yield Request(url=url,meta={'restaurant':restaurant,},callback=self.parse_comment,dont_filter=True)

        # 查找unrecommended评论
        if response.xpath('//div[@class="not-recommended ysection"]/a[@class="subtle-text inline-block js-expander-link"]//@href'):
            unrec_url = response.xpath('//div[@class="not-recommended ysection"]/a[@class="subtle-text inline-block js-expander-link"]//@href').extract_first()
            un_url = self.base_url+str(unrec_url)
            print("unrecommend review"+un_url)
            yield Request(url=un_url, meta={'restaurant':restaurant, 'rest_url':rest_url},callback=self.parse_unrecomment, dont_filter=True)

    def parse_user(self,response):
        print('crawl good user')
        user_name = response.xpath('//div[@class="user-profile_info arrange_unit"]/h1//text()').extract_first()
        user_id = response.meta['user_id']
        user_url = response.url
        user_location = response.xpath('//div[@class="user-profile_info arrange_unit"]/h3/text()').extract_first()
        friends = response.xpath('//div[@class="user-profile_info arrange_unit"]/div[@class="clearfix"]/ul/li[@class="friend-count"]/strong//text()').extract_first()
        reviews = response.xpath('//div[@class="user-profile_info arrange_unit"]/div[@class="clearfix"]/ul/li[@class="review-count"]/strong//text()').extract_first()
        photos = response.xpath('//div[@class="user-profile_info arrange_unit"]/div[@class="clearfix"]/ul/li[@class="photo-count"]/strong//text()').extract_first()
        hasphoto = bool(response.xpath('//div[@class="photo-slideshow_image"]/a/img//@srcset').extract_first()!=None)
        if hasphoto:
            user_avatar = True
        else:
            user_avatar = False
        countFive = response.css('.histogram_count::text').extract()[0]
        countFour = response.css('.histogram_count::text').extract()[1]
        countThree = response.css('.histogram_count::text').extract()[2]
        countTwo = response.css('.histogram_count::text').extract()[3]
        countOne= response.css('.histogram_count::text').extract()[4]
        user_useful = 0
        user_funny = 0
        user_cool = 0
        for i in response.xpath('//ul[@class="ylist ylist--condensed"]/li').extract():
            if 'Useful' in str(i):
                user_useful = re.findall('<strong>(.*?)</strong>', str(i))[0]
            if 'Funny' in str(i):
                user_funny = re.findall('<strong>(.*?)</strong>', str(i))[0]
            if 'Cool' in str(i):
                user_cool = re.findall('<strong>(.*?)</strong>', str(i))[0]

        lastDate = response.xpath('//div[@class="review-content"]/div[@class="review-content"]/div/span//text()').extract_first()
        itemUser = UserItem()
        for field in itemUser.fields:
            try:
                itemUser[field] = eval(field)
            except NameError:
                self.logger.debug("Field not Defined" + field)
        yield itemUser


    # 爬不好的评论
    def parse_unrecomment(self, response):
        reviews = response.xpath('//div[@class="ysection not-recommended-reviews review-list-wide"]/ul[@class="ylist ylist-bordered reviews"]/li/div[@class="review review--with-sidebar"]')
        restaurant = response.meta['restaurant']
        rest_url = response.meta['rest_url']
        for review in reviews:
            restaurant = restaurant
            rest_url = rest_url
            # review-sidebar
            isphoto = bool(review.xpath('.//div[@class="review-sidebar"]/div/div[@class="ypassport media-block"]/div[@class="media-avatar responsive-photo-box"]/div/img//@srcset').extract_first())
            if isphoto:
                user_avatar = True
            else:
                user_avatar = False
            user_name = review.xpath('.//div[@class="review-sidebar"]/div/div/div[@class="media-story"]/ul[@class="user-passport-info"]/li[@class="user-name"]/span//text()').extract_first()
            print('crawl bad user')
            user_id = "data-hovercard-id:"+str(review.xpath('.//@data-hovercard-id').extract_first())
            review_id = review.xpath('.//@data-review-id').extract_first()
            friends = review.xpath('.//div[@class="review-sidebar"]/div/div/div[@class="media-story"]/ul[@class="user-passport-stats"]/li[@class="friend-count responsive-small-display-inline-block"]/b//text()').extract_first()
            reviews = review.xpath('.//div[@class="review-sidebar"]/div/div/div[@class="media-story"]/ul[@class="user-passport-stats"]/li[@class="review-count responsive-small-display-inline-block"]/b//text()').extract_first()
            photos = (response.xpath('//div[@class="user-profile_info arrange_unit"]/div[@class="clearfix"]/ul/li[@class="photo-count"]/strong//text()').extract_first() if response.xpath('//div[@class="user-profile_info arrange_unit"]/div[@class="clearfix"]/ul/li[@class="photo-count"]/strong//text()').extract_first() else 0)
            user_location = review.xpath('.//div[@class="review-sidebar"]/div/div/div[@class="media-story"]/ul[@class="user-passport-info"]/li[@class="user-location responsive-hidden-small"]/b//text()').extract_first()
            countFive = 0
            countFour = 0
            countThree = 0
            countTwo = 0
            countOne = 0
            lastDate = None
            user_useful = 0
            user_funny = 0
            user_cool = 0
            itemUser = UserItem()
            for field in itemUser.fields:
                try:
                    itemUser[field] = eval(field)
                except NameError:
                    self.logger.debug("Field not Defined" + field)
            yield itemUser

            # review-wrapper

            score = review.xpath('.//div[@class="review-wrapper"]/div/div/div[@class="biz-rating__stars"]/div//@title').re_first("(.*?) star rating")
            date = review.xpath('.//div[@class="review-wrapper"]/div/div/span[@class="rating-qualifier"]//text()').extract_first().strip()
            comment = review.xpath('.//div[@class="review-wrapper"]//p//text()').extract_first()
            have_pic = (1 if review.xpath('.//div[@class="review-wrapper"]/div/div/ul[@class="photo-box-grid clearfix js-content-expandable lightbox-media-parent"]') else 0)
            useful = (review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"useful")]/span[@class="count"]//text()').extract_first() if review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"useful")]/span[@class="count"]//text()') else 0)
            funny = (review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"funny")]/span[@class="count"]//text()').extract_first() if review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"funny")]/span[@class="count"]//text()') else 0)
            cool = (review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"cool")]/span[@class="count"]//text()').extract_first() if review.xpath('.//div[@class="review-wrapper"]/div[@class="review-footer clearfix"]/div/ul/li/a[contains(@rel,"cool")]/span[@class="count"]//text()') else 0)
            label = 0
            items = CommentItem()
            for field in items.fields:
                try:
                    items[field] = eval(field)
                except NameError:
                    self.logger.debug("Field not Defined" + field)
            yield items

        # 判断是否还有下一页
        next = bool(response.xpath('//span[@class="pagination-label responsive-hidden-small pagination-links_anchor"]').extract_first())
        if next:
            url = response.xpath('//a[@class="u-decoration-none next pagination-links_anchor"]//@href').extract_first()
            yield Request(url=url, meta={'restaurant':restaurant}, callback=self.parse_unrecomment,dont_filter=True)
