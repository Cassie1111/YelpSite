from scrapy import Item,Field


class RestaurantItem(Item):
    # define the fields for your item here like:
    table_name = 'restaurantTable'
    restaurant = Field()
    rest_url = Field()
    score = Field()
    total_reviews = Field()
    category = Field()
    address = Field()
    phone = Field()

class CommentItem(Item):
    table_name = 'commentTable'
    restaurant = Field()
    rest_url = Field()
    review_id = Field()
    user_id = Field()
    score = Field()
    date = Field()
    comment = Field()
    useful = Field()
    funny = Field()
    cool = Field()
    label = Field()
    have_pic = Field()

class UserItem(Item):
    table_name = 'userTable'
    user_avatar = Field()
    user_url = Field()
    user_id = Field()
    user_name = Field()
    data_hovercard_id = Field()
    user_location = Field()
    friends = Field()
    reviews = Field()
    photos = Field()
    countFive = Field()
    countFour = Field()
    countThree = Field()
    countTwo = Field()
    countOne = Field()
    lastDate = Field()
    user_useful = Field()
    user_funny = Field()
    user_cool = Field()

