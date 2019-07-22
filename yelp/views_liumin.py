from django.shortcuts import render
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql.functions import *
from pyspark.sql.types import *
from preprocess import *
#import pymongo
#from run_scrapy import scrapy
#from read_data import *
#import requests
#from django.http import JsonResponse
#from scrapyd_api import ScrapydAPI
#from sparksql import *

spark = SparkSession.builder \
        .master('yarn') \
        .appName("Yelp Online Testing") \
        .getOrCreate()

lda_model = PipelineModel.load('hdfs:///project/small_data/lda_model_10')
lr_model = LogisticRegressionModel.load('hdfs:///project/small_data/lr-model-10')

def home_view(request):    
    print('in home page')
    return render(request, "home.html")


def review_list_view(request):
    business_url = ''
    
    #connect scrapyd service
    #scrapyd = ScrapydAPI('http://localhost:6800')

    if request.method == "POST":
        business_url = request.POST['business_url']
        print(business_url)
     #   domain = urlparse(business_url).netloc
        #unique_id = str(uuid4())#create a unique ID
      #  task = scrapyd.schedule('Yelp','yelp',url=business_url,domain=domain)
       # print(JsonResponse({'task_id':task,'status':'started'})

    
    #myclient = pymongo.MongoClient('mongodb://localhost:27017')
    #db = myclient.url
    #collection = db.url_table
    #dic = {'url': business_url}
    #myclient.close()

    #scrapy()

    #[review, user, business] = read_data()
    #print(review)
    #print(user)
    #print(business)
    #review.head()
    #user.head()
    #business.head()
    
    # reviews
    raw_review_df = load_review(spark, 'hdfs:///project/small_data/small_review.csv')
    raw_review_df = raw_review_df.limit(50)
    raw_review_df = raw_review_df.fillna(0)

    # preprocessing review
    review_df = lda_model.transform(raw_review_df)
    review_df = pre_review(review_df)
    raw_review_df = review_df.select('rid', 'uid', 'rate', 'review_date', 'text', 'useful', 'funny', 'cool', 'wordCount')
    review_df = review_df.drop('text')

    # users
    raw_user_df = load_user(spark, 'hdfs:///project/small_data/small_user.csv')
    user_df = pre_user(raw_user_df)
    raw_user_df = user_df.select('uid', 'u_useful', 'u_funny', 'u_cool', 'friends', 'user_review_num', 'photo_num', 'isActive')

    # business
    raw_business_df = load_business(spark, 'hdfs:///project/small_data/small_business.csv')
    business_df = pre_business(raw_business_df)

    # combile all features
    full_df = get_fullset(review_df, user_df, business_df)

    # prediction
    prediction = lr_model.transform(full_df)

    # select useful columns
    prediction =  prediction.select('rid', 'prediction')   
    
    business_row = raw_business_df.collect()[0]
    business = {
        'url': business_url,
        'category': business_row['category'],
        'bus_location': business_row['bus_location'],
        'total_reviews': business_row['total_reviews'],
        'score': business_row['score']
    }

    result_df = raw_review_df.join(prediction, 'rid').join(raw_user_df, 'uid')
    result_df = result_df.drop('uid').drop('rid')

    result_list = []
    unrecommend = []

    for row in result_df.collect():
        item = row.asDict()
        
        if item['prediction'] == 1.0:
            result_list.append(item)
        else:
            if item['wordCount'] <= 20:
                item['reason'] = 'Low Quality!'
            elif item['isActive'] == 0:
                item['reason'] = 'Reviewer is not active!'
            else:
                item['reason'] = 'Fake Review!'
            
            unrecommend.append(item)



   # recommend_df = result_df.filter('prediction == 1.0')
   # recommend_df = recommend_df.drop('wordCount')

    #unrecommend_df = result_df.filter('prediction == 0.0')

    #for row in recommend_df.collect():
     #   result_list.append(row.asDict())
    
    #def reason(wordCount, isActive):
     #   if wordCount <= 20:
      #      return 'Low Quality!'
      #  if isActive == 0:
       #     return 'Reviewer is not Active!'
        
       # return 'Fake Review'
    
   # reason_udf = udf(lambda w, a: reason(w, a), StringType())
   # unrecommend_df = unrecommend_df.withColumn('reason', reason_udf('wordCount', 'isActive'))
   # unrecommend_df = unrecommend_df.drop('wordCount').drop('isActive')

   # unrecommend = []
   # for row in unrecommend_df.collect():
    #    unrecommend.append(row.asDict())

    context = {
        'business': business,
        'result': result_list,
        'unrecommend': unrecommend
    }
      
    return render(request, "review_list.html", context)

