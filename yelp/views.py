from django.shortcuts import render
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST, require_http_methods
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegressionModel
from preprocess import *
from read_data import *
from scrapyd_api import ScrapydAPI
from urllib.parse import urlparse

from django.http import JsonResponse
#spark = SparkSession.builder \
#        .master('yarn') \
#        .appName("Yelp Model Testing") \
#        .getOrCreate()

# lda model
#lda_model = PipelineModel.load('hdfs:///project/small_data/lda_model')

# lr model
#lr_model = LogisticRegressionModel.load('hdfs:///project/small_data/lr-model')

# Create your views here.
scrapyd = ScrapydAPI('http://localhost:6800')
def home_view(request):    
    return render(request, "home.html")


def is_valid_url(url):
    validate = URLValidator()
    try:
        validate(url)  # check if url format is valid
    except ValidationError:
        return False

    return True


@csrf_exempt
@require_http_methods(['POST', 'GET'])  # only get and post
def crawl(request):
    if request.method == 'POST':
        url = request.POST.get('url', None)  # take url comes from client. (From an input may be?)

        if not url:
            return JsonResponse({'error': 'Missing  args'})

        if not is_valid_url(url):
            return JsonResponse({'error': 'URL is invalid'})

        domain = urlparse(url).netloc  # parse the url and extract the domain
        task = scrapyd.schedule('default', 'scrapy_yelp',
                            settings="", url=url, domain=domain)

        return JsonResponse({'task_id': task, 'status': 'started'})
    elif request.method == 'GET':
        task_id = request.GET.get('task_id', None)

        if not task_id:
            return JsonResponse({'error': 'Missing args'})

        status = scrapyd.job_status('default', task_id)

        return JsonResponse({'task_id': task_id, 'status': status})


@require_http_methods(['GET'])
def review_list_view(request):
    business_url = ''

    if request.method == "GET":
        business_url = request.GET['url']

        [review, user, business] = read_data()

        print(type(review))
        print(type(user))
        print(type(business))

        return JsonResponse({
            'review': list(review),
            'user': list(review),
            'business': list(business),
        })

        context = {
            'business': {
                'url': business_url,
            },
            'result': [],
            'unrecommend': []
        }
    
        # reviews
        if status == 'finished': 
            return render(request, "review_list.html", context)

        return render(request, "review_list.html", context)
