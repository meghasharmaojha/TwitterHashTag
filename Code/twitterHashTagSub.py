from __future__ import print_function

import json
import sys
import time

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext

from collections import namedtuple

TagCount = namedtuple('TagCount',("tag", "count"))

def extractTweetText(tweetJSON):
    if('text' in tweetJSON):
        return tweetJSON['text']
    else:
        return ''
    

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage trendingtages.py <hostname> <port>", file = sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName = "StreamingTwitterHashtags")
    windowIntervalSecs = 10
    ssc = StreamingContext(sc, windowIntervalSecs)
    
    tweetsDSStream = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    (tweetsDSStream
        .map(lambda tweet: extractTweetText(json.loads(tweet)))
        .flatMap(lambda text: text.split(" "))
        .filter (lambda word: word.startswith("#"))
        .map(lambda word: (word.lower(), 1))
        .reduceByKey(lambda a, b: a+b)
        .map(lambda rec: TagCount(rec[0], rec[1]))
        .foreachRDD(lambda rdd: rdd.toDF().registerTempTable("tag_counts"))
    )
    
    ssc.start()
    
    sqlContext = SQLContext(sc)
    count = 0
    while(count<100):
        time.sleep(15)
        count = count + 1
        top_10_hash_tags = sqlContext.sql('select tag, count from tag_counts order by count desc limit 10')
        for row in top_10_hash_tags.collect():
            print(row.tag, row['count'])
        print('-------')
        
    ssc.awaitTermination()