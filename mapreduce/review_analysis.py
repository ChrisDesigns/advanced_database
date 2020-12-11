#!/usr/local/bin/python3.8

import json
from mapreduce import MapReduce

class ReviewAnalysis(MapReduce):

    def mapper(self, _, review):
        line = review["text"]
        for word in line.split(" "):
            yield (word.strip(),1)

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

# input = [
#     "this is an example of this line",
#     "this is an example of some example text",
#     "this is another example",
#     "and this is some more text and text and text"
#     ]

with open("reviews.json","r") as f:
    lines = f.readlines()
    reviews = [json.loads(line) for line in lines]

output = ReviewAnalysis().run(reviews)
max_key = ""
max_value = 0

for key, value in output:
    # print(key, " -- ", value)
    if (value > max_value) and (len(key) > 6):
        max_key = key
        max_value = value

print("Most common word", max_key, max_value)