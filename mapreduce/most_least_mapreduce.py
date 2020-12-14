from mapreduce import MapReduce
from string import punctuation
import math
import bisect

class MostLeast(MapReduce):

    def mapper(self, _, line):
        for word in line.split(" "):
            # basic stripping of text and ending punctuation
            wordStripped = word.strip().rstrip(punctuation)
            # only non-captialized words
            if (wordStripped.islower()):
                yield (wordStripped,1)

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


with open("alice.txt","r") as f:
    input = f.readlines()

output = MostLeast().run(input)

most_common = []
most_common_sums = []
smallest_most_value = 0

least_common = []
least_common_sums = []
largest_least_value = math.inf


for key, value in output:
    # logic for most common
    if (value >= smallest_most_value):
        # get the postion of where the value would be inorder
        position = bisect.bisect(most_common_sums, value)
        # and insert it into the 10 most common list in order
        most_common.insert(position, key)
        most_common_sums.insert(position, value)
        # if the list is over the 10 elements remove the smallest
        if (len(most_common) == 11):
            most_common.pop(0)
            most_common_sums.pop(0)

    # logic for least common
    if (value <= largest_least_value):
        # get the postion of where the value would be inorder
        position = bisect.bisect(least_common_sums, value)
        least_common.insert(position, key)
        least_common_sums.insert(position, value)
        # if the list is over the 10 elements remove the largest
        if (len(least_common) == 11):
            least_common.pop(10)
            least_common_sums.pop(10)



print("Top 10 most common words", most_common, most_common_sums)
print("Top 10 least common words", least_common, least_common_sums)