from mrjob.job import MRJob
import re

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())
        for word in words:
            if len(word)>0 and word.isalpha():
                yield word[0], 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()
