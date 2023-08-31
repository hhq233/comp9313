from mrjob.job import MRJob
import re

class MRCoTremNSPair(MRJob):	
    def mapper(self, _, line):
        words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())
        for i in range(len(words)):
            if len(words[i]) == 0:
                continue
            for j in range(i+1, len(words)):
                if len(words[j]) == 0:
            	    continue
                yield (words[i],words[j]),1
                
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == '__main__':
    MRCoTremNSPair.run()
