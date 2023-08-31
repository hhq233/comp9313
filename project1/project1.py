from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import math


class Mr_project1(MRJob):
    def mapper_init(self):
        self.tmp = {}
        
    def mapper(self, _, line):
        strings = line.split(',')
        year = strings[0][:4]
        words = strings[1].split()
        for word in words :
            key = word+' '+year
            self.tmp[key] = self.tmp.get(key, 0) + 1

    def mapper_final(self):
        for k, v in self.tmp.items():
            yield k, v

            
    def reducer(self, key, values):
        strings = key.split(' ')
        key1 = strings[0]
        yield (key1 + ' *'), 1
        yield key, sum(values)
        
         
    def reducer_special_key(self, key, values):
        yield key, sum(values)
                   
    def reducer_get_svalue(self):
        self.num_key = 0

          
    def reducer_get_weight(self, key, values):
        strings = key.split(' ')
        word = strings[0]
        year = strings[1]
        tf = 0
        for value in values:
            tf = value
        if year == '*':
            self.num_key = tf
        else :
            Total_years = int(jobconf_from_env('myjob.settings.years'))
            if self.num_key == 0:
                yield self.num_key, 0
            weight = math.log10(Total_years/self.num_key)*tf       
            yield word, (year + ',' +str(weight))
            
    def mapper_final_1(self):
        self.tmpword = {}
    def mapper_final_2(self, key, values):
        if self.tmpword.get(key) == None :
            self.tmpword[key] = [values]
        else:
            self.tmpword[key].append(values)
    def mapper_final_3(self):
        for k, v in self.tmpword.items():
            yield k, ';'.join(v)
        
        
    
    SORT_VALUES = True

    JOBCONF = {
      'mapreduce.map.output.key.field.separator':' ',
      'mapreduce.job.reduces':2,
      'mapreduce.partition.keypartitioner.options':'-k1,1', 
      'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
      'mapreduce.partition.keycomparator.options':'-k1,2'          
    }      
    def steps(self):
        return [MRStep(mapper_init = self.mapper_init,
                       mapper = self.mapper,
                       mapper_final = self.mapper_final,
                       reducer = self.reducer
                ),
                MRStep(reducer = self.reducer_special_key),   
                MRStep(
                       reducer_init = self.reducer_get_svalue,
                       reducer = self.reducer_get_weight,
                ),
                MRStep(
                       mapper_init = self.mapper_final_1,
                       mapper = self.mapper_final_2,
                       mapper_final = self.mapper_final_3,
                )
        ]

if __name__ == '__main__':
    Mr_project1.run()
