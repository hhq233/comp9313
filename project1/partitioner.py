from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import math


class Mr_project1(MRJob):
    def mapper(self, _, line):
        strings = line.split(',')
        year = strings[0][:4]
        words = strings[1].split()
        for word in words:
            if len(word):
                yield (word+','+year), 1
         
    def combiner_special_key(self, key, values):
        strings = key.split(',')
        word = strings[0]
        yield word + ',*',1
        yield key, values
        
        
                   
    def reducer_get_svalue(self):
        self.num_key = 1

          
    def reducer_get_weight(self, key, values):
        strings = key.split(',')
        word = strings[0]
        year = strings[1]
        tf = values
        if year == '*':
            self.num_key = tf
            yield key, values
        else :
            Total_years = int(jobconf_from_env('myjob.settings.years'))
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
        
        
    
    
    def steps(self):
        return [MRStep(
                       mapper = self.mapper,),
                MRStep (mapper = self.combiner_special_key,),
                MRStep (  
                       mapper_init = self.reducer_get_svalue,
                       mapper = self.reducer_get_weight,
                ),
      #          MRStep(
      #                 mapper_init = self.mapper_final_1,
      #                 mapper = self.mapper_final_2,
      #                 mapper_final = self.mapper_final_3,
      #          )
        ]
        
    SORT_VALUES = True
            
    JOBCONF = {
        'mapreduce.map.output.key.field.separator':',',
        'mapreduce.job.reduces':2,
        'mapreduce.partition.keypartitioner.options':'-k1,1',
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
      'mapreduce.partition.keycomparator.options':'-k1,1n' 
    }   

if __name__ == '__main__':
    Mr_project1.run()
