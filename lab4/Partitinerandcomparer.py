from mrjob.job import MRJob

class PartitionerAndComparator(MRJob):

    def mapper(self, _, line):
        yield line,''

    def reducer(self, key, values):
        yield key, ';'.join(values)

    SORT_VALUES = True

    JOBCONF = {
      'mapreduce.map.output.key.field.separator':'.',
      'mapreduce.job.reduces':2,
      'mapreduce.partition.keypartitioner.options':'-k1,2', 
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
      'mapreduce.partition.keycomparator.options':'-k1,2 -k4,4nr' 
    }
if __name__ == '__main__':
    PartitionerAndComparator.run()
