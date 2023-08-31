from mrjob.job import MRJob

class TestPartitioner(MRJob):

    def mapper(self, _, line):
        yield line,''

    def reducer(self, key, values):
        yield key, ';'.join(values)

    SORT_VALUES = True

    JOBCONF = {
      'mapreduce.map.output.key.field.separator':'.',
      'mapreduce.job.reduces':2,
      'mapreduce.partition.keypartitioner.options':'-k1,2'           
    }
if __name__ == '__main__':
    TestPartitioner.run()
