import apache_beam as beam


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return (0.0,0)
    def add_input(self,sum_count,input):
        (sum,count)=sum_count
        return sum+input,count+1
    
    def merge_accumulators(self,accumulators):
        ind_sums,ind_counts = zip(*accumulators)
        return sum(ind_sums),sum(ind_counts)

    def extract_output(self,sum_count):
        (sum,count)=sum_count
        return sum/count if count else "NaN"

p1 = beam.Pipeline()



average = (


    p1 
    |beam.Create([12,23,45,13,21,42,31,25,77,28,55,41,91])
    |beam.CombineGlobally(AverageFn())
    |beam.io.WriteToText('./output/combiner.txt')
)

p1.run()
