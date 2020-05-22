import apache_beam as beam 
from apache_beam.pipeline import PipelineOptions 

options = PipelineOptions()


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='Input for the pipeline',
                            default='./data/')
        parser.add_argument('--output',
                            help='Output for the pipeline',
                            default='./output/')

class Split(beam.DoFn):
    def process(self,element):
        Date,Open,High,Low,Close,Volume = element.split(',')
        try:
            return [{
                'Open':float(Open),
                'Close':float(Close)
            }]
        except:
            return [{
                'Open':0.0,
                'Close':0.0
            }]


class CollectOpen(beam.DoFn):
    def process(self, element):
        result = [(1, element['Open'])]
        return result

class CollectClose(beam.DoFn):
    def process(self, element):
        # Returns a list of tuples containing the 1 key and Close value
        result = [(1, element['Close'])]
        return result




# setting input and output files
input_filename = "./data/sp500.csv"
output_filename = "./output/result.txt"



with beam.Pipeline(options=options)  as p:
    csv_lines = (
        p | beam.io.ReadFromText(input_filename, skip_header_lines=1) | beam.ParDo(Split())
    )
    # calculate the mean for Open values
    mean_open = (
        csv_lines | beam.ParDo(CollectOpen()) |
        "Grouping keys Open" >> beam.GroupByKey() |
        "Calculating mean for Open" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
            )
        )

    mean_close =(
        csv_lines | beam.ParDo(CollectClose()) |
        "Grouping keys Close" >> beam.GroupByKey() |
        "Calculating mean for  Close" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        )

    )

    # writing results to file
    output= ( 
        { 
            'Mean Open': mean_open,
            'Mean Close': mean_close 
        } | 
        beam.CoGroupByKey() | 
        beam.io.WriteToText(output_filename)
    )