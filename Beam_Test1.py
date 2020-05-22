import apache_beam as beam 
import os


# setting input and output files
input_filename = "./data/dept_data.txt"
output_folder = "./output"


class SplitRow(beam.DoFn):
    def process(self,element):
        return [element.split(',')]

## def SplitRow(element):
##     return element.split(',')


p1 = beam.Pipeline()

input_collection=(
    p1
    |beam.io.ReadFromText(input_filename)
    # |beam.Map(SplitRow)
    |beam.ParDo(SplitRow)
)
accounts_count = (

    input_collection
    |"Accounts filter" >> beam.Filter(lambda x:x[3]=='Accounts')
    |"Accounts Map tupple" >> beam.Map(lambda x:(x[1],1))
    |"Accounts Combine" >> beam.CombinePerKey(sum)
    #   |"Accounts Write" >> beam.io.WriteToText(os.path.join(output_folder,'accounts.txt')
    
)


hr_count = (

    input_collection
    |"HR filter" >> beam.Filter(lambda x:x[3]=='HR')
    |"HR Map tupple" >>beam.Map(lambda x:(x[1],1))
    |"HR Combine" >> beam.CombinePerKey(sum)
#    |"HR Write" >> beam.io.WriteToText(os.path.join(output_folder,'hr.txt')
    
)


both_count = (
    (accounts_count,hr_count)
    |beam.Flatten()
    |"Both Write" >> beam.io.WriteToText(os.path.join(output_folder,'both_pardo.txt')
)
)
p1.run()