import apache_beam as beam 
import os
input__dir = './data_groupby'
output_dir = './output'


def retTuple(element):
    thisTuple = element.split(',')
    return (thisTuple[0],thisTuple[1:])

p1 = beam.Pipeline()

dep_rows = (

    p1
    |"Read Dept Data" >> beam.io.ReadFromText(os.path.join(input__dir,'dept_data.txt'))
    |"Pair Each Employee with Key" >> beam.Map(retTuple)
)

loc_rows = (

    p1
    |"Read Location Data" >> beam.io.ReadFromText(os.path.join(input__dir,'location.txt'))
    |"Pair Each Loc with Key" >> beam.Map(retTuple)

)


results = (

    {
        'dept_data':dep_rows,
        'loc_data':loc_rows
    }

    |beam.CoGroupByKey()
    |"Write Results" >> beam.io.WriteToText(os.path.join(output_dir,'groupby.txt'))
)

p1.run()
