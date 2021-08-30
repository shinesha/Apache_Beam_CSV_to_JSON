import argparse
import logging
import re
import pandas as pd
import json
from apache_beam.dataframe.io import read_csv
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# defining custom arguments
class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='Input for the pipeline',
                            default='C:/Users/Jodre/OneDrive/Desktop/App-2021.csv')
        parser.add_argument('--output',
                            help='Output for the pipeline',
                            default='C:/Users/Jodre/OneDrive/Desktop/Trading/Output.txt')


class Split(beam.DoFn):
    def process(self, element):
        price, dateTime, postCode, PAON, SAON, street, town, county = element.split(",")
        return [{
            'Price': int(price),
            'DateTime': dateTime,
            'PostCode': postCode,
            'PAON': PAON,
            'SAON': SAON,
            'Street': street,
            'Town': town,
            'County': county
        }]


# setting input and output files
input_filename = "C:/Users/Jodre/OneDrive/Desktop/App-2021.csv"
output_filename = "C:/Users/Jodre/OneDrive/Desktop/Trading/Output.csv"

# instantiate the pipeline
options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    # reading the csv and splitting lines by elements we want to retain
    df = p | read_csv(input_filename)
    #agg = df['Price']
    # for row in df:




    df.to_csv(output_filename)
    #
    # #reader = csv.DictReader(csvfile, fieldnames)
    # #for row in reader:
    #     json.dump(row, output_filename)
    #     df.write('\n')



    #print((tuple(df1)))
# # calculate the mean for Open values
# mean_open = (
#     csv_lines | beam.ParDo(CollectOpen()) |
#     "Grouping keys Open" >> beam.GroupByKey() |
#     "Calculating mean for Open" >> beam.CombineValues(
#         beam.combiners.MeanCombineFn()
#         )
#     )
#
# # calculate the mean for Close values
# mean_close = (
#     csv_lines | beam.ParDo(CollectClose()) |
#     "Grouping keys Close" >> beam.GroupByKey() |
#     "Calculating mean for Close" >> beam.CombineValues(
#         beam.combiners.MeanCombineFn()
#         )
#     )

# writing results to file
# output= (
#     {
#         'Mean Open': mean_open,
#         'Mean Close': mean_close
#     } |
#     beam.CoGroupByKey() |
#     beam.io.WriteToText(output_filename)
# )
