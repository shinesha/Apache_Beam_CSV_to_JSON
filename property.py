# pytype: skip-file
import pandas as pd
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CsvToJsonConvertion(beam.DoFn):

    def __init__(self, inputFilePath, outputFilePath):
        self.inputFilePath = inputFilePath
        self.outputFilePath = outputFilePath


    def process(self, something):
        df = pd.read_csv(self.inputFilePath)
        df.to_json(self.outputFilePath)

def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--inputFilePath',
        dest='input',
        default='C:/Users/Jodre/OneDrive/Desktop/App-2021.csv',
        help='Input file to process.')

    parser.add_argument(
        '--outputFilePath',
        dest='output',
        # CHANGE 1/6: The Google Cloud Storage path is required
        # for outputting the results.
        default='C:/Users/Jodre/OneDrive/Desktop/Trading/Output.txt',
        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)


    pipeline_args.extend([
        # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--runner=DirectRunner',
        # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
        # run your pipeline on the Google Cloud Dataflow Service.
        '--project=SET_YOUR_PROJECT_ID_HERE',
        # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
        # is required in order to run your pipeline on the Google Cloud
        # Dataflow Service.
        '--region=SET_REGION_HERE',
        # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
        # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
        '--job_name=your-wordcount-job',
    ])




    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the text file[pattern] into a PCollection.

        lines = pipeline | ReadFromText(known_args.input)

        (lines
         | 'Start' >> beam.Create([None])
         | 'Convertion CSV to JSON' >> beam.ParDo(
                    CsvToJsonConvertion(lines, 'C:/Users/Jodre/OneDrive/Desktop/Trading/Output.txt'))
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
