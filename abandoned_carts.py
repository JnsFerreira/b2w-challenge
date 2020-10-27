#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import

# Built-in libs
import argparse
import json
import logging
import datetime
from builtins import object

# Apache Beam libs
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class JsonCoder(object):
  """A JSON coder interpreting each line as a JSON string."""
  def encode(self, file):
    return json.dumps(file)

  def decode(self, file):
    return json.loads(file)

def find_abandoned_carts(record):
    """
    Description

    Args
      record: Each customer of input file and their transaction history

    Returns
      A generator with all abandoned cart events
    """

    # If you want to change the timeout time (In minutes), just alter this variable
    session_timeout = 10

    # All interactions of the currently user
    interactions = record[1]
    
    # Checks the time delta between interaction
    for i in range(0, len(interactions)-1):

        last_interaction = datetime.datetime.strptime(interactions[i]['timestamp'],'%Y-%m-%d %H:%M:%S')
        interaction = datetime.datetime.strptime(interactions[i+1]['timestamp'], '%Y-%m-%d %H:%M:%S')
        
        # Caculate the time delta
        interval = (interaction - last_interaction).total_seconds() / 60.0

        # If the interval is greater than session timeout, stores the transaction
        if(interval >= session_timeout):            
            yield interactions[i+1]
    
    # Checking the last interaction! Stores the transaction if page is different from 'checkout'
    if(interactions[-1]['page'] != 'checkout'):
        yield interactions[-1]


def run(argv=None):
    """
    Pipeline for filter abandoned carts events based on bussiness rules:
    
      - We define as a session, a 10-minute window where the client interacts (views pages) on our website
      - Session time is renewed with each new interaction
      - The standard page flow for an order is: product -> basket -> checkout
    """
    # Arguments 
    parser = argparse.ArgumentParser()

    # Input directory
    # default: input/page-views.json
    parser.add_argument('--input', 
                        required=False, 
                        help='Input file to process.',
                        default='input/page-views.json')

    # Output directory
    # default: output/abandoned_carts
    parser.add_argument('--output', 
                        required=False, 
                        help='Output file to write results to.',
                        default='output/abandoned_carts')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    
    #Branched Pipeline
    pipe = beam.Pipeline(options=pipeline_options)

    # Reads and encondes the file as a initial PCollection
    input_data = (
                  pipe
                  | "Reads json file" >> ReadFromText(known_args.input, coder=JsonCoder())
                 )

    # Filters abandoned carts
    filtered = (
                input_data
                | "Creating a key:value structure" >> beam.Map(lambda elem: (elem['customer'],elem))
                | "Grouping by key" >> beam.GroupByKey()
                | "Mapping the results" >> beam.FlatMap(find_abandoned_carts)
               )

    # Writes the results to a json file 
    output = (
                filtered
                | "Encodes file to json" >> beam.Map(json.dumps)
                | "Writes to output file" >> WriteToText(known_args.output, '.json')
             )

    # Running the entire pipeline
    pipe.run()

# Auto start
if __name__ == '__main__':
  # Collecting info logs
  logging.getLogger().setLevel(logging.INFO)
  run()