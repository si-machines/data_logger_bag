#!/usr/bin/env python
# Script to start loading data into pytables and convert into meaningful features
import roslib; roslib.load_manifest("data_logger_bag")
import rospy
import sys
import tables
import numpy as np
import cPickle
from collections import defaultdict
import matplotlib.pyplot as plt


def extract_data(one_run):
    '''
    Pulls out the data we want from a single run
    '''

    # Create a dictionary to store all of the run
    store_data = defaultdict(dict)

    # pull out the topics
    data_topics = [_v for _v in one_run._v_groups]

    for topic in data_topics:

        # Pull out the data
        topic_data = eval('one_run.'+topic)

        # Pull out fields for the topic
        fields = [_v for _v in topic_data._v_leaves]
       
        for field in fields:
            store_data[topic][field] = eval('topic_data.'+field+'[:]')

    return store_data

def load_data(input_filename, output_filename, save_to_file):

    if not input_filename.endswith(".h5"):
        raise Exception("Input file is %s \nPlease pass in a hdf5 data file" % input_filename)

    if save_to_file: 
        if not output_filename.endswith(".pkl"):
            output_filename = output_filename + '.pkl'

    # Load the data from an h5 file
    all_data = tables.openFile(input_filename)

    # Store number of runs
    num_runs = 0

    # Pull pointers to only the file heads of the data structure
    all_runs_root = [_g for _g in all_data.walkGroups("/") if _g._v_depth == 1]

    # Create a dictionary to store all of the data 
    stored_data = dict()

    # For each file extract the segments and data
    for _objectRun in all_runs_root:
        num_runs += 1
        stored_data[_objectRun._v_name] = extract_data(_objectRun) 

    # if we want to save to file
    if (save_to_file):
        file_ptr = open(output_filename, "w")
        cPickle.dump(stored_data, file_ptr, cPickle.HIGHEST_PROTOCOL)
        file_ptr.close()

    return stored_data

def main():

    if len(sys.argv) == 2:
        input_file = sys.argv[1]
        data = load_data(input_file, '', False)

        import pdb; pdb.set_trace()
    elif len(sys.argv) == 3:
        input_file = sys.argv[1]
        output_file = sys.argv[2]

        load_data(input_file, output_file, True)
    else:
        raise Exception("Usage: %s [input_file] [output_file]", sys.argv[0])

if __name__== "__main__":
    main()

