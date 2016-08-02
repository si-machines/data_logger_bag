#! /usr/bin/python

# Copyright (c) 2016, Socially Intelligent Machines Lab 
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# 
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# 
# * Neither the name of data_logger_bag nor the names of its 
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Author: Vivian Chu, vchu@gatech.edu

'''
parse_bag_pytables.py

This is the main class that processes either standalone bag files or a directory
of bag files. It calls the TableBagData class to actually convert raw data into hdf5 format

It is designed to allow for raw topic writing (at the Hz it was recorded) or a subsampled
format that uses a specific topic to subsample the remaining the topics to. This guarantees
data alignment
'''

import rospy
import rosbag
import numpy
import sys
import os
import tables
import subprocess
import yaml
import glob
from collections import defaultdict
import numpy as np
from table_bag_data import TableBagData

class DataTableBagProcessor():

    def __init__(self):

        # Intialize the node
        rospy.init_node("bag_parser", anonymous=True)
        rospy.loginfo("Initializing bag parser")
      
        ###################################################################
        # Read from the parameter server/input to determine
        # what topics to process, whether to subsample, and input/outputs
        ###################################################################
 
        # Default topics to skip and subsample to
        default_skip_topics = "camera/depth_registered/image_raw camera/rgb/image_raw camera/rgb/camera_info camera/depth_registered/camera_info"
        default_trigger_topic = "C6_FSM_state"

        # read paramater
        self.skip_topics = rospy.get_param("~skip_topics", default_skip_topics)
        input_files = rospy.get_param("~input_files", "")
        output_file = rospy.get_param("~output_file", "converted_data.h5")

        # These need to be set if we want to subsample down to a topic
        # Otherwise by default nothing is aligned
        self.sub_sample = rospy.get_param("~sub_sample_flag", False)
        self.trigger_topic = rospy.get_param("~trigger_topic", default_trigger_topic)

        # Check if we have a flag for custom ordering
        self.custom_order = rospy.get_param("~custom_order", None)

        # Actually exits the system with the usage if input files are not provided
        if input_files == "":
            rospy.logerr("Usage: %s _input_files:=<input_files> \nOptional args:\n\n\
               _output_file:=<output_file.h5>\n\
               _skip_topics:=<ros topics to NOT parse>\n\
               _trigger_topic:=<what ros topic to downsample to>\n\
               _sub_sample_flag:=<True or False>\n\
               _custom_order:=<Token where run numbers fall after. e.g. Run>" % sys.argv[0])
            sys.exit()

        ###################################################################
        # Process input/output and subsample flags
        ###################################################################

        # Check if we passed in a directory and setup proper input/output files
        if os.path.isdir(input_files):
            self.input_filenames = input_files

            if 'converted_data.h5' in output_file: 
                # Setup output file names with directory format
                file_split = input_files.split(os.sep)
                self.output_filename = file_split[-1]+'.h5'
            else:
                self.output_filename = os.path.expanduser(output_file)
        else:
            # Expand wildcards if any
            if input_files.endswith('.bag'):
                self.input_filenames = [input_files]
            else:
                self.input_filenames = glob.glob(os.path.expanduser(input_files))
            self.output_filename = os.path.expanduser(output_file)

        # Check if we're subsampling the data
        if self.sub_sample:
            self.output_filename = self.output_filename.split('.')[0] + '_subsampled.h5'
            rospy.logwarn("WARNING: subsample topic is %s. Make sure this is your state..." % self.trigger_topic)

        rospy.loginfo("Output file name is: %s" % self.output_filename)

        ###################################################################
        # Some simple vars to init
        ###################################################################
        self.fileCounter = dict()
        self.fileCounter['playback'] = 0
        self.fileCounter['learning'] = 0
        self.bag_group = None
       
    def setup_h5(self):

        filters = tables.Filters(complevel=9)
        self.h5file = tables.openFile(self.output_filename, mode="w", title="Aggregated Exploration Data",
                                 filters = filters)

        rospy.loginfo("Initialized h5 output file")

    def get_subdirs(self, a_dir):
        '''
        Only gets the immediate subdirs
        '''
        return [name for name in os.listdir(a_dir)
                if os.path.isdir(os.path.join(a_dir, name))]

    def get_subfiles(self, a_dir):
        '''
        Only gets the immediate files 
        '''
        return [name for name in os.listdir(a_dir)
                if not os.path.isdir(os.path.join(a_dir, name))]

    def process_general(self):
        '''
        This is the main starting function that processes a directory or
        a set of files and calls the internal recursive process to create
        h5 groups
        '''
        # Check if it is a directory
        if not isinstance(self.input_filenames, list):
          
            # Check if group_name is empty
            root_name = os.path.split(self.input_filenames)[-1]
            group_name = self.h5file.createGroup("/", root_name) 

            # Actually calls the heavy duty function 
            self.process_all_recurse(self.input_filenames, group_name) 
            
        else:
            # Else just treat the input as a list of files
            # Currently assumes you will pass in the files in the order you want
            self.process_files(self.input_filenames)

    def process_all_recurse(self, filename, group_name, custom_order=None):
        '''
        Recursively goes through the directory and files and 
        creates the proper h5 groups and populates them
        '''
        dirs = self.get_subdirs(filename)
        files = self.get_subfiles(filename)

        # Write all files in the current directory
        if files:
            self.process_directory(filename, group_name)

        # If we have more directories, recurse down
        if dirs:
            for directory in dirs:
                foldername = os.path.split(directory)[-1]
                new_group = self.h5file.createGroup(group_name, foldername)
                self.process_all_recurse(os.path.join(filename,directory), new_group) 
             

    def process_directory(self, directory, group_name): 
        '''
        Internal function that takes a directory and converts it into a list
        of files to write to a specific bag group
        '''
 
        # Create the bag group to write to
        self.bag_group = group_name

        # for all files finally
        filenames = []
        filesdir = os.listdir(directory) 

        # Check if we have a custom sorting order
        # Assume that custom_order contains the string to split with the
        # first thing after the split the run number
        # Ex. 'playback_move_breadbox-Depth0Run5_successDemo.h5 - custom_order = 'Run'
        if self.custom_order is not None:
            # Pull out the run order according to the custom order token
            run_order = map(int,[x.split(self.custom_order)[1].split('_')[0] for x in filesdir])

            # Sort and reorg the filenames
            sorted_order = sorted(range(len(run_order)), key=lambda k: run_order[k]) 

            # Now sort!
            filesdir.sort(key=dict(zip(filesdir, run_order)).get)
        else:
            filesdir.sort() # puts in the order listed in the OS

        # Go through each file that ends in .bag and start to convert
        for f in filesdir:
            if f.endswith(".bag"):
                filename = os.path.join(directory, f)
                filenames.append(filename)

        # Now actually call the main function that processes the files
        self.process_files(filenames)

    def process_files(self, input_files):

        # Reset the count per directory
        self.fileCounter['playback'] = 0
        self.fileCounter['learning'] = 0

        for filename in input_files:

            # Go through each file
            self.process_bag(filename)
            if 'playback' in filename:
                self.bagData.write_pytables(filename, self.fileCounter['playback'], self.bag_group)
                self.fileCounter['playback'] +=1
            else:
                self.bagData.write_pytables(filename, self.fileCounter['learning'], self.bag_group)
                self.fileCounter['learning'] +=1

    def fix_bag(self, bag):
        """
        This is a custom fix for messages from rosjava. There is a minor bug that
        causes rosjava to not correctly write the message definitions to the bag file.
        
        This in turn causes the dynamic message loader genpy.dynamic (line 125) to
        not find the message and error out.
        """

        # Go through each of the rosbag messages and fix them
        # This is a manual hack to deal with certain message types that do not
        # exist correctly in rosjava
        for i in xrange(len(bag._connections)):
            connection = bag._connections[i]
            if 'c6_end_effect_pose' in connection.topic:
                bag._connections[i].msg_def = '# A representation of pose in free space, composed of postion and orientation. \nPoint position\nQuaternion orientation\n\n================================================================================\nMSG: geometry_msgs/Point\n# This contains the position of a point in free space\nfloat64 x\nfloat64 y\nfloat64 z\n\n================================================================================\nMSG: geometry_msgs/Quaternion\n# This represents an orientation in free space in quaternion form.\n\nfloat64 x\nfloat64 y\nfloat64 z\nfloat64 w\n\n'

        return bag

    def process_bag(self, filename):

        rospy.loginfo("Processing bag: %s" % filename)
        bag = rosbag.Bag(filename)

        # Create an data object to store
        self.bagData = TableBagData()
        self.bagData.h5file = self.h5file

        # Initialize variables
        self.all_topics = []
        #self.topic_types = dict()
        self.data_store = dict()
        self.bagData.topic_types = dict()

        # Open up the bag file and get info on it
        info_dict = yaml.load(subprocess.Popen(['rosbag', 'info', '--yaml', filename], stdout=subprocess.PIPE).communicate()[0])

        # Fix the bag file for rosjava incorrect message headers
        bag = self.fix_bag(bag)

        # Go through each of the topics and initialize variables
        for topics in info_dict['topics']:
            if not topics['topic'] in self.skip_topics:
                self.all_topics.append(topics['topic']) 
                self.bagData.topic_types[topics['topic']] = topics['type'] 

                # create dummy objects if we're subsampling
                if self.sub_sample:
                    self.data_store[topics['topic']] = (self.create_dummy_msg(topics['topic']), 0)

        # Go through the topics in the bag file
        for topic, msg, stamp in bag.read_messages(topics=self.all_topics):
            
            # Store off the topics not being downsampled
            self.data_store[topic] = (msg,stamp)

            # write off the values
            if self.sub_sample:
                if topic == self.trigger_topic:

                    # Go through all of the topics we stored off and write them to
                    # a dictionary
                    for topic_store in self.data_store:
                        if isinstance(self.data_store[topic_store], list):
                            self.bagData.process_msg(topic_store, self.data_store[topic_store])
                        else: 
                            self.bagData.process_msg(topic_store, (self.data_store[topic_store][0], stamp))

            else:
                self.bagData.process_msg(topic, (msg, stamp))

    def create_dummy_msg(self, topic):
        '''
        This function is only called during subsampling.  This takes care of the possiblity
        that certain messages have not been seen before the first "tick" of the trigger
        topic and populates the message with NaNs.  The loading of h5 takes care of
        processing the NaNs

        Note: This section has some hard-coded messages and topic sizes that
              might be effected drastically with changes in the robot or messages
        '''

        # Custom function that will process the messages depending on type
        msg_type = self.bagData.topic_types[topic] 
        obj = Object()
        obj._type = msg_type

        vec_obj = Object()
        vec_obj.x = float('nan')
        vec_obj.y = float('nan')
        vec_obj.z = float('nan')
        vec_obj._type = 'geometry_msgs/Vector3'

        if msg_type == 'geometry_msgs/Wrench':
            obj.force = vec_obj
            obj.torque = vec_obj
            return obj

        elif msg_type == 'geometry_msgs/Pose':
            point_obj = Object()
            point_obj._type = 'geometry_msgs/Pose'

            point_obj.position = Object()
            point_obj.position.x = float('nan')
            point_obj.position.y = float('nan')
            point_obj.position.z = float('nan')
            point_obj.position._type = 'geometry_msgs/Point'

            point_obj.orientation = Object()
            point_obj.orientation.x = float('nan')
            point_obj.orientation.y = float('nan')
            point_obj.orientation.z = float('nan')
            point_obj.orientation.w = float('nan')
            point_obj.orientation._type = 'geometry_msgs/Quaternion'

            return point_obj

        elif msg_type == 'std_msgs/Int8':
            obj.data = float('nan')
            return obj

        # This is a simple hack the determine how many states
        # Be aware that joint_states might not actually have 57 states
        elif msg_type == 'sensor_msgs/JointState':
            if topic == 'joint_states':
                num_joint = 57
            elif topic == 'zlift_state':
                num_joint = 1
            else:
                num_joint = 36

            obj.name = ['no_joints'] * num_joint
            obj.position = [float('nan')]* num_joint
            obj.velocity = [float('nan')]* num_joint
            obj.effort = [float('nan')]* num_joint
            return obj
            
        elif msg_type == 'rospcseg/ClusterArrayV0':
            obj_clusters = Object()

            obj_clusters.centroid = vec_obj
            obj_clusters.angle = float('nan')

            color = Object()
            color.r = float('nan')
            color.g = float('nan')
            color.b = float('nan')
            color.a = float('nan')
            color._type = 'std_msgs/ColorRGBA'
            obj_clusters.rgba_color =  color

            obj_clusters.volume2 = float('nan')
            obj_clusters.bb_volume = float('nan')
            obj_clusters.bb_area = float('nan')

            obj_clusters.aligned_bounding_box_size = vec_obj

            obj_clusters.bb_aspect_ratio = float('nan')
            obj_clusters.av_ratio = float('nan') 
            obj_clusters.compactness = float('nan')

            obj_clusters.aligned_bounding_box_center = vec_obj

            obj_clusters.min = vec_obj
            obj_clusters.max = vec_obj
            obj.clusters = [obj_clusters]
            return obj

        elif msg_type == 'std_msgs/String':
            obj.data = 'no_data'
            return obj
            
        elif msg_type == 'data_logger_bag/LogControl':
            obj.taskName = 'no task'
            obj.actionType = 'no action'
            obj.skillName = 'no skil;'
            obj.topics = 'no topics'
            obj.playback = False
            return obj
            
        elif msg_type == 'std_msgs/Bool':
            obj.data = False
            return obj

        elif msg_type == 'geometry_msgs/Pose2D':
            obj.x = float('nan')
            obj.y = float('nan')
            obj.theta = float('nan')
            return obj

        elif msg_type == 'audio_common_msgs/AudioData':
            obj.data = []
            #obj.data = [float('nan')]*2048
            rospy.logwarn("Audio might be misaligned")
            return obj

        elif msg_type == 'gait_capture/PersonFrame':
            obj.person_id = float('nan')
            obj.latest_time = float('nan')
            obj.body_parts = []
            return obj

        elif msg_type == 'cob_people_detection_msgs/DetectionArray':
            obj.detections = [float('nan')]
            return obj

        else:
            rospy.logerr("Message type: %s is not supported" % msg_type)

class Object:
    pass

def main():

    # Create DataTableBag object
    data = DataTableBagProcessor()

    # Open up h5 file to write
    data.setup_h5()
        
    # start processing bag files
    data.process_general()

    # Close the h5 file
    data.h5file.close()

if __name__ == "__main__":
    main()
