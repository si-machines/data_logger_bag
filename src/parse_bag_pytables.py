#! /usr/bin/python
import roslib
roslib.load_manifest("data_logger_bag")
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

from geometry_msgs.msg import Pose2D, Wrench, Vector3, Point, Pose
#from std_msgs.msg import String, Int8, Float32MultiArray
#from sensor_msgs.msg import JointState
#from pcseg_msgs.msg import ClusterArrayV0



class DataTableBagProcessor():

    def __init__(self):

        # Intialize the node
        rospy.init_node("bag_parser", anonymous=True)
        rospy.loginfo("Initializing bag parser")
       
        # Default topics to skip and subsample to
        default_skip_topics = "camera/depth_registered/image_raw camera/rgb/image_raw c6_end_effect_pose_right c6_end_effect_pose_left camera/rgb/camera_info camera/depth_registered/camera_info"
        default_trigger_topic = "C6_FSM_state"

        # read paramater
        self.skip_topics = rospy.get_param("~skip_topics", default_skip_topics)
        input_files = rospy.get_param("~input_files", "")
        output_file = rospy.get_param("~output_file", "converted_data.h5")
        output_file_flag = rospy.get_param("~dir_outfile_flag", False)

        # These need to be set if we want to subsample down to a topic
        # Otherwise by default nothing is aligned
        self.num_joints = int(rospy.get_param("~num_joints", "57"))
        self.sub_sample = rospy.get_param("~sub_sample_flag", False)
        self.trigger_topic = rospy.get_param("~trigger_topic", default_trigger_topic)

        if input_files == "":
            rospy.logerr("Usage: %s _input_files:=<input_files> \n Optional args:\n_output_file:=<output_file.h5>\n_skip_topics:=<ros topics to NOT parse>\n_trigger_topic:=<what ros topic to downsample to>\n_sub_sample_flag:=<True or False>\n_num_joints:=<num_joints>\n_dir_outfile_flag:=<True or False>" % sys.argv[0])
            sys.exit()

        # Expand wildcards if any
        self.input_filenames = glob.glob(os.path.expanduser(input_files))
        self.output_filename = os.path.expanduser(output_file)

        if output_file_flag:
            # Setup output file names with directory format
            file_split = input_files.split(os.sep)
            self.output_filename = "-".join(file_split[-3:-1])+'.h5'

        if self.sub_sample:
            self.output_filename = self.output_filename.split('.')[0] + '_subsampled.h5'
            rospy.loginfo("WARNING: subsample topic is %s. Make sure this is your state..." % self.trigger_topic)

        # Initialize some variables that might be needed
        self.fileCounter = 0 
       
    def setup_h5(self):

        filters = tables.Filters(complevel=9)
        self.h5file = tables.openFile(self.output_filename, mode="w", title="Aggregated Exploration Data",
                                 filters = filters)

        rospy.loginfo("Initialized h5 output file")

    def process_files(self):

        for filename in self.input_filenames:

            # Go through each file
            self.process_bag(filename)
            self.bagData.write_pytables(filename, self.fileCounter)
            self.fileCounter +=1

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
            obj_clusters.aligned_bounding_box_size = vec_obj
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

        elif msg_type == 'audio_tracking/AudioData16':
            obj.data = [float('nan')]*2048
            return obj

        elif msg_type == 'gait_capture/PersonFrame':
            obj.person_id = float('nan')
            obj.latest_time = float('nan')
            obj.body_parts = []
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
    data.process_files()

    # Close the h5 file
    data.h5file.close()

if __name__ == "__main__":
    main()
