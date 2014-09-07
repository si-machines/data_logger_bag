#! /usr/bin/python
import roslib
roslib.load_manifest("data_logger_bag")
import rospy
import rosbag
import numpy
import sys
import os
from optparse import OptionParser
import itertools
import tables
import subprocess
import yaml
from collections import defaultdict
import numpy as np

from geometry_msgs.msg import Pose2D, Wrench, Vector3, Point, Pose
from std_msgs.msg import String, Int8, Float32MultiArray
from sensor_msgs.msg import JointState

class DataTableBag():

    def __init__(self):

        # TODO: These need to be parameterized later
        # Topics to skip
        self.skip_topics = "camera/depth_registered/image_raw camera/rgb/image_raw c6_end_effect_pose_right c6_end_effect_pose_left"
        
        # Topic to downsample to
        self.trigger_topic = "aff_haptic_state"

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
            self.write_pytables(filename)
            self.fileCounter +=1

    def process_bag(self, filename):

        print "Processing bag: %s" % filename
        bag = rosbag.Bag(filename)

        # Initialize variables
        self.all_topics = []
        self.topic_types = dict()
        self.data_store = dict()
        self.all_data = defaultdict(dict)

        # Open up the bag file and get info on it
        info_dict = yaml.load(subprocess.Popen(['rosbag', 'info', '--yaml', filename], stdout=subprocess.PIPE).communicate()[0])

        # Go through each of the topics and initialize variables
        for topics in info_dict['topics']:
            if not topics['topic'] in self.skip_topics:
                self.all_topics.append(topics['topic']) 
                self.topic_types[topics['topic']] = topics['type'] 

        # Number of entries
        num_entries = 0
        num_controller_entries = 0
        
        # Go through the topics in the bag file
        for topic, msg, stamp in bag.read_messages(topics=self.all_topics):
            num_entries += 1

            # Store off the topics not being downsampled
            self.data_store[topic] = (msg,stamp)

            # write off the values
            if topic == self.trigger_topic:
                num_controller_entries += 1

                # Go through all of the topics we stored off and write them to
                # a dictionary
                for topic_store in self.data_store:
                    self.process_msg(topic_store, self.data_store[topic_store])

       
    def process_msg(self, topic, data):

        # Custom function that will process the messages depending on type
        msg_type = self.topic_types[topic] 
        msg = data[0]
        stamp = data[1]

        if msg_type == 'geometry_msgs/Wrench':
            self.process_wrench(topic, msg, stamp)

        elif msg_type == 'std_msgs/Int8':
            self.process_int8(topic, msg, stamp)

        elif msg_type == 'sensor_msgs/JointState':
            # We only pass in the msg and not the timestamp because the msg itself has
            # a time stamp in the header
            self.process_jointState(topic, msg)
            
        elif msg_type == 'rospcseg/ClusterArrayV0':
            self.process_clusterArray(data)
            
        elif msg_type == 'sensor_msgs/CameraInfo':
            self.process_cameraInfo(data)
            
        elif msg_type == 'std_msgs/String':
            self.process_string(topic, msg, stamp)
            
        elif msg_type == 'data_logger_bag/LogControl':
            self.process_logControl(topic, msg, stamp)
            
        elif msg_type == 'std_msgs/Bool':
            self.process_bool(topic, msg, stamp)
        else:
            print "Message type: %s is not supported" % msg_type


    def process_wrench(self, topic, msg, stamp):

        #print "processing geometry_msgs/Wrench message"
        # Process forces
        if 'force' not in self.all_data[topic]:
            self.all_data[topic]['force'] = []

        # Store the value
        force_store = []
        force_store.append(msg.force.x)
        force_store.append(msg.force.y)
        force_store.append(msg.force.z)
        self.all_data[topic]['force'].append(force_store)
        
        # Process torques
        if 'torque' not in self.all_data[topic]:
            self.all_data[topic]['torque'] = []

        torque_store = []
        torque_store.append(msg.torque.x)
        torque_store.append(msg.torque.y)
        torque_store.append(msg.torque.z)
        self.all_data[topic]['torque'].append(torque_store)
        
        # Process timestamp - currently just seconds
        if 'time' not in self.all_data[topic]:
            self.all_data[topic]['time'] = []

        self.all_data[topic]['time'].append(stamp.to_sec())

    def process_int8(self, topic, msg, stamp):

        #print "processing std_msgs/Int8 message"
        if 'data' not in self.all_data[topic]:
            self.all_data[topic]['data'] = []

        self.all_data[topic]['data'].append(msg.data)
       
        if 'time' not in self.all_data[topic]:
            self.all_data[topic]['time'] = []

        self.all_data[topic]['time'].append(stamp.to_sec())

    def process_jointState(self, topic, msg):

        #print "processing sensor_msgs/JointState message"
        if 'name' not in self.all_data[topic]:
            self.all_data[topic]['name'] = msg.name

        if 'position' not in self.all_data[topic]:
            self.all_data[topic]['position'] = []

        self.all_data[topic]['position'].append(msg.position)

        if 'velocity' not in self.all_data[topic]:
            self.all_data[topic]['velocity'] = []

        self.all_data[topic]['velocity'].append(msg.velocity)

        if 'effort' not in self.all_data[topic]:
            self.all_data[topic]['effort'] = []

        self.all_data[topic]['effort'].append(msg.effort)
        
        if 'time' not in self.all_data[topic]:
            self.all_data[topic]['time'] = []

        self.all_data[topic]['time'].append(msg.header.stamp.to_sec())

    def process_clusterArray(self, msg):

        #print "processing rospcseg/ClusterArrayV0 message"
        return
        
    def process_cameraInfo(self, msg):

        #print "processing sensor_msgs/CameraInfo message"
        return

    def process_string(self, topic, msg, stamp):

        # It is currently the same for this type
        self.process_int8(topic, msg, stamp)

    def process_logControl(self, topic, msg, stamp):

        #print "processing data_logger_bag/LogControl message"
        return

    def process_bool(self, topic, msg, stamp):

        #print "processing data_logger_bag/LogControl message"
        return

    def write_pytables(self, filename):

        # Setup the pytable names
        '''
        Start writing data to the table
        '''
        # TODO figure out naming scheme for groups
        group_name = filename.partition(".")[0]
        path_name = os.path.split(group_name) # Split filepath from filename
        group_name = path_name[1] + '_'+path_name[0].split("/")[-1]
        #group_name = group_name+'_'+str(fileCounter) # Add simple counter for ID purposes later
        group_name = "haptic_exploration_"+path_name[0].split("/")[-1]+"_"+str(self.fileCounter) 
        
        print "Writing file: %s to pytable" % path_name[1]

        bag_group = self.h5file.createGroup("/", group_name)
       
        # Go through and write dictionary types into pytable
        for topic in self.all_data:
            msg_type = self.topic_types[topic]

            topic_group = self.h5file.createGroup(bag_group, topic)
            data = self.all_data[topic]

            if msg_type == 'geometry_msgs/Wrench':
                self.write_wrench(topic_group, data)

            elif msg_type == 'std_msgs/Int8':
                self.write_int8(topic_group, data)

            elif msg_type == 'sensor_msgs/JointState':
                self.write_jointState(topic_group, data)
                
            elif msg_type == 'rospcseg/ClusterArrayV0':
                self.write_clusterArray(topic_group, data)
                
            elif msg_type == 'sensor_msgs/CameraInfo':
                self.write_cameraInfo(topic_group, data)
                
            elif msg_type == 'std_msgs/String':
                self.write_string(topic_group, data)
                
            elif msg_type == 'data_logger_bag/LogControl':
                self.write_logControl(topic_group, data)

            elif msg_type == 'std_msgs/Bool':
                self.write_bool(topic_group, data)
                
            else:
                print "Message type: %s is not supported" % msg_type
        

    def write_wrench(self, topic_group, data):

        # Write force
        data_size = np.shape(data['force'])
        carray = self.h5file.createCArray(topic_group, 'force', tables.Float64Atom(), data_size)
        carray[:] = data['force']
       
        # Write torque
        data_size = np.shape(data['torque'])
        carray = self.h5file.createCArray(topic_group, 'torque', tables.Float64Atom(), data_size)
        carray[:] = data['torque']

        # Write time
        data_size = np.shape(data['time'])
        carray = self.h5file.createCArray(topic_group, 'time', tables.Float64Atom(), data_size)
        carray[:] = data['time']

    def write_int8(self, topic_group, data):

        # Write data
        data_size = np.shape(data['data'])
        carray = self.h5file.createCArray(topic_group, 'data', tables.Int64Atom(), data_size)
        carray[:] = data['data']

        # Write time
        data_size = np.shape(data['time'])
        carray = self.h5file.createCArray(topic_group, 'time', tables.Float64Atom(), data_size)
        carray[:] = data['time']

    def write_jointState(self, topic_group, data):

        # Write joint names
        data_size = np.shape(data['name'])
        carray = self.h5file.createCArray(topic_group, 'name', tables.StringAtom(itemsize=20), (data_size))
        carray[:] = data['name']

        # Write joint positions
        data_size = np.shape(data['position'])
        carray = self.h5file.createCArray(topic_group, 'position', tables.Float64Atom(), data_size)
        carray[:] = data['position']
        
        # Write joint velocities
        data_size = np.shape(data['velocity'])
        carray = self.h5file.createCArray(topic_group, 'velocity', tables.Float64Atom(), data_size)
        carray[:] = data['velocity']

        # Write joint velocities
        data_size = np.shape(data['effort'])
        carray = self.h5file.createCArray(topic_group, 'effort', tables.Float64Atom(), data_size)
        carray[:] = data['effort']

        # Write time
        data_size = np.shape(data['time'])
        carray = self.h5file.createCArray(topic_group, 'time', tables.Float64Atom(), data_size)
        carray[:] = data['time']

    def write_clusterArray(self, bag_group, topic):

        return

    def write_cameraInfo(self, bag_group, topic):
        return

    def write_string(self, bag_group, topic):
        return

    def write_logControl(self, bag_group, topic):
        return

    def write_bool(self, bag_group, topic):
        return


def main():

    if len(sys.argv) < 3:
        print "Usage: %s [input_files] [output_files]" % sys.argv[0]
        return

    input_filenames = sys.argv[1:-1]
    output_filename = sys.argv[-1]

    if not output_filename.endswith(".h5"):
        print "Filename: %s is not a h5 file" % output_filename
        sys.exit()

    # Create DataTableBag object
    data = DataTableBag()
    data.input_filenames = input_filenames
    data.output_filename = output_filename

    # Open up h5 file to write
    data.setup_h5()
        
    # start processing bag files
    data.process_files()

    # Close the h5 file
    data.h5file.close()

if __name__ == "__main__":
    main()
