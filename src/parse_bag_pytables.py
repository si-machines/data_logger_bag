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

#from geometry_msgs.msg import Pose2D, Wrench, Vector3, Point, Pose
#from std_msgs.msg import String, Int8, Float32MultiArray
#from sensor_msgs.msg import JointState
#from pcseg_msgs.msg import ClusterArrayV0

class DataTableBag():

    def __init__(self):

        # Intialize the node
        rospy.init_node("bag_parser", anonymous=True)
        rospy.loginfo("Initializing bag parser")
       
        # Default topics to skip and subsample to
        default_skip_topics = "camera/depth_registered/image_raw camera/rgb/image_raw c6_end_effect_pose_right c6_end_effect_pose_left"
        default_trigger_topic = "C6_FSM_state"

        # read paramater
        self.skip_topics = rospy.get_param("~skip_topics", default_skip_topics)
        input_files = rospy.get_param("~input_files", "")
        output_file = rospy.get_param("~output_file", "converted_data.h5")

        # These need to be set if we want to subsample down to a topic
        # Otherwise by default nothing is aligned
        self.num_joints = int(rospy.get_param("~num_joints", "57"))
        self.sub_sample = rospy.get_param("~sub_sample_flag", False)
        self.trigger_topic = rospy.get_param("~trigger_topic", default_trigger_topic)

        if input_files == "":
            rospy.logerr("Usage: %s _input_files:=<input_files> \n Optional args:\n_output_file:=<output_file.h5>\n_skip_topics:=<ros topics to NOT parse>\n_trigger_topic:=<what ros topic to downsample to>\n_sub_sample_flag:=<True or False>\n_num_joints:=<num_joints>" % sys.argv[0])
            sys.exit()

        # Expand wildcards if any
        self.input_filenames = glob.glob(os.path.expanduser(input_files))
        self.output_filename = os.path.expanduser(output_file)

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

        rospy.loginfo("Processing bag: %s" % filename)
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
                            self.process_msg(topic_store, self.data_store[topic_store])
                        else: 
                            self.process_msg(topic_store, (self.data_store[topic_store][0], stamp))

            else:
                self.process_msg(topic, (msg, stamp))
       
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
            self.process_jointState(topic, msg, stamp)
            
        elif msg_type == 'rospcseg/ClusterArrayV0':
            self.process_clusterArray(topic, msg, stamp)
            
        elif msg_type == 'sensor_msgs/CameraInfo':
            self.process_cameraInfo(data)
            
        elif msg_type == 'std_msgs/String':
            self.process_string(topic, msg, stamp)
            
        elif msg_type == 'data_logger_bag/LogControl':
            self.process_logControl(topic, msg, stamp)
            
        elif msg_type == 'std_msgs/Bool':
            self.process_bool(topic, msg, stamp)
        else:
            rospy.logerr("Message type: %s is not supported" % msg_type)


    def msg_field_helper(self, msg):
        '''
        Used for custom messages that are not standard in ROS
        '''
        
        helper_fields = ['serialize', 'serialize_numpy', 'deserialize', 'deserialize_numpy']
        fields = [name for name in dir(msg) if not (name.startswith('__') or name.startswith('_') or name in helper_fields)]
        return fields 

    def process_wrench(self, topic, msg, stamp):

        msg_fields = ['force','torque']
        for field in msg_fields:
            if field not in self.all_data[topic]:
                self.all_data[topic][field] = []

            data_store = []
            data_store.append(eval('msg.'+field+'.x'))
            data_store.append(eval('msg.'+field+'.y'))
            data_store.append(eval('msg.'+field+'.z'))
            self.all_data[topic][field].append(data_store)
        
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

    def process_jointState(self, topic, msg, stamp):

        # Store the name only once
        if 'name' not in self.all_data[topic] and hasattr(msg, 'header'):
            self.all_data[topic]['name'] = msg.name

        # Store all of the timestamps by seconds from msg
        if 'time' not in self.all_data[topic]:
            self.all_data[topic]['time'] = []

        if hasattr(msg, 'header'):
            self.all_data[topic]['time'].append(msg.header.stamp.to_sec())
        else:
            self.all_data[topic]['time'].append(stamp.to_sec())
            
        # Go through the actual fields of the msg and populate
        msg_fields = ['position','velocity','effort']
        for msg_field in msg_fields:
            if msg_field not in self.all_data[topic]:
                self.all_data[topic][msg_field] = []

            self.all_data[topic][msg_field].append(eval('msg.'+msg_field))

    def process_clusterArray(self, topic, msg, stamp):

        fields = self.msg_field_helper(msg.clusters[0])

        # Go through each cluster and turn into a single dict that we will convert
        clusters = []
        for one_cluster in msg.clusters:
            obj = dict()
            for field in fields:
                data = eval('one_cluster.'+field)
                if hasattr(data, '_type'):
                    if data._type == 'geometry_msgs/Vector3':
                        val = [data.x, data.y, data.z]
                    elif data._type == 'std_msgs/ColorRGBA':
                        val = [data.r, data.g, data.b, data.a]
                    else:
                        rospy.logerr("Unknown field type %s in msg type %s" % (data._type, msg.clusters[0]._type))
                else: 
                    val = data

                # Write the data to the field
                obj[field] = val
            clusters.append(obj)
     
        # Store the clusters away
        if 'clusters' not in self.all_data[topic]:
            self.all_data[topic]['clusters'] = []
        self.all_data[topic]['clusters'].append(clusters)

        # Store all of the timestamps by seconds from stamp
        if 'time' not in self.all_data[topic]:
            self.all_data[topic]['time'] = []
        self.all_data[topic]['time'].append(stamp.to_sec())

        # Store the max number of objects seen 
        if 'max_item' not in self.all_data[topic]:
            cur_max = 1
        else:
            cur_max = self.all_data[topic]['max_item']

        self.all_data[topic]['max_item'] = max(cur_max,len(clusters))
            

    def process_cameraInfo(self, msg):

        #print "processing sensor_msgs/CameraInfo message"
        return

    def process_string(self, topic, msg, stamp):

        # It is currently the same for this type
        self.process_int8(topic, msg, stamp)

    def process_logControl(self, topic, msg, stamp):

        msg_fields = self.msg_field_helper(msg)
        for msg_field in msg_fields:
            if msg_field not in self.all_data[topic]:
                self.all_data[topic][msg_field] = []

            self.all_data[topic][msg_field].append(eval('msg.'+msg_field))

        # Store all of the timestamps by seconds from stamp
        if 'time' not in self.all_data[topic]:
            self.all_data[topic]['time'] = []
        self.all_data[topic]['time'].append(stamp.to_sec())

    def process_bool(self, topic, msg, stamp):

        # It is currently the same for this type
        self.process_int8(topic, msg, stamp)

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
        group_name = "exploration_"+path_name[0].split("/")[-1]+"_"+str(self.fileCounter) 
        
        rospy.loginfo("Writing file: %s to pytable" % path_name[1])

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
                rospy.logerr("Message type: %s is not supported" % msg_type)
        

    def write_wrench(self, topic_group, data):

        self.pytable_writer_helper(topic_group, data.keys(), tables.Float64Atom(), data)

    def write_int8(self, topic_group, data):

        self.pytable_writer_helper(topic_group, ['data'], tables.Int64Atom(), data)
        self.pytable_writer_helper(topic_group, ['time'], tables.Float64Atom(), data)

    def write_jointState(self, topic_group, data):

        fields = ['position', 'velocity', 'effort', 'time']
        self.pytable_writer_helper(topic_group, fields, tables.Float64Atom(), data)
        self.pytable_writer_helper(topic_group, ['name'], tables.StringAtom(itemsize=20), data)

    def write_clusterArray(self, topic_group, data):

        cluster_dict = defaultdict(dict)
        clusters = data['clusters']
        dummy_obj = clusters[0][0] # Just grab a some random object
        # Go through the supposed number of objects
        for i in xrange(data['max_item']):

            # Go through each timestamp of the objects
            for cluster_array in clusters:

                good_value = True
                # Check to see if the object number exists
                if i >= len(cluster_array):
                    good_value = False
                    obj = dummy_obj
                else:
                    obj = cluster_array[i]
                
                # Go through the msg fields in the objects
                for field in obj:

                    # Check if field exists in the dictionary first
                    if field not in cluster_dict[i]:
                        cluster_dict[i][field] = []
                    
                    # Store NaNs if there was no object
                    if good_value is False:
                        if isinstance(dummy_obj[field],float):
                            dummy_vals = np.nan
                            cluster_dict[i][field].append(dummy_vals)
                        else:
                            dummy_vals = np.empty(np.shape(dummy_obj[field]))
                            dummy_vals[:] = np.nan
                            cluster_dict[i][field].append(dummy_vals.tolist())
                    else:
                        cluster_dict[i][field].append(obj[field])

        # populate the structure now
        for i in xrange(data['max_item']):
            single_obj = cluster_dict[i]
            obj_group = self.h5file.createGroup(topic_group, 'object_'+str(i))
            self.pytable_writer_helper(obj_group, single_obj.keys(), tables.Float64Atom(), single_obj)

    def write_cameraInfo(self, topic_group, data):
        return

    def write_string(self, topic_group, data):

        self.pytable_writer_helper(topic_group, ['data'], tables.StringAtom(itemsize=20), data)
        self.pytable_writer_helper(topic_group, ['time'], tables.Float64Atom(), data)

    def write_logControl(self, topic_group, data):

        fields = ['taskName', 'actionType', 'skillName', 'topics']
        self.pytable_writer_helper(topic_group, fields, tables.StringAtom(itemsize=20), data)
        self.pytable_writer_helper(topic_group, ['playback'], tables.BoolAtom(), data)

    def write_bool(self, topic_group, data):

        self.pytable_writer_helper(topic_group, ['data'], tables.BoolAtom(), data)
        self.pytable_writer_helper(topic_group, ['time'], tables.Float64Atom(), data)

    def pytable_writer_helper(self, topic_group, fields, data_type, data):

        # Go through the fields and write to the group
        for field in fields:
            data_size = np.shape(data[field])
            carray = self.h5file.createCArray(topic_group, field, data_type, data_size)
            carray[:] = data[field]

    def create_dummy_msg(self, topic):

        # Custom function that will process the messages depending on type
        msg_type = self.topic_types[topic] 
        obj = Object()

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

        elif msg_type == 'sensor_msgs/JointState':
            obj.name = ['no_joints'] * self.num_joints
            obj.position = [float('nan')]* self.num_joints
            obj.velocity = [float('nan')]* self.num_joints
            obj.effort = [float('nan')]* self.num_joints
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

        else:
            rospy.logerr("Message type: %s is not supported" % msg_type)

class Object:
    pass

def main():

    # Create DataTableBag object
    data = DataTableBag()

    # Open up h5 file to write
    data.setup_h5()
        
    # start processing bag files
    data.process_files()

    # Close the h5 file
    data.h5file.close()

if __name__ == "__main__":
    main()
