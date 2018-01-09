#!/usr/bin/env python

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
logger.py

Base logger class that controls calling the rosbag command to write to bag files.
Allows for specification of where to store the bag files to as well as
the topics to record
'''

import rospy
import os
import time
import subprocess
import signal
from std_msgs.msg import Bool, String
from data_logger_bag.msg import LogControl
from data_logger_bag.srv import GetSettingsResponse, GetSettings

class BagDataLogger:

    REQUEST_LOGGER_SETTING_SRV = "get_data_logger_settings"
    GLOBAL_CONTROL_TOPIC = "/data_logger_bag/control"
    GLOBAL_START_STOP_TOPIC = "/data_logger_bag/flag_topic"

    ''' 
    Initialize the node 
    '''
    def __init__(self):

        # Initialize node
        rospy.init_node("data_logger", anonymous=True)

        # default topics to record - separated only by a space
        default_record_topics = "joint_states c6_logger_flag"
        default_log_flag = "data_logger_flag"
        default_data_location = "data"
        default_data_prefix = "playback"
        default_c6_task_topic = "C6_Task_Description"
        default_task = "default_task"
        default_skill = "default_skill"
        default_timestamp = True

        # Get parameter names from the param server
        self.c6_task_topic = rospy.get_param("~c6_task_topic", default_c6_task_topic) 
        self.logger_flag_topic = rospy.get_param("~log_flag_topic", default_log_flag)
        self.record_topics = rospy.get_param("~record_topics", default_record_topics)
        self.data_location = rospy.get_param("~datapath", default_data_location)
        self.data_prefix = rospy.get_param("~data_prefix", default_data_prefix)
        self.timestamp = rospy.get_param("~timestamp", default_timestamp)

        # Set the global topic name
        rospy.set_param(self.GLOBAL_CONTROL_TOPIC, self.c6_task_topic)
        rospy.set_param(self.GLOBAL_START_STOP_TOPIC, self.logger_flag_topic)

        # Initialize the flag to false
        self.logger_flag = False

        # Intialize default task and skill names
        self.task = rospy.get_param("~task", default_task)
        self.skill = rospy.get_param("~skill", default_skill)

        # Initialize some default runName
        self.runName = "";
        self.filename = "";

        # Append the task and skill onto the data_location
        self.data_custom_location = os.path.join(self.data_location, self.task, self.skill)

        # Setup publisher for bagfile
        self.bag_file_loc_pub = rospy.Publisher('bag_file_loc', String, queue_size=10)

        # Setup service for getting the current settings
        self.setting_srv = rospy.Service(self.REQUEST_LOGGER_SETTING_SRV, GetSettings, self._return_settings)

        rospy.loginfo("Initialized data logger node with logger flag topic: %s" % self.logger_flag_topic)

    '''
    Setup service response to getting the current settings of the logger
    '''
    def _return_settings(self, req):

        response = GetSettingsResponse()
        msg = LogControl()
        msg.taskName = self.task
        msg.skillName = self.skill
        msg.actionType = self.data_prefix
        msg.topics = self.record_topics
        msg.runName = self.runName
        response.response = msg
        response.data_location = os.path.join(os.path.expanduser("~"),self.data_custom_location)
        response.filename = self.filename
        return response

    '''
    Setup the subscribers
    '''
    def startListener(self):

        # We do this after we init to not have issues with messages being listen to before we are ready
        # Listen for the flag to know when to start and stop the recording
        rospy.Subscriber(self.logger_flag_topic, Bool, self.flag_callback)

        # Setup a listener for the task and action to put in a custom folder
        # This message also contains which logging topics that the user can
        # specify to listen to
        rospy.Subscriber(self.c6_task_topic, LogControl, self.change_log_settings_cb)

        # Start the ros node
        rospy.spin()


    '''
    CB that changes the location of where we are saving data to based off of 
    the current task and action
    '''
    def change_log_settings_cb(self, msg):

        # Only change the directory if we are not currently writing to a file
        if self.logger_flag is False:
    
            if msg.taskName is not "":
                self.task = msg.taskName

            if msg.skillName is not "":
                self.skill = msg.skillName

            if msg.topics is not "":
                self.record_topics = msg.topics
        
            if msg.runName is not "":
                self.runName = msg.runName
                rospy.loginfo("Filename to be written is: %s" % self.runName)
    
            playback_flag = msg.playback

            # Append the task and skill onto the data_location
            self.data_custom_location = os.path.join(self.data_location, self.task, self.skill) 
            rospy.loginfo("Location writing changed to: %s" % self.data_custom_location)

            # What topics we're recording
            rospy.loginfo("Topics that will be subscribed to: %s" % self.record_topics)

            # Change prefix of the data to say if we are demonstrating or playing back
            if (playback_flag):
                self.data_prefix="playback"
            else:
                self.data_prefix="learning"

            rospy.loginfo("Data prefix is: %s" % self.data_prefix)

        else:
            rospy.loginfo("Currently still writing previous record. Settings for logging NOT changed")


    '''
    Callback for when to trigger recording of bag files
    '''
    def flag_callback(self, msg):

        # Setup flag from message
        rospy.loginfo(rospy.get_name() + ": I heard %s" % msg.data) 
       
        # Checks for change in flag first
        if self.logger_flag != msg.data:
            
            rospy.loginfo("Detected flag state change")

            # Set the flag after checking 
            self.logger_flag = msg.data

            # Then check what the message is telling us
            if msg.data is True:
                self.startRecord()
            else:
                self.stopRecord()



    def startRecord(self):
    
        rospy.loginfo("Set up bag file to write to")
        if self.runName is "":
            if self.timestamp:
                filename = self.data_prefix + "_"+time.strftime("%Y-%m-%dT%H%M%S") + ".bag"
            else:
                filename = self.data_prefix + ".bag"
                
        else:
            if self.timestamp:
                filename = self.data_prefix + "_"+self.runName+"_"+time.strftime("%Y-%m-%dT%H%M%S") + ".bag"
            else:
                filename = self.data_prefix + ".bag"

        # Store the current filename
        self.filename = filename

        self.datapath = os.path.join(os.path.expanduser("~"),self.data_custom_location, filename)
        #rospy.loginfo("File name is: %s" % datapath)
        
        # Check if directory exists
        self.ensure_dir(self.datapath)
       
        # Setup the command for rosbag
        # We don't use the compression flag (-j) to avoid slow downs
        rosbag_cmd = " ".join(("rosbag record -O",self.datapath,self.record_topics))
        rospy.loginfo("Command to run: %s" % rosbag_cmd)

        # Start the command through the system
        self.rosbag_proc = subprocess.Popen([rosbag_cmd], shell=True)
        rospy.loginfo("Recording bag file")
        

    def stopRecord(self):

        # Send command to the process to end (same as ctrl+C)
        self.rosbag_proc.send_signal(subprocess.signal.SIGINT)
        rospy.loginfo("Stopping bag file recording")

        # Kill all extra rosbag "record" nodes
        self.terminate_ros_node("/record")

        # Publish out what bag file that we finished reccording to
        self.bag_file_loc_pub.publish(String(self.datapath))

    '''
    Useful function from ROS answers that kills all nodes with the string match
    at the beginning
    '''
    def terminate_ros_node(self,s):
        list_cmd = subprocess.Popen("rosnode list", shell=True, stdout=subprocess.PIPE)
        list_output = list_cmd.stdout.read()
        retcode = list_cmd.wait()
        assert retcode == 0, "List command returned %d" % retcode
        for str in list_output.split("\n"):
            if (str.startswith(s)):
                os.system("rosnode kill " + str)



    '''
    Simple function that makes sure the data directory specified exists
    '''
    def ensure_dir(self, f):
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d)

if __name__ == '__main__':

    data_logger = BagDataLogger()
    data_logger.startListener()






