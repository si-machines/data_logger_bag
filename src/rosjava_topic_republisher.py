#!/usr/bin/env python
import roslib; roslib.load_manifest("data_logger_bag")
import rospy
from std_msgs.msg import String
from geometry_msgs.msg import Pose

def publish(msg, publisher):
    publisher.publish(msg)


def callback(data):
    #rospy.loginfo("I heard %s",data)
    publish(data, pub)   

def callbackRight(data):
    #rospy.loginfo("I heard %s",data)
    publish(data, pubRight)   

 
def listener():
    rospy.Subscriber("/c6_end_effect_pose_left", Pose, callback)
    rospy.Subscriber("/c6_end_effect_pose_right", Pose, callbackRight)
    # spin() simply keeps python from exiting until this node is stopped
    rospy.spin()

if __name__ == "__main__":

    rospy.init_node('relogger_node')
    rospy.loginfo("Started up republisher.  Will republish left and right EEF")
    pub = rospy.Publisher("c6_EEF_left", Pose)
    pubRight = rospy.Publisher("c6_EEF_right", Pose)
    listener()
