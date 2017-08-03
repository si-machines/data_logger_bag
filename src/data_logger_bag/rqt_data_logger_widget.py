import rospy
import rospkg
import os
from .logger import BagDataLogger
from data_logger_bag.srv import GetSettings
from data_logger_bag.msg import LogControl

from python_qt_binding import loadUi
from python_qt_binding.QtCore import Qt, Slot, qWarning
from python_qt_binding.QtGui import QIcon, QMenu, QTreeWidgetItem, QWidget, QListWidgetItem

class DataLoggerWidget(QWidget):
    """
    Widget to help visualize and control the data logger package
    """

    def __init__(self, context):

        super(DataLoggerWidget, self).__init__()
        ui_file = os.path.join(rospkg.RosPack().get_path('data_logger_bag'), 'resource', 'DataLogger.ui')
        loadUi(ui_file, self)

        self.setObjectName('DataLoggerUi')
        self.setWindowTitle(self.windowTitle() + (' (%d)' % context.serial_number()))
        # Add widget to the user interface
        context.add_widget(self)

        # Buttons/Text for setting values
        self.sendParamsButton.clicked[bool].connect(self._send_params)

        # Buttons/Text for current values
        self.refresh_current_settings.clicked[bool].connect(self._reload_values)

        # Setup publishers and subscribers
        rospy.logwarn("Waiting for data logger service: %s" % BagDataLogger.REQUEST_LOGGER_SETTING_SRV)
        rospy.wait_for_service(BagDataLogger.REQUEST_LOGGER_SETTING_SRV)
        self.log_set_srv = rospy.ServiceProxy(BagDataLogger.REQUEST_LOGGER_SETTING_SRV, GetSettings)
        rospy.logwarn("Data logger service: %s loaded" % BagDataLogger.REQUEST_LOGGER_SETTING_SRV)

        # Get the data logger control topic
        self.log_control_topic = rospy.get_param(BagDataLogger.GLOBAL_CONTROL_TOPIC, "C6_Task_Description")

        # Setup publishers
        self.log_pub = rospy.Publisher(self.log_control_topic, LogControl, queue_size=5)

    def _send_params(self):

        # Create a log control message and populate
        ctrl_msg = LogControl()
        ctrl_msg.taskName = self.setTaskBox.text()
        ctrl_msg.skillName = self.setSkillBox.text()

        # Publish the values to the data logger
        rospy.loginfo("Sending task: %s, skill: %s." % (ctrl_msg.taskName, ctrl_msg.skillName))
        self.log_pub.publish(ctrl_msg)

    def _reload_values(self):
        response = self.log_set_srv(True)
        self.cur_data_path.setText(response.data_location)
        topics = response.response.topics.split(' ')
        self.current_topics.clear()

        # Go through the topics we're recording
        for topic in topics:
            item = QListWidgetItem(topic)
            self.current_topics.addItem(item)

        # Get the current task and skill (They are used for directory location)
        self.currentTask.setText(response.response.taskName)
        self.currentSkill.setText(response.response.skillName)


