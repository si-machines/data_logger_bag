# data_logger_bag

This package contains utilities to write to bag files and then convert to hdf5 format. 

Specifically, this package contains two main core functions:

1. The ability for folks to programmatically start/stop recording bag files using the commandline rosbag interface rather than the rosbag API. It also allows changing record topics and naming of bag files using a custom message that can be published from any node.

2. Utilities that automatically converts rosbag files based on message type into hdf5 format. Also some basic utilities that can then load the hdf5 file into python using pytables.

For more details on how to install and tutorials on how to run the package please see the [wiki](https://github.com/si-machines/data_logger_bag/wiki)


