HADOOP_HOME="/home/user/hadoop-2.7.1"
HDFS_PATH=hdfs://192.168.184.165:54310/

INPUT_FILES="
input.txt"

# DANGER!!! delete all the files
#$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS_PATH/*

for file in $INPUT_FILES
do 
	echo "copying ${file##*/}"
	$HADOOP_HOME/bin/hdfs dfs -rm -r $HDFS_PATH/${file##*/}
	$HADOOP_HOME/bin/hdfs dfs -put $file $HDFS_PATH/${file##*/}
done
