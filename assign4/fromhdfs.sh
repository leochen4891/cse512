HADOOP_HOME="/home/user/hadoop-2.7.1"
HDFS_PATH="hdfs://192.168.184.165:54310/"

OUTPUT_PATH="."
OUTPUT_FILES="
output.txt"


mkdir $OUTPUT_PATH
rm $OUTPUT_PATH/*

for file in $OUTPUT_FILES
do 
	echo "copying $file"
	$HADOOP_HOME/bin/hdfs dfs -get $HDFS_PATH/$file $OUTPUT_PATH/$file
done



