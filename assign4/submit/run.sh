HADOOP_CMD="/home/user/hadoop-2.7.1/bin/hadoop"
#HADOOP_CMD="/usr/local/hadoop/bin/hadoop"

FUNCTION_NAME="Equijoin"
INPUT_PATH=""
INPUT_FILE="input.txt"
OUTPUT_PATH="/output1"


set -x

# compile
${HADOOP_CMD} com.sun.tools.javac.Main ${FUNCTION_NAME}.java 

# build the jar
jar cf ${FUNCTION_NAME}.jar ${FUNCTION_NAME}*.class
rm ${FUNCTION_NAME}*.class

# clean the output path
${HADOOP_CMD} fs -rm ${OUTPUT_PATH}/*
${HADOOP_CMD} fs -rmdir ${OUTPUT_PATH}

# run the function
${HADOOP_CMD} jar ${FUNCTION_NAME}.jar ${FUNCTION_NAME} ${INPUT_PATH}/${INPUT_FILE} ${OUTPUT_PATH}

# print the result
${HADOOP_CMD} fs -ls ${OUTPUT_PATH}
${HADOOP_CMD} fs -cat ${OUTPUT_PATH}/part-r-00000

