export HADOOP_HOME="/home/vinod/hadoop/hadoop2"

export CLASSPATH="$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.2.2.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-common-3.2.2.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-3.2.2.jar:$HADOOP_HOME/lib/*"

javac -d . driver.java reducer.java mapper.java

jar cfm lab1.jar Manifest.txt code/*.class

$HADOOP_HOME/bin/hadoop jar lab1.jar dataset.csv output
