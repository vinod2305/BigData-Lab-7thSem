#/!bin/bash
tar xzvf hadoop-3.2.2.tar.gz
mv hadoop-3.2.2 hadoop2
echo export JAVA_HOME=/$(readlink -f $(which javac) | cut -d "/" -f 2,3,4,5) >> hadoop2/etc/hadoop/hadoop-env.sh
