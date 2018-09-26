# Distributed Cache Join
This program performs a inner join operation on the input files given as first and second parameters with the help of a distributed cache. Output is written into third parameter folder.

**Example usage:**
```
javac DistributedCacheJoin.java -cp $(hadoop classpath) -Xlint:deprecation
jar cf dcj.jar DistributedCacheJoin*.class
hadoop jar dcj.jar DistributedCacheJoin /input/student_5000000.txt /input/score_500000.txt /output
hadoop fs -cat /output/part-m-00000 | sort
```
