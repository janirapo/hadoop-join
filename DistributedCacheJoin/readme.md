How to run and check results
```
javac DistributedCacheJoin.java -cp $(hadoop classpath) -Xlint:deprecation
jar cf dcj.jar DistributedCacheJoin*.class
hadoop jar dcj.jar DistributedCacheJoin /input/student_5000000.txt /input/score_500000.txt /output
hadoop fs -cat /output/part-m-00000 | sort
```
