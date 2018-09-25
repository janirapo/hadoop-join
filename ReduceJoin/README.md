How to run
```
javac ReduceJoin.java -cp $(hadoop classpath) -Xlint:deprecation
jar cf rj.jar ReduceJoin*.class
hadoop jar rj.jar ReduceJoin /input/student_5000000.txt /input/score_500000.txt /output
hadoop fs -cat /output/part-r-00000 | sort
```
