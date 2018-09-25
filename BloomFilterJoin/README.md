```
javac BloomFilterJoin.java -cp $(hadoop classpath) -Xlint:deprecation
jar cf bfj.jar BloomFilterJoin*.class
hadoop jar bfj.jar BloomFilterJoin /input/student_5000000.txt /input/score_500000.txt /output
hadoop fs -cat /output/part-r-00000 | sort
```
