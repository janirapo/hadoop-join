# Bloom filter join

This program performs a inner join operation on the input files given as first and second parameters with the help of a bloof filter. Output is written into third parameter folder.

**Example usage:**
```
javac BloomFilterJoin.java -cp $(hadoop classpath) -Xlint:deprecation
jar cf bfj.jar BloomFilterJoin*.class
hadoop jar bfj.jar BloomFilterJoin /input/student_5000000.txt /input/score_500000.txt /output
hadoop fs -cat /output/part-r-00000 | sort
```
