//Delete the temporary file using:

hdfs dfs -rm -r /bxr140530/Asgn/Q2IntermediateOutput

//Now run the jar file using

hadoop jar Question2.jar Question2 /yelpdatafall/review/review.csv /bxr140530/output2

//See the output using
hdfs dfs -cat /bxr140530/output2/*

(or)

The output for the program is included in the folder name "out6"
Open the part-r-00000 file in either notepad++ or sublime text to view the output.

