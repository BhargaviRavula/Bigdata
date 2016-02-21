//Delete the temporary file using:

hdfs dfs -rm -r /bxr140530/Asgn/temp

//Now run the jar file using

hadoop jar Question3.jar Question3 /yelpdatafall/review/review.csv /yelpdatafall/business/business.csv /bxr140530/output3

//See the output using
hdfs dfs -cat /bxr140530/output3/*

(or)

The output for the program is included in the folder name "out7"
Open the part-r-00000 file in either notepad++ or sublime text to view the output.

