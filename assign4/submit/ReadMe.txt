Mapper: map from each input line to a key-value pair with the joining column as the key and the whole line as the value

Reducer:
1. Divide the values into two sets of lines, by the table name.
2. For each line in the first set, append it with each line in the second set.
3. Write the appended line to the output file

Driver: example code from Hadoop's tutorial

Just in case, included run.sh to recompile and run the function, need to change variables in run.sh to your local settings
