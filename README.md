The source files: MySort.cpp, quicksort.cpp and minheap.cpp.MySort.cpp is the source file where the code handles sort function.quicksort.cpp has the quicksort algorithm and minheap.cpp is used of the implementation of the min heap.

In this assignment gensort is used.Files which are generated by the gensort are taken as the input for MySort.cpp and linsort for sorting the data.

To generate the file :use the following command

  ./64/gensort -a filesize filename.txt
  
filesize for example: for 1gb file the filesize will be 10000000
 
To compile use the command: make all
which runs the following:
 
 g++ -std=c++11 -g -pthread quicksort.cpp MySort.cpp minheap.cpp -o ms.out

To run the code use the following command:

  ./ms.out -F [filename] -t [number of threads]
  
file name is the input file which is generated using the gensort
number of threads is number of threads which are used for internal sorting and external sorting
if it is the internal sorting 2 and 4 will be the number of threads
if it is external sorting 24 and 48 will be the number of threads

after running the command ms.out will create a file with name sorted-data which contains the sorted data by MySort.cpp
As the 16gb and 64gb file sizes are more after executing MySort.cpp the input files will be deleted.so it has to be generated again. 


linsort for all the files of sizes 1gb,4gb,16gb,64gb and time is calculated for generated file.

for linsort following command is used:

time sort filename.txt -k1 -S8G --parallel=<number_of_threads> -o <path_of_file> | tee -a <path_where_log_to_be_generated>


sar command for calculating memory,cpu and I/O utilization used the following command:

sar -r -u -b 1 2>errorfile.txt 1>outputfile.txt

-r → is for the calculating memory
-u → is for calculating cpu utilization
-b → is for calculating I/O utilization
The errors generated will be stored in the errorfile.txt and the output is stored in the outputfile.txt.


