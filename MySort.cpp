#include <iostream>
//For string functions
#include <cstring>
#include <string>
//For files
#include <fstream>
#include "quicksort.h"
#include <queue>
#include <thread>
#include <chrono>
#include <numeric>
#include <cstdio>
#include <vector>
#include "mergesort.hpp"
#include "minheap.h"

using namespace std;

//Constants
#define mb (1024*1024)
#define kb (1024)
long long gb  = 1024*1024*1024;

long long memory_size  = 8*gb;

#define length_of_line 200

#define SORT_THREADS 48


double tot_write_time = 0;
double tot_sort_time = 0;
double tot_read_time = 0;
double tot_merge_time = 0;



class File_Operations{
    
    char **data;
    long long num_of_lines; //Number of lines of data 
    fstream handle_file;
    std::string File_Name;
    int Open_Type;
    long long Total_Size;
    long long Total_Num_Lines;
    public:
    File_Operations(std::string file_name,long long total_size,int open_type){
        Total_Size = total_size;
        File_Name = file_name;
        Total_Num_Lines = (long long)total_size/length_of_line;
        Open_Type = open_type;
    }
    void open_file()
    {
        if(Open_Type == 0){  //reading from file
            handle_file.open(File_Name,ios::out | ios::in);
        }
        else{
            handle_file.open(File_Name,ios::out);
        }
    }
    void close_file()
    {
        handle_file.close();
        
    }
    char** read_file(long long num_lines,long long start_line,vector<double> &read_time_spent, int read_index){
        chrono::high_resolution_clock::time_point r_start = chrono::high_resolution_clock::now();
        ifstream input_file;
        input_file.open(File_Name,ios::binary);
        input_file.seekg(start_line * length_of_line,ios_base::beg);
        char **a;
        a = (char**)malloc(num_lines * sizeof(char*));
        for (long long i = 0; i < num_lines; i++)
        {
            char* line = (char*)malloc(length_of_line);
            input_file.read(line,length_of_line);
            a[i] = line;
        }
        input_file.close();
        chrono::high_resolution_clock::time_point r_end = chrono::high_resolution_clock::now();
        chrono::duration<double> read_time = chrono::duration_cast<chrono::duration<double>>(r_end - r_start);
        read_time_spent[read_index] = read_time.count();
        return a;
    }

    //Write/append data to files
    void write_file(char** write_data,long long num_lines,vector<double> &write_time_spent, int write_index)
    {
        // chrono::high_resolution_clock::time_point w1 = chrono::high_resolution_clock::now();
        chrono::high_resolution_clock::time_point w_start = chrono::high_resolution_clock::now();
        ofstream outfile(File_Name,ios::binary);

        for(long long i = 0; i < num_lines; ++i)
        {
            outfile.write(write_data[i],length_of_line);
        }
        for (long long i = 0; i < num_lines; i++)
        {
            delete[] write_data[i];
        }
        
        delete[] write_data;
        
        chrono::high_resolution_clock::time_point w_end = chrono::high_resolution_clock::now();
        chrono::duration<double> write_time = chrono::duration_cast<chrono::duration<double>>(w_end - w_start);
        write_time_spent[write_index] = write_time.count();

    }

    long long getFileSize()
    {
        return Total_Size;
    }
    long long getNumOfLines()
    {
        return Total_Num_Lines;
    }
    char** getData()
    {
        return data;
    }
    void setData(char** input_data)
    {
        data = input_data;
    }
    void setNumOfDataLines(long long num_of_data_lines)
    {
        num_of_lines = num_of_data_lines;
    }
    string getFileName(){
        return File_Name;
    }
};

class Details
{
    int num_threads;
    string file_name;
    bool file_name_exists;
    bool thread_exists;
    public:
    Details(int argc, char* argv[])
    {
        thread_exists = false;

        for (int i = 0; i < argc; i++)
        {
            if(i+1 != argc){
                if(strcmp(argv[i],"-t") == 0)
                {
                    num_threads = std::stoi(argv[i+1]);
                    thread_exists = true;
                }
                else if(strcmp(argv[i],"-F") == 0)
                {
                    file_name = argv[i+1];
                    file_name_exists = true;
                }
            }
        }
        if(thread_exists == false)
        {
            num_threads = SORT_THREADS;
        }
    }
    int file_verification()
    {
        if(file_name_exists == false)
        {
            return 0;
        }
        return 1;
    }
    int getNumOfThreads()
    {
        return num_threads;
    }
    string getFileName()
    {
        return file_name;
    }
};


void sort_file(char** sort_data,long long num_lines,vector<double>&sort_time,int sort_index)
{
    chrono::high_resolution_clock::time_point sort_t1 = chrono::high_resolution_clock::now();
    //QuickSort(sort_data, 0, num_lines - 1);
    MergeSort(sort_data, 0, num_lines - 1);
    chrono::high_resolution_clock::time_point sort_t2 = chrono::high_resolution_clock::now();
    chrono::duration<double> sort_span = chrono::duration_cast<chrono::duration<double>>(sort_t2 - sort_t1);
    sort_time[sort_index] = sort_span.count();
}

//Function that performs quick sort operation
void write_sort(char** w_s_data,long long w_s_num_lines,long long w_s_file_size,string w_s_outfilename,vector<double>&write_time_spent,int w_index,vector<double>&sort_time_spent,int s_index)
{

    sort_file(w_s_data,w_s_num_lines,sort_time_spent,s_index);

    File_Operations *outfile = new File_Operations(w_s_outfilename,w_s_file_size,1); //writing to file//NOTE: Actually file_size will be input file size by number of threads.
    outfile->setNumOfDataLines((long long)(w_s_file_size/length_of_line));
    outfile->write_file(w_s_data,w_s_num_lines,write_time_spent,w_index);
}


//This needs list of files and output file name
class Merge_File
{
    string outfile;
    vector<string> inputfilenames;
    public:
    Merge_File(vector<string> ifile,string ofile)
    {
        inputfilenames = ifile;
        outfile = ofile;
    }
    void merge_file()
    {

        chrono::high_resolution_clock::time_point merge_start = chrono::high_resolution_clock::now();

        ifstream infile[inputfilenames.size()];
        ofstream ofile;
        long num_in_files =inputfilenames.size();
        int line_length = 200;
        //Input files
        for (size_t i = 0; i < num_in_files; i++)
        {
            infile[i].open(inputfilenames.at(i),ios::binary);
        }
        ofile.open(outfile,ios::binary);
        MinHeapNode harr[num_in_files];
        priority_queue<MinHeapNode, vector<MinHeapNode>, comparing> pq;
        int i = 0;
        for (i = 0; i < num_in_files; i++)
        {
            char* line = (char*)malloc(line_length);
            
            chrono::high_resolution_clock::time_point t1 = chrono::high_resolution_clock::now();
            infile[i].read(line,line_length);
            chrono::high_resolution_clock::time_point t2 = chrono::high_resolution_clock::now();
            chrono::duration<double> time_span = chrono::duration_cast<chrono::duration<double>>(t2 - t1);
            tot_read_time += time_span.count();
            harr[i].element = line;
            harr[i].i = i;
            pq.push(harr[i]);
        }
        int count = 0;
        while (count != num_in_files)
        {
            MinHeapNode root = pq.top();

            pq.pop();
            chrono::high_resolution_clock::time_point w1 = chrono::high_resolution_clock::now();
            ofile.write(root.element,line_length);
            
            chrono::high_resolution_clock::time_point w2 = chrono::high_resolution_clock::now();
            chrono::duration<double> write_time_span = chrono::duration_cast<chrono::duration<double>>(w2 - w1);
            tot_write_time += write_time_span.count();
            delete root.element;
            char* line = (char*)malloc(line_length);
            i = root.i;
            if(infile[i].peek() == EOF)
            {
                count++;
                if (remove(inputfilenames[i].data()) != 0){
                        string msg = inputfilenames[i] + " File deletion failed";
                        cout<<msg<<endl;
                }
                continue;
            }
            chrono::high_resolution_clock::time_point r1 = chrono::high_resolution_clock::now();
            infile[i].read(line,line_length);
            chrono::high_resolution_clock::time_point r2 = chrono::high_resolution_clock::now();
            chrono::duration<double> read_time_span = chrono::duration_cast<chrono::duration<double>>(r2 - r1);
            tot_read_time += read_time_span.count();
            
            root.element = line;
            pq.push(root);
        }

        chrono::high_resolution_clock::time_point merge_end = chrono::high_resolution_clock::now();
        chrono::duration<double> merge_time_span = chrono::duration_cast<chrono::duration<double>>(merge_end - merge_start);
        tot_merge_time = merge_time_span.count();

    }
};

class Int_Ext_Sort
{
    int type;
    File_Operations *inputFile;
    Details* opt;
    public:
    Int_Ext_Sort(File_Operations* input,Details* options)
    {
        
        inputFile = input;
        if(inputFile->getFileSize() > memory_size)
        {
            type = 1;//EXTERNAL_SORTING
        }
        else
        {
            type = 0; //INTERNAL_SORTING
        }
        opt = options;
    }

    int sorting()
    {
        inputFile->open_file();
        //Read the data into memory
        long long num_lines = inputFile->getNumOfLines();
        long long file_size = inputFile->getFileSize();
        switch (type)
        {
            case 0:
            {
                vector<double> read_time(1,0);
                vector<double> write_time(1,0);
                vector<double> sort_time(1,0);
                char** data = inputFile->read_file(num_lines,0,std::ref(read_time),0);
                tot_read_time = read_time[0];
                string outfilename = "sorted-data";
                int index=0;
                write_sort(data,num_lines,file_size,outfilename,std::ref(write_time),index,std::ref(sort_time),index);
                    
                tot_write_time = write_time[0];
                tot_sort_time = sort_time[0];
            }
            break;
            case 1:
            {
                int total_num_threads = opt->getNumOfThreads();
                long long c_size = (long long)(memory_size/(total_num_threads*length_of_line))*length_of_line;
                long long num_lines_chunk = (long long)c_size/length_of_line;
                long long lines_sorted = num_lines_chunk*total_num_threads;
                int num_ite = (int)(file_size/(length_of_line*lines_sorted));
                num_ite += 1;
                long long lines_left = num_lines;
                long long p = 0;
                thread myThreads[total_num_threads];
                int it = 0;
                vector<string> temp_files;
                int rem = false;
                vector<double> read_time(total_num_threads,0);
                vector<double> write_time(total_num_threads,0);
                vector<double> sort_time(total_num_threads,0);
                    
                while(it < num_ite)
                {
                    fill(read_time.begin(),read_time.end(),0);
                    fill(write_time.begin(),write_time.end(),0);
                    fill(sort_time.begin(),sort_time.end(),0);
                    int numOfThreadsRunning = 0;
                    for (int i = 0; i < total_num_threads; i++)
                    {
                        if(lines_left > 0)
                        {
                            char **data;
                            //Read num lines per chunk from the input file
                            if(lines_left>num_lines_chunk){
                                data = inputFile->read_file(num_lines_chunk,p,std::ref(read_time),i);
                            }
                            else
                            {
                                data = inputFile->read_file(lines_left,p,std::ref(read_time),i);
                                rem = true;
                            }
                            string outfilename = "file_"+to_string(it)+"_"+ to_string(i);
                            temp_files.push_back(outfilename);
                            long long file_size = num_lines_chunk*length_of_line;
                            if(rem == false)
                            {
                                myThreads[i] = thread(write_sort,data,num_lines_chunk,file_size,outfilename,std::ref(write_time),i,std::ref(sort_time),i);
                                numOfThreadsRunning++;
                            }
                            else
                            {
                                myThreads[i] = thread(write_sort,data,lines_left,file_size,outfilename,std::ref(write_time),i,std::ref(sort_time),i);
                                numOfThreadsRunning++;
                            }
                            p+=num_lines_chunk;
                            lines_left -=num_lines_chunk;
                        }
                        else
                        {
                            break;
                        }
                    }

                    for (size_t i = 0; i < numOfThreadsRunning; i++)
                    {
                        myThreads[i].join();
                    }
                    //Merge all read times
                    double iteration_read_time = accumulate(read_time.begin(),read_time.end(),0.0);
                    double iteration_write_time = accumulate(write_time.begin(),write_time.end(),0.0);
                    double iteration_sort_time = accumulate(sort_time.begin(),sort_time.end(),0.0);
               
                    tot_read_time += (iteration_read_time/numOfThreadsRunning);
                    tot_write_time += (iteration_write_time/numOfThreadsRunning);
                    tot_sort_time += (iteration_sort_time/numOfThreadsRunning);
                    it++;
                    //leftover_lines -= lines_sorted_per_iteration;
                }

                if (remove(inputFile->getFileName().data()) != 0)
                {
                    perror("File deletion failed");
                }
                //Now call the operation of min heap
                string outfile = "sorted-data";
                Merge_File* fm = new Merge_File(temp_files,outfile);
                fm->merge_file();
            }
            
            break;
            default:
                break;
        }
        return 0;
    }

};


long long FileSize(std::string p_fileName)
{
    FILE *p_file = NULL;
    p_file = fopen(p_fileName.c_str(),"r");
    fseek(p_file,0,SEEK_END);
    long long p_size = ftell(p_file);
    fclose(p_file);
    return p_size;
}

int main(int argc,char *argv[])
{

    double m_time = 0;
    chrono::high_resolution_clock::time_point m_start = chrono::high_resolution_clock::now();
    
    Details *opt = new Details(argc,argv);
    if(!opt->file_verification())
    {
        std::cout<<"Incorrect Usage"<<std::endl;
        std::cout<<"Usage: ./ms.out -F [filename] -t [number of threads]"<<std::endl;
        exit(0);
    };

    long long file_size = FileSize(opt->getFileName());
    File_Operations *fh = new File_Operations(opt->getFileName(),file_size,0);//reading from file

    Int_Ext_Sort* ct = new Int_Ext_Sort(fh,opt);
    ct->sorting();


    cout<<"Total Read time : "<<tot_read_time<<endl;
    cout<<"Total Write time : "<<tot_write_time<<endl;
    cout<<"Total Sort time : "<<tot_sort_time<<endl;
    cout<<"Total Merge time : "<<tot_merge_time<<endl;

    cout<<"file size is "<<file_size<<endl;
    double w_speed = 0;
    double r_speed = 0;
    
    if(file_size > memory_size)
    {
        w_speed =  (file_size*2)/(tot_write_time*mb);
        r_speed = (file_size*2)/(tot_read_time*mb);
    }
    else
    {
        w_speed =  (file_size)/(tot_write_time*mb);
        r_speed = (file_size)/(tot_read_time*mb);
    }

    double sort_speed = (file_size)/(tot_sort_time*mb);

    cout<<"Read speed : "<<r_speed<<" MBPS"<<endl;
    cout<<"Write speed : "<<w_speed<<" MBPS"<<endl;
    cout<<"Sort speed : "<<sort_speed<<" MBPS"<<endl;

    chrono::high_resolution_clock::time_point m_end = chrono::high_resolution_clock::now();
    chrono::duration<double> m_time_span = chrono::duration_cast<chrono::duration<double>>(m_end - m_start);
    m_time = m_time_span.count();

    double s_speed = (file_size)/(m_time*mb);
    cout<<"Main routine time is: "<<m_time<<endl;
    cout<<"MySort speed : "<<s_speed<<" MBPS"<<endl;

    return 0;
}

