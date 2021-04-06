#include <iostream>
#include <string>
#include <cstring>
#include "quicksort.h"
#include <vector>
#include <queue>
#include <numeric>

using namespace std;  
  

void swapping(char **p,char **q)
{
    char* temp = *p;
    *p = *q;
    *q = temp;
}

long long division(char** ar,long low, long high)
{
    char* pi = ar[high];
    long long i = low-1;
    string pv = string(pi);
    for (long long j = low; j < high; j++)
    {
        string k = string(ar[j]);
        
        if(k.compare(0,10,pv) < 0)
        {
    
            i++;
            swapping(&ar[i],&ar[j]);
        }
    }
    swap(ar[i+1],ar[high]);
    return i+1;
}   

//sorting function
void QSortfunc(char** ar,long long low,long long high)
{
    if(low<high)
    {
        long long d = division(ar,low,high);
        QSortfunc(ar,low,d-1);
        QSortfunc(ar,d+1,high);
    }
}

void QuickSort(char** ar,long long low,long long high)
{
    QSortfunc(ar,low,high);
}


void outf(string ar[], long long size)
{  
    int i;  
    for (i = 0; i < size; i++)  
        cout << ar[i] << " ";
    cout << endl;  
}
