#include <iostream>
#include <queue>
#include <algorithm>
#include <fstream>
#include <limits>
#include "minheap.h"
#include <cstring>


using namespace std;


bool comparing::operator()(const MinHeapNode l, const MinHeapNode r) const
{
    int out = strcmp(l.element,r.element);

    if(out > 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}
