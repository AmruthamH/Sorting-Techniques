#ifndef MINHEAP_H
#define MINHEAP_H

struct MinHeapNode
{
    // The element to be stored
    char* element;

    // index of the array from which the element is taken
    int i;
};

struct comparing
{
    bool operator()(const MinHeapNode l, const MinHeapNode r) const;
};

#endif
