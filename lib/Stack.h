#pragma once

#include <assert.h>

class Stack {
public:

	Stack () : nr_elems(0) {};

	inline void push(void *elem)
	{
		assert(nr_elems < STACK_LENGTH);
		elems[nr_elems++] = elem;
	}
	
	inline void *pop()
	{
		return (nr_elems == 0) ? NULL : elems[--nr_elems];
	}
	
	inline void reset()
	{
		nr_elems = 0;
	}
	
	inline int size()
	{
		return nr_elems;
	}

private:

	static const int STACK_LENGTH = 10000;
	void *elems[STACK_LENGTH];
	int nr_elems;
};
