#pragma once

#include <stdlib.h>
#include <sys/time.h>

class Timer {
	private:

    struct timeval t1;
    struct timeval t2;
    double duration;

	public:

	Timer() : duration(0) {};
	~Timer() {};

	inline void start()
	{
		gettimeofday(&t1, 0);
	}

	inline void stop()
	{
		gettimeofday(&t2, 0);
    	duration += (double)((t2.tv_sec - t1.tv_sec) * 1000000 \
                       + t2.tv_usec - t1.tv_usec) / 1000000;
	}

	inline double report_sec()
	{
    	return duration;
	}
};
