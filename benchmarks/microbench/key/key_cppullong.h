#pragma once

#include <iostream>

#ifndef CPPULLONG_KEY_SZ
#define CPPULLONG_KEY_SZ 64
#endif
#define PAD_SZ (CPPULLONG_KEY_SZ - sizeof(unsigned long long))

class key_ullong {
public:
	unsigned long long val;
	char padding[PAD_SZ];

	key_ullong() { this->val = 0; }
	key_ullong(const unsigned long long v) { this->val = v; }

	bool operator < (const key_ullong& k) const { volatile int sum = 0; for (volatile int i=0; i < PAD_SZ; i++) sum += (int)padding[i]; return val < k.val; }
	bool operator > (const key_ullong& k) const { volatile int sum = 0; for (volatile int i=0; i < PAD_SZ; i++) sum += (int)padding[i]; return val > k.val; }
	bool operator ==(const key_ullong& k) const { volatile int sum = 0; for (volatile int i=0; i < PAD_SZ; i++) sum += (int)padding[i]; return val == k.val; }
	bool operator <=(const key_ullong& k) const { volatile int sum = 0; for (volatile int i=0; i < PAD_SZ; i++) sum += (int)padding[i]; return val <= k.val; }
	bool operator >=(const key_ullong& k) const { volatile int sum = 0; for (volatile int i=0; i < PAD_SZ; i++) sum += (int)padding[i]; return val >= k.val; }
	bool operator !=(const key_ullong& k) const { volatile int sum = 0; for (volatile int i=0; i < PAD_SZ; i++) sum += (int)padding[i]; return val != k.val; }

	//> postfix ++ operator
	key_ullong operator++(int) {
		key_ullong old = *this;
		this->val++;
		return old;
	}
	key_ullong operator+(const key_ullong& other) const {
		key_ullong n(this->val + other.val);
		return n;
	}
	key_ullong operator+(int someint) const {
		key_ullong n(this->val + (unsigned long long)someint);
		return n;
	}
	key_ullong operator-(int someint) const {
		key_ullong n(this->val - (unsigned long long)someint);
		return n;
	}
	const key_ullong operator-(const key_ullong& other) const {
		key_ullong n(this->val - other.val);
		return n;
	}

	friend std::ostream &operator<<(std::ostream &output, const key_ullong &k) {
		output << k.val;
		return output;
	}

	operator long long() const { return (long long)val; }
	operator void *() const { return (void *)val; }
};

static const key_ullong __max_key(999999999999999999);
static const key_ullong __min_key(0);

#define MIN_KEY __min_key
#define MAX_KEY __max_key
#define KEY_GET(k, someint) ((k).val = (unsigned long long)someint) 

//> This is necessary because OpenBWTree by Wang uses std::hash<K> in its
//> constructor.
namespace std {
	template <> struct hash<key_ullong>
	{
		size_t operator()(const key_ullong& x) const
		{
			return hash<unsigned long long>()(x.val);
		}
	};
}
