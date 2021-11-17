#pragma once

#include <iostream>
#include <string>


#ifndef STR_KEY_SZ
#define STR_KEY_SZ 128
#endif

#define STR_HELPER(x) #x
#define TO_STR(x) STR_HELPER(x)
#define SZ (STR_KEY_SZ+1)
#define FORMAT "%0"TO_STR(STR_KEY_SZ)

class key_stdstr {
public:
	std::string val;

	key_stdstr() { this->val = std::string(STR_KEY_SZ, '0'); }
	key_stdstr(const unsigned long long v) {
		this->val = std::to_string(v);
	}

	bool operator <(const key_stdstr& k) const { return  val < k.val; }
	bool operator >(const key_stdstr& k) const { return  val > k.val; }
	bool operator ==(const key_stdstr& k) const { return val == k.val; }
	bool operator <=(const key_stdstr& k) const { return val <= k.val; }
	bool operator >=(const key_stdstr& k) const { return val >= k.val; }
	bool operator !=(const key_stdstr& k) const { return val != k.val; }

	//> postfix ++ operator
	key_stdstr operator++(int) {
		key_stdstr old = *this;
		unsigned long long current = std::stoull(this->val, NULL, 10);
		this->val = std::to_string(current + 1);
		return old;
	}
	key_stdstr operator+(const key_stdstr& other) const {
		unsigned long long n1 = std::stoull(this->val, NULL, 10);
		unsigned long long n2 = std::stoull(other.val, NULL, 10);
		key_stdstr n(n1 + n2);
		return n;
	}
	key_stdstr operator+(int someint) const {
		unsigned long long n1 = std::stoull(this->val, NULL, 10);
		key_stdstr n(n1 + (unsigned long long)someint);
		return n;
	}
	key_stdstr operator-(int someint) const {
		unsigned long long n1 = std::stoull(this->val, NULL, 10);
		key_stdstr n(n1 - (unsigned long long)someint);
		return n;
	}
	const key_stdstr operator-(const key_stdstr& other) const {
		unsigned long long n1 = std::stoull(this->val, NULL, 10);
		unsigned long long n2 = std::stoull(other.val, NULL, 10);
		key_stdstr n(n1 - n2);
		return n;
	}

	friend std::ostream &operator<<(std::ostream &output, const key_stdstr &k) {
		output << k.val;
		return output;
	}

	operator long long() const { return std::stoull(this->val, NULL, 10); }
	operator void *() const { return (void *)std::stoull(this->val, NULL, 10); }
};

static const key_stdstr __max_key(999999999999999999);
static const key_stdstr __min_key(0);

#define MIN_KEY __min_key
#define MAX_KEY __max_key
#define KEY_GET(k, someint) (k.val = std::to_string(someint))
