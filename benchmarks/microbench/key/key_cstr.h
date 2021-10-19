#pragma once

#include <iostream>
#include <cstring>

#ifndef STR_KEY_SZ
#define STR_KEY_SZ 60
#endif

#define STR_HELPER(x) #x
#define TO_STR(x) STR_HELPER(x)
#define SZ (STR_KEY_SZ+1)
#define FORMAT "%0"TO_STR(STR_KEY_SZ)

class key_cstr {
public:
	char val[SZ];

	key_cstr() { this->val[0] = '\0'; }
	key_cstr(const unsigned long long v) {
		snprintf(this->val, SZ, FORMAT"llu", v);
	}

	bool operator <(const key_cstr& k) const { return (strcmp(val, k.val) < 0); }
	bool operator >(const key_cstr& k) const { return (strcmp(val, k.val) > 0); }
	bool operator ==(const key_cstr& k) const { return (strcmp(val, k.val) == 0); }
	bool operator <=(const key_cstr& k) const { return (strcmp(val, k.val) <= 0); }
	bool operator >=(const key_cstr& k) const { return (strcmp(val, k.val) >= 0); }
	bool operator !=(const key_cstr& k) const { return (strcmp(val, k.val) != 0); }

	//> postfix ++ operator
	key_cstr operator++(int) {
		key_cstr old = *this;
		unsigned long long current = strtoll(this->val, NULL, 10);
		snprintf(this->val, SZ, FORMAT"llu", current+1);
		return old;
	}
	key_cstr operator+(const key_cstr& other) const {
		unsigned long long n1 = strtoll(this->val, NULL, 10);
		unsigned long long n2 = strtoll(other.val, NULL, 10);
		key_cstr n(n1 + n2);
		return n;
	}
	key_cstr operator+(int someint) const {
		unsigned long long n1 = strtoll(this->val, NULL, 10);
		key_cstr n(n1 + (unsigned long long)someint);
		return n;
	}
	key_cstr operator-(int someint) const {
		unsigned long long n1 = strtoll(this->val, NULL, 10);
		key_cstr n(n1 - (unsigned long long)someint);
		return n;
	}
	const key_cstr operator-(const key_cstr& other) const {
		unsigned long long n1 = strtoll(this->val, NULL, 10);
		unsigned long long n2 = strtoll(other.val, NULL, 10);
		key_cstr n(n1 - n2);
		return n;
	}

	friend std::ostream &operator<<(std::ostream &output, const key_cstr &k) {
		output << k.val;
		return output;
	}

	operator long long() const { return strtoll(this->val, NULL, 10); }
	operator void *() const { return (void *)strtoll(this->val, NULL, 10); }
};

static const key_cstr __max_key(999999999999999999);
static const key_cstr __min_key(0);

#define MIN_KEY __min_key
#define MAX_KEY __max_key
#define KEY_GET(k, someint) (snprintf(k.val, SZ, FORMAT"d", someint))

//> This is necessary because OpenBWTree by Wang uses std::hash<K> in its
//> constructor.
namespace std {
	template <> struct hash<key_cstr>
	{
		size_t operator()(const key_cstr& x) const
		{
			return hash<unsigned long long>()(strtoll(x.val, NULL, 10));
		}
	};
}
