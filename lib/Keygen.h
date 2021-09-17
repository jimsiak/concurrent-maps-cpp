#pragma once

#include <cstdint>
#include <cmath>
#include <cassert>

/**
 * A KeyGenerator is used to generate a series of 64 bit unsigned integers
 * which can then be used to form keys stored inside the map data structures.
 * The only method that is necessary for a KeyGenerator is the next() method
 * which returns the next uint64_t number in the given sequence.
 **/
class KeyGenerator {
public:
	virtual uint64_t next() = 0;
};

class RandomFNV1A {
private:
	uint64_t seed;

public:
	void set_seed(uint64_t seed) { this->seed = seed; }
	
	uint64_t next() {
		uint64_t offset = 14695981039346656037ULL;
		uint64_t prime = 1099511628211;
		uint64_t hash = offset;
		hash ^= seed;
		hash *= prime;
		seed = hash;
		return hash;
	}

	uint64_t next(uint64_t n) { return next() % n; }
};

class KeyGeneratorUniform : public KeyGenerator {
public:
	KeyGeneratorUniform(uint64_t seed, uint64_t max_key)
	{
		this->rng.set_seed(seed);
		this->max_key = max_key;
	}

	uint64_t next()
	{
		return rng.next(max_key);
	}

private:
	RandomFNV1A rng;
	uint64_t max_key;
};

class KeyGeneratorZipf : public KeyGenerator {
public:
	KeyGeneratorZipf(uint64_t seed, uint64_t max_key, uint64_t alpha)
	{
		this->rng.set_seed(seed);
		this->max_key = max_key;
		this->alpha = alpha;

		//> Compute normalization constant
		for (uint64_t i=1; i <= max_key; i++)
		c = c + (1.0 / pow((double) i, alpha));
		c = 1.0 / c;
	}

	uint64_t next() {
		double z; // Uniform random number (0 < z < 1)
		double sum_prob;
		uint64_t zipf_value; // Computed exponential value to be returned
		uint64_t i;
		
		// Pull a uniform random number (0 < z < 1)
		do {
			z = rng.next() / (double)UINT64_MAX;
		} while ((z == 0) || (z == 1));
		
		// Map z to the value
		sum_prob = 0;
		for (i=1; i <= max_key; i++) {
			sum_prob = sum_prob + c / pow((double) i, alpha);
			if (sum_prob >= z)
			{
				zipf_value = i;
				break;
			}
		}
		
		assert((zipf_value >= 1) && (zipf_value <= max_key));
		return (zipf_value-1);
	}

private:
	RandomFNV1A rng;
	double c;
	double alpha;
	uint64_t max_key;
};
