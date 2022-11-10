package extendible_hash_table

import (
	"sync"
)

type bucket[K comparable, V any] struct {
	max_size_, depth_ int
	arr_ []pair[K, V]
}

type pair[K any, V any] struct {
	first_ K
	second_ V
}

func newBucket[K comparable, V any](max_size, depth int) *bucket[K, V] {
	this := &bucket[K,V]{}
	this.max_size_ = max_size
	this.depth_ = depth
	return this
}

/* @brief Check if a bucket is full. */
func (this *bucket[K, V]) isFull() bool { return len(this.arr_) >= this.max_size_ }

/* @brief Get the bucket's maximum size. */
func (this *bucket[K, V]) getMaxSize() int { return this.max_size_ }

/* @brief Get the number of keys in the bucket. */
func (this *bucket[K, V]) getSize() int { return len(this.arr_) }

/* @brief Get the local depth of the bucket. */
func (this *bucket[K, V]) getDepth() int { return this.depth_ }

/* @brief Increment the local depth of a bucket. */
func (this *bucket[K, V]) incrementDepth() { this.depth_++ }

func (this *bucket[K, V]) getData() []pair[K, V] { return this.arr_ }

/*
*
* @brief Find the value associated with the given key in the bucket.
* @param key The key to be searched.
* @param[out] value The value associated with the key.
* @return True if the key is found, false otherwise.
*/
func (this *bucket[K, V]) find(key *K, val *V) bool {
	for _, pair := range this.arr_ {
		if pair.first_ == *key {
			*val = pair.second_
			return true
		}
	}
	return false
}

/*
*
* @brief Given the key, remove the corresponding key-value pair in the bucket.
* @param key The key to be deleted.
* @return True if the key exists, false otherwise.
*/
func (this *bucket[K, V]) remove(key *K) bool {
	n := len(this.arr_)
	for i, pair := range this.arr_ {
		if pair.first_ == *key {
			for j := i + 1; j < n; j++ { this.arr_[j-1] = this.arr_[j] }
			this.arr_ = this.arr_[:n-1]
			return true
		}
	}
	return false
}

/**
*
* @brief Insert the given key-value pair into the bucket.
*      1. If a key already exists, the value should be updated.
*      2. If the bucket is full, do nothing and return false.
* @param key The key to be inserted.
* @param value The value to be inserted.
* @param skip Skip searching for the key
* @return True if the key-value pair is inserted, false otherwise.
*/
func (this *bucket[K, V]) insert(key *K, val *V) bool {
	for i := range this.arr_ {
		if this.arr_[i].first_ == *key {
			this.arr_[i].second_ = *val
			return false
		}
	}
	this.arr_ = append(this.arr_, pair[K, V]{*key, *val})
	return true
}


type ExtendibleHashTable[K comparable, V any] struct {
	mu sync.RWMutex
	hash func(key *K) int
	depth_, bucket_size_, num_buckets_, num_keys_ int
	arr_ []*bucket[K, V]
}

func New[K comparable, V any](bucket_size_ int, hash func(key *K) int) *ExtendibleHashTable[K,V] {
	this := &ExtendibleHashTable[K, V]{}
	this.hash = hash
	this.depth_ = 0
	this.bucket_size_ = bucket_size_
	this.num_buckets_ = 1
	this.num_keys_ = 0
	this.arr_ = make([]*bucket[K, V], 1)
	this.arr_[0] = newBucket[K,V](bucket_size_, 0)
	return this
}



/* Some private methods */

/* @brief */
func (this *ExtendibleHashTable[K, V]) indexOf(key *K) int {
	mask := (1 << this.depth_) - 1
	return this.hash(key) & mask
}

/* @brief */
func (this *ExtendibleHashTable[K, V]) getBucket(index int) *bucket[K, V] { return this.arr_[index] }

/* @brief */
func (this *ExtendibleHashTable[K, V]) getLocalDepth(index int) int { return this.arr_[index].depth_ }

/* @brief */
func (this *ExtendibleHashTable[K, V]) incrementGlobalDepth() { this.depth_++ }


/* Some public methods */

/* @brief */
func (this *ExtendibleHashTable[K, V]) GetNumberOfKeys() int { return this.num_keys_ }

/* @brief */
func (this *ExtendibleHashTable[K, V]) GetGlobalDepth() int { return this.depth_ }

/* @brief */
func (this *ExtendibleHashTable[K, V]) GetNumberOfBuckets() int { return this.num_buckets_ }

/* @brief */
func (this *ExtendibleHashTable[K, V]) GetNumberOfDirectories() int { return len(this.arr_) }


/*
@brief Find the value associated with the given key.
Use IndexOf(key) to find the directory index the key hashes to.
@param key The key to be searched.
@param[out] value The value associated with the key.
@return True if the key is found, false otherwise.
*/
func (this *ExtendibleHashTable[K, V]) Find(key *K, val *V) bool { 
	this.mu.RLock()
	defer this.mu.RUnlock()
	i := this.indexOf(key)
	return this.arr_[i].find(key, val)
}

/*
@brief Insert the given key-value pair into the hash table.
If a key already exists, the value should be updated.
If the bucket is full and can't be inserted, do the following steps before retrying:
   1. If the local depth of the bucket is equal to the global depth,
       increment the global depth and double the size of the directory.
   2. Increment the local depth of the bucket.
   3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
@param key The key to be inserted.
@param value The value to be inserted.
*/
func (this *ExtendibleHashTable[K, V]) Insert(key *K, val *V) {
	this.mu.Lock()
	defer this.mu.Unlock()
	i := this.indexOf(key)
	b := this.getBucket(i)
	var pl V
	if b.find(key, &pl) {
		b.insert(key, val)
		return
	}
	if !b.isFull() {
		b.insert(key, val)
		this.num_keys_++
		return
	}
	this.handleFull(i, key, val)
}

/*
@brief Given the key, remove the corresponding key-value pair in the hash table.
Shrink & Combination is not required for this project
@param key The key to be deleted.
@return True if the key exists, false otherwise.
*/
func (this *ExtendibleHashTable[K, V]) Remove(key *K) bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	i := this.indexOf(key)
	removed := this.arr_[i].remove(key)
	if removed { this.num_keys_-- }
	return removed
}

func (this *ExtendibleHashTable[K, V]) handleFull(index int, key *K, val *V) {
	bkt_1 := this.getBucket(index)
	bkt_1.insert(key, val)
	this.num_keys_++

	l_depth := bkt_1.getDepth()
	g_depth := this.GetGlobalDepth()
	if (l_depth == g_depth) {
		this.doubleDirectory()
		this.incrementGlobalDepth()
	}
	bkt_1.incrementDepth()

	bkt_2 := newBucket[K,V](bkt_1.getMaxSize(), l_depth + 1)
	this.num_buckets_++

	var b1, b2 []pair[K, V]
	high_bit := 1 << l_depth
	for _, p := range bkt_1.getData() {
		if (high_bit & this.hash(&p.first_)) == 0 {
			b1 = append(b1, p)
		} else {
			b2 = append(b2, p)
		}
	}

	bkt_1.arr_ = b1
	bkt_2.arr_ = b2

	for i := (high_bit - 1) & index; i < this.GetNumberOfDirectories(); i += high_bit {
		if (i & high_bit) == 0 {
			this.arr_[i] = bkt_1
		} else {
			this.arr_[i] = bkt_2
		}
	}
}

/* @brief double the size of the directory */
func (this *ExtendibleHashTable[K, V]) doubleDirectory() {
	n := len(this.arr_)
	new_arr_ := make([]*bucket[K, V], n * 2)
	copy(new_arr_, this.arr_)
	copy(new_arr_[n:], this.arr_)
	this.arr_ = new_arr_
}