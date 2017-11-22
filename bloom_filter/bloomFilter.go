// Package bloom_filter provides bloom filter implementation compatible with Guava (Java library).
 ackage bloom_filter

import (
	"encoding/binary"
	"errors"
	"io"
	"github.com/spaolacci/murmur3"
	"math"
)

type BitArray struct {
	data []uint64
}

func CreateBitArray(bitNum int64) *BitArray {
	remain := bitNum % 64
	longNum := bitNum / 64
	if remain != 0 {
		longNum += 1
	}
	bitArray := BitArray { make([]uint64, longNum) }
	return &bitArray
}

func (bar *BitArray) Set(idx int64) bool {
	dataIdx := idx >> 6
	bitMask := uint64(1) << uint64(idx & 63)
	oldResult := (bar.data[dataIdx] & bitMask) != 0
	bar.data[dataIdx] |= bitMask
	return !oldResult
}

func (bar *BitArray) Get(idx int64) bool {
	dataIdx := idx >> 6
	bitMask := uint64(1) << uint64(idx & 63)
	return (bar.data[dataIdx] & bitMask) != 0
}

func (bar *BitArray) BitsSize() int64 {
	return int64(len(bar.data) * 64)
}

type BloomFilter struct {
	numOfHash	int32
	data 		*BitArray
}

func LoadFromReader(reader io.Reader) (*BloomFilter, error) {
	var strategy byte = 0
	err := binary.Read(reader, binary.BigEndian, &strategy)
	if err != nil {
		return nil, err
	}
	if strategy != 1 {
		return nil, errors.New("Stragtegy of the bloom filter must be 1.")
	}

	var numOfHash byte = 0
	err = binary.Read(reader, binary.BigEndian, &numOfHash)
	if err != nil {
		return nil, err
	}

	dataLength, err := func() (int64, error) {
		// The data length stored in the file is the number of long.
		// It should be multiplied by 8 to be converted into the number of byte.
		var length uint32 = 0
		err := binary.Read(reader, binary.BigEndian, &length)
		if err != nil {
			return 0, err
		}
		return int64(length) * 8, err
	}()
	if err != nil {
		return nil, err
	}

	bitArray := BitArray{make([]uint64, dataLength / 8)}
	for idx, _ := range bitArray.data {
		var data uint64 = 0
		binary.Read(reader, binary.BigEndian, &data)
		bitArray.data[idx] = data
	}

	bloomFilter := BloomFilter { numOfHash: int32(numOfHash), data: &bitArray }
	return &bloomFilter, nil
}

func CreateBloomFilter(expectedInsertions int64, fpp float64) *BloomFilter {
	optimumNumOfBits := int64(float64(-expectedInsertions) * math.Log(fpp) / (math.Log(2) * math.Log(2)))
	optimumNumOfHash := int32(math.Max(float64(1), math.Floor(0.5 + float64(optimumNumOfBits) / float64(expectedInsertions) * math.Log(2))))
	bitArray := CreateBitArray(optimumNumOfBits)
	bloomFilter := BloomFilter { numOfHash: optimumNumOfHash, data: bitArray }
	return &bloomFilter
}

func (bf *BloomFilter) Save(w io.Writer) error {
	// Write strategy
	err := binary.Write(w, binary.BigEndian, byte(1))
	if err != nil {
		return err
	}

	// Write number of hash
	err = binary.Write(w, binary.BigEndian, byte(bf.numOfHash))
	if err != nil {
		return err
	}

	// Write data length
	err = binary.Write(w, binary.BigEndian, uint32(len(bf.data.data)))
	if err != nil {
		return err
	}

	// Write data
	for _, data := range bf.data.data {
		err = binary.Write(w, binary.BigEndian, data)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bf *BloomFilter) MightContain(data []byte) bool {
	hasher := murmur3.New128()
	hasher.Write(data)
	h1, h2 := hasher.Sum128()
	combinedHash := int64(h1)
	for i := bf.numOfHash; i > 0; i-- {
		if !bf.data.Get(combinedHash & int64(0x7fffffffffffffff) % bf.data.BitsSize()) {
			return false
		}
		combinedHash += int64(h2)
	}
	return true
}

func (bf *BloomFilter) Put(data[] byte) bool {
	hasher := murmur3.New128()
	hasher.Write(data)
	h1, h2 := hasher.Sum128()
	combinedHash := int64(h1)
	bitsChanged := false
	for i := bf.numOfHash; i > 0; i-- {
		bitsChanged = bf.data.Set(combinedHash & int64(0x7fffffffffffffff) % bf.data.BitsSize()) || bitsChanged
		combinedHash += int64(h2)
	}
	return bitsChanged
}
