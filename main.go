package main

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"time"
)

const (
	maxWorkStation = 10000
)

func parseChunk(data []byte, cm *customMap) {
	prev := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			name, val := parseString(data[prev:i])
			prev = i + 1

			arrayData := cm.get(name)

			if arrayData == nil {
				arrayData = &node{}
			}

			cm.put(name, val)

		}
	}
}

func parseString(data []byte) ([]byte, int32) {
	for i := len(data) - 1; ; i-- {
		if data[i] == ';' {
			return data[:i], parseFloat(data[i+1:])
		}
	}

}

func parseFloat(data []byte) int32 {
	var res int32

	var mult int32 = 1
	i := 0

	if data[0] == '-' {
		mult = -1
		i++
	}

	for ; i < len(data); i++ {
		if data[i] != '.' {
			res = res*10 + int32(data[i]-'0')
		}

	}
	return mult * res
}

type res struct {
	min, sum, max, count int32
}

type node struct {
	key []byte
	res
	next *node
}

type customMap []*node

func (c *customMap) put(key []byte, val int32) {
	index := hash(key) % maxWorkStation
	nd := (*c)[index]

	if nd == nil {
		(*c)[index] = &node{key: key, res: res{min: val, max: val, sum: val, count: 1}}
	} else {
		for nd.next != nil && !bytes.Equal(key, nd.key) {
			nd = nd.next
		}

		if bytes.Equal(key, nd.key) {
			nd.res = res{min: min(nd.min, val), max: max(nd.max, val), sum: nd.sum + val, count: nd.count + 1}
		} else {
			nd.next = &node{key: key, res: res{min: val, max: val, sum: val, count: 1}}
		}
	}
}

func (c *customMap) get(key []byte) *node {
	index := hash(key) % maxWorkStation
	nd := (*c)[index]

	for nd != nil && !bytes.Equal(key, nd.key) {
		nd = nd.next
	}

	if nd != nil {
		return nd
	}

	return nil
}

func hash(s []byte) int {
	h := fnv.New32a()
	h.Write(s)
	return int(h.Sum32())
}

func main() {
	defer func(t time.Time) {
		fmt.Println(time.Since(t))
	}(time.Now())

	file, err := os.Open("1brc/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	chunkSize := 2048 * 1024 * 1024

	buf := make([]byte, chunkSize)
	leftOver := make([]byte, 0, chunkSize)

	mp := make(customMap, maxWorkStation)

	for {
		n, err := file.Read(buf)

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			panic(err)
		}

		buf = buf[:n]

		toSend := make([]byte, n)
		copy(toSend, buf)

		lastNewLineIndex := bytes.LastIndexByte(buf, '\n')

		toSend = append(leftOver, buf[:lastNewLineIndex]...)
		leftOver = make([]byte, len(buf[lastNewLineIndex+1:]))
		copy(leftOver, buf[lastNewLineIndex+1:])

		parseChunk(toSend, &mp)

	}

	for _, item := range mp {
		if item != nil {
			fmt.Printf("%s: min: %.1f, max: %.1f, avg: %.1f\n", item.key, float32(item.min)/10, float32(item.sum/item.count)/10, float32(item.max)/10)
			for item.next != nil {
				item = item.next
				fmt.Printf("%s: min: %.1f, max: %.1f, avg: %.1f\n", item.key, float32(item.min)/10, float32(item.max)/10, float32(item.sum/item.count)/10)
			}
		}
	}

}
