package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

func parseChunk(data []byte, mp map[string]*res) {
	prev := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			name, val := parseString(data[prev:i])
			prev = i + 1

			data, ok := mp[string(name)]
			if !ok {
				data = &res{}
				mp[string(name)] = data
			}

			data.min = min(data.min, val)
			data.max = max(data.max, val)
			data.sum += val
			data.count++
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

	mp := make(map[string]*res)

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

		parseChunk(toSend, mp)

	}

	for k, v := range mp {
		fmt.Printf("%s: min: %.1f, max: %.1f, avg: %.1f\n", k, float32(v.min)/10, float32(v.max)/10, float32(v.sum/v.count)/10)
	}

}
