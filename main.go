package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

type res struct {
	min, sum, max, count int64
}

func parseChunk(data []byte) map[string]*res {
	result := make(map[string]*res, 500)

	strBuff := string(data)

	start := 0
	end := 0

	for i, char := range strBuff {
		switch char {
		case ';':
			end = i
		case '\n':
			city := strBuff[start:end]
			val := parseFloat(strBuff[end+1 : i])

			if item, ok := result[city]; ok {
				item.min = min(item.min, val)
				item.max = max(item.max, val)
				item.sum += val
				item.count++
			} else {
				result[city] = &res{min: val, max: val, sum: val, count: 1}
			}

			start = i + 1
		}
	}
	return result
}

func parseFloat(data string) int64 {
	var res int64
	var mult int64 = 1
	if data[0] == '-' {
		mult = -1
		data = data[1:]
	}
	switch len(data) {
	case 3:
		res = int64(data[0])*10 + int64(data[2]) - int64('0')*11
	case 4:
		res = int64(data[0])*100 + int64(data[1])*10 + int64(data[3]) - (int64('0') * 111)
	}

	return mult * res
}

func main() {
	defer func(t time.Time) {
		fmt.Println("Execution time: ", time.Since(t))
	}(time.Now())

	goRoutineCount := runtime.NumCPU()
	chunkStream := make(chan []byte, 1000)
	resultStream := make(chan map[string]*res, 1000)

	mp := make(map[string]*res)

	var wg sync.WaitGroup

	for i := 0; i < goRoutineCount-1; i++ {
		wg.Add(1)
		go func() {
			for data := range chunkStream {
				resultStream <- parseChunk(data)
			}
			wg.Done()
		}()
	}

	file, err := os.Open("1brc/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	chunkSize := 8 * 1024 * 1024

	go func() {
		buf := make([]byte, chunkSize)
		leftOver := make([]byte, 0, chunkSize)
		for {
			readTotal, err := file.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				panic(err)
			}
			buf = buf[:readTotal]

			toSend := make([]byte, readTotal)
			copy(toSend, buf)

			lastNewLineIndex := bytes.LastIndexByte(buf, '\n')

			toSend = append(leftOver, buf[:lastNewLineIndex+1]...)

			leftOver = make([]byte, len(buf[lastNewLineIndex+1:]))
			copy(leftOver, buf[lastNewLineIndex+1:])
			chunkStream <- toSend

		}

		close(chunkStream)
		wg.Wait()
		close(resultStream)
	}()

	for data := range resultStream {
		for key, val := range data {
			if item, ok := mp[key]; ok {
				item.min = min(item.min, val.min)
				item.max = max(item.max, val.max)
				item.sum += val.sum
				item.count += val.count
			} else {
				mp[key] = val
			}
		}

	}

	//expected := advanced.Advanced()
	//
	//for key, item := range mp {
	//	if val, ok := expected[key]; ok {
	//		if item.min != val.Min || item.max != val.Max || item.sum != val.Sum || item.count != val.Count {
	//			fmt.Printf("Error: %s: min: %d, max: %d, sum: %d, count: %d\n", key, item.min, item.max, item.sum, item.count)
	//		}
	//	} else {
	//		fmt.Printf("Error: %s: min: %d, max: %d, sum: %d, count: %d\n", key, item.min, item.max, item.sum, item.count)
	//	}
	//
	//}
	//var count int64 = 0
	for key, item := range mp {
		//count += item.count
		fmt.Printf("%s: min: %.1f, max: %.1f, avg: %.1f\n", key, float32(item.min)/10, float32(item.sum/item.count)/10, float32(item.max)/10)
	}

	//log.Println("count: ", count)

}
