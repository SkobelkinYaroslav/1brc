package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func parseString(data string) (string, float64) {
	var name string
	var temp float64

	for i, v := range data {
		if v == ';' {
			name = data[:i]
			temp, _ = strconv.ParseFloat(data[i+1:], 64)

			return name, temp
		}
	}

	return "", 0
}

type res struct {
	min   float64
	sum   float64
	max   float64
	count float64
}

func main() {
	t := time.Now()

	file, err := os.Open("1brc/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	mp := make(map[string]*res)

	for scanner.Scan() {
		name, temp := parseString(scanner.Text())

		if _, ok := mp[name]; !ok {
			mp[name] = &res{}
		}

		v := mp[name]

		v.count += 1

		v.sum += temp

		if v.min > temp {
			v.min = temp
		} else if v.max < temp {
			v.max = temp
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	for key, val := range mp {
		fmt.Printf("%s;%f;%f;%f\n", key, val.min, val.sum/val.count, val.max)
	}

	fmt.Println(time.Since(t))
}
