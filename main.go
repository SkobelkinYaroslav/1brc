package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"time"
)

func parseString(data string) (string, float64) {
	var name string
	var temp float64

	for i := len(data) - 1; ; i-- {
		if data[i] == ';' {
			name = data[:i]
			temp = parseFloat(data[i+1:])

			return name, temp
		}
	}

	return "", 0
}

func parseFloat(data string) float64 {
	var res float64
	flag := false
	var deg float64

	for i := 0; i < len(data); i++ {
		if data[i] != '.' {
			res = res*10 + float64(data[i]-'0')
		} else if data[i] == '.' && !flag {
			flag = true
			deg = pow(i)
		}

	}
	return res / deg
}

func pow(deg int) float64 {
	var res float64 = 10
	for i := 1; i < deg; i++ {
		res *= 10
	}

	return res
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
			mp[name] = &res{
				min: math.MaxFloat64,
				max: -math.MaxFloat64,
			}
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
