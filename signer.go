package main

import (
	"fmt"
	"hash/crc32"
	"hash/md5"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...func(<-chan interface{}) <-chan interface{}) {
	input := make(chan interface{})
	for _, job := range jobs {
		output := job(input)
		input = output
	}
}

func SingleHash(in <-chan interface{}) <-chan interface{} {
	out := make(chan interface{})
	var wg sync.WaitGroup

	for data := range in {
		wg.Add(1)
		go func(d interface{}) {
			defer wg.Done()

			dataString := strconv.Itoa(d.(int))
			crc32md5 := DataSignerCrc32(fmt.Sprintf("%x", md5.Sum([]byte(dataString))))
			crc32Data := DataSignerCrc32(dataString)
			out <- fmt.Sprintf("%s~%s", crc32Data, crc32md5)
		}(data)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func MultiHash(in <-chan interface{}) <-chan interface{} {
	out := make(chan interface{})
	var wg sync.WaitGroup

	for data := range in {
		wg.Add(1)
		go func(d interface{}) {
			defer wg.Done()

			var result []string
			for i := 0; i < 6; i++ {
				result = append(result, DataSignerCrc32(strconv.Itoa(i)+d.(string)))
			}

			out <- fmt.Sprintf("%s%s%s%s%s%s", result[0], result[1], result[2], result[3], result[4], result[5])
		}(data)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func CombineResults(in <-chan interface{}) <-chan interface{} {
	out := make(chan interface{})
	var result []string

	for data := range in {
		result = append(result, data.(string))
	}

	sort.Strings(result)
	out <- fmt.Sprintf("%s", strings.Join(result, "_"))
	close(out)

	return out
}

func DataSignerCrc32(data string) string {
	return fmt.Sprintf("%x", crc32.ChecksumIEEE([]byte(data)))
}

func DataSignerMd5(data string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(data)))
}
