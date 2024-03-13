package main

import (
	"github.com/andigntv/gohtml/parser"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	routinesLimit := 100

	writerErrorsChan, readerErrorChan := make(chan error), make(chan error)

	dataChan := make(chan map[string]string)

	writerMu := &sync.Mutex{}

	go func() {
		writerMu.Lock()
		defer writerMu.Unlock()
		WriteData(dataChan, "data.csv", writerErrorsChan)
	}()

	limitChan := make(chan struct{}, routinesLimit)

	wg := &sync.WaitGroup{}

	for i := 1; i <= 47000; i++ {
		time.Sleep(100 * time.Millisecond) // to avoid 429 error
		select {
		case <-writerErrorsChan:
			close(readerErrorChan)
		default:
		}
		limitChan <- struct{}{}
		go func() {
			wg.Add(1)
			defer wg.Done()
			CollectData(dataChan, "https://www.auto-data.net/en/automatic-"+strconv.Itoa(i), readerErrorChan)
			<-limitChan
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		close(dataChan)
		writerMu.Lock()
	case <-writerErrorsChan:
		close(readerErrorChan)
	}

}

func CollectData(ch chan<- map[string]string, url string, errChan <-chan error) {
	res := make(map[string]string)

	response, err := http.Get(url)
	if err != nil {
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return
	}
	p, err := parser.New(string(body))
	if err != nil {
		return
	}
	p = p.Find("html").Find("body")
	divs := p.FindAll("div")
	if len(divs) < 2 {
		return
	}
	p = divs[1].Find("table")
	rows := p.FindAll("tr")
	for _, row := range rows {
		fieldP, valueP := row.Find("th"), row.Find("td")
		field, value := fieldP.Find("a").Text(), valueP.Find("a").Text()
		if field == "" {
			field = fieldP.Text()
		}
		if value == "" {
			value = valueP.Text()
		}
		field, value = strings.Split(field, "\r")[0], strings.Split(value, "\r")[0]
		field, value = strings.TrimSpace(field), strings.TrimSpace(value)
		res[field] = value
	}
	select {
	case ch <- res:
		return
	case <-errChan:
		return
	}
}

var fields = [13]string{
	"Brand",
	"Modification (Engine)",
	"Fuel Type",
	"Fuel consumption (economy) - urban",
	"Engine displacement",
	"Power",
	"Torque",
	"Acceleration 0 - 100 km/h",
	"Maximum speed",
	"Length",
	"Width",
	"Height",
	"Kerb Weight",
}

func WriteData(ch <-chan map[string]string, filename string, errChan chan<- error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		errChan <- err
		return
	}
	defer file.Close()

	_, err = file.WriteString("Brand,Model,Fuel Type,Fuel Consumption,Engine volume (L),Power (PS),Torque (N*m),0-100 km/h (s),Max speed (km/h),Length (mm),Width (mm),Height (mm),Weight (kg)\n")

	checkFields := func(data map[string]string) bool {
		for _, field := range fields {
			if _, ok := data[field]; !ok {
				return false
			}
		}
		return true
	}

	for data := range ch {
		if !checkFields(data) {
			continue
		}
		temp := ""
		for _, field := range fields {
			if value, ok := data[field]; ok {
				temp += value + ","
			} else {
				continue
			}
		}
		temp = temp[:len(temp)-1] + "\n"
		_, err = file.WriteString(temp)
		if err != nil {
			errChan <- err
			return
		}
	}
}
