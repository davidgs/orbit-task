/**
 * MIT License
 *
 * Copyright (c) 2021 David G. Simmons
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	camundaclientgo "github.com/citilinkru/camunda-client-go/v2"
	"github.com/citilinkru/camunda-client-go/v2/processor"
	log "github.com/sirupsen/logrus"
)

 const (
	 // DefaultCamundaURL is the default URL for Camunda
	 DefaultCamundaURL = "https://sentiment.camunda.com:8443"
	 // DefaultCamundaUser is the default user for Camunda
	 DefaultCamundaUser = "davidgs"
	 // DefaultCamundaPassword is the default password for Camunda
	 DefaultCamundaPassword = "Toby66.Mime!"
	 // DefaultCamundaClientTimeout is the default timeout for Camunda
	 DefaultCamundaClientTimeout = time.Second * 10
	 // DefaultCamundaProcessDefinition is the default process definition for Camunda
	 DefaultCamundaProcessDefinition = "task-worker"

	 // DefaultOrbitURL is the default URL for Orbit
	 DefaultOrbitURL = "https://app.orbit.love/api/v1/"
	 // DefaultOrbit API key
	 DefaultOrbitAPIKey = "obu_oMdW2GEjOJiTIVxx2AS38l9OM8ptwasx_ddqJ5yA"
	 // DefaultAirtableURL is the default URL for Airtable
	 DefaultAirtableURL = "https://api.airtable.com/v0/"
	 //DefaultAirtableAPIKey is the default API key for Airtable
	 DefaultAirtableAPIKey = "keycNYs4vDaTYWIQQ"
	 // DefaultAirtableBaseID is the default base ID for Airtable
	 DefaultAirtableBaseID = "appk9RU4oePua1fyY"
	 // DefaultAirtableTableName is the default table name for Airtable
	 DefaultAirtableTableName = "Table%201"
 )

 type ProcessData struct {
		Direction string `json:"direction"`
		Items string `json:"items"`
		SortString string `json:"sort_string"`
		WorkplaceSlug string `json:"workplace_slug"`
		OrbitToken string `json:"Orbit_token"`
		BaseID string `json:"base_id"`
		TableName string `json:"table_name"`
		AirtableToken string `json:"airtable_token"`
		OrbitQuery string `json:"Orbit_query"`
}

 type OrbitData struct {
	Data []struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		Attributes struct {
			ID             string    `json:"id"`
			Name           string    `json:"name"`
			Website        string    `json:"website"`
			MembersCount   int       `json:"members_count"`
			EmployeesCount int       `json:"employees_count"`
			LastActive     time.Time `json:"last_active"`
			ActiveSince    time.Time `json:"active_since"`
		} `json:"attributes"`
	} `json:"data"`
	Links struct {
		First string      `json:"first"`
		Last  string      `json:"last"`
		Prev  interface{} `json:"prev"`
		Next  string      `json:"next"`
	} `json:"links"`
}

type AirtableData struct {
	Records []struct {
		Fields struct {
			ID            string `json:"ID"`
			Type string `json:"Type"`
			Name          string `json:"Name"`
			Website       string `json:"Website"`
			MemberCount   int    `json:"MemberCount"`
			EmployeeCount int    `json:"EmployeeCount"`
			LastActive    string `json:"LastActive"`
			ActiveSince   string `json:"ActiveSince"`
		} `json:"fields"`
	} `json:"records"`
}

func main() {
	logger := func(err error) {
		log.Error(err)
	}
	var (
		camundaURL      = DefaultCamundaURL
		camundaUser     = DefaultCamundaUser
		camundaPassword = DefaultCamundaPassword
		// camundaTimeout  = DefaultCamundaClientTimeout
		//camundaProcess  = DefaultCamundaProcessDefinition
	)

	// Parse command line arguments
	for i := 0; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-u":
			camundaURL = os.Args[i+1]
			i++
		case "-U":
			camundaUser = os.Args[i+1]
			i++
		case "-p":
			camundaPassword = os.Args[i+1]
			i++
		// case "-t":
		// 	camundaTimeout, _ = time.ParseDuration(os.Args[i+1])
		// 	i++
		// case "-P":
		// 	camundaProcess = os.Args[i+1]
		// 	i++
		}
	}
	asyncResponseTimeout := 5000
	// Create a new Camunda client
	client := camundaclientgo.NewClient(camundaclientgo.ClientOptions{
		UserAgent:   "",
		EndpointUrl: camundaURL + "/engine-rest",
		Timeout: time.Second * 10,
		ApiUser: camundaUser,
		ApiPassword: camundaPassword,
	},
	)

	// Create a new Camunda processor
	proc := processor.NewProcessor(client, &processor.ProcessorOptions{
		WorkerId:                  "OrbitAirtable",
		LockDuration:              time.Second * 20,
		MaxTasks:                  10,
		MaxParallelTaskPerHandler: 100,
		LongPollingTimeout:        25 * time.Second,
		AsyncResponseTimeout:      &asyncResponseTimeout,
	}, logger)
	log.Debug("Processor started ... ")
	// add a handler for checking the existing Queue
	proc.AddHandler(
		&[]camundaclientgo.QueryFetchAndLockTopic{
			{TopicName: "process_data"},
		},
		func(ctx *processor.Context) error {
			return handleProcess(ctx.Task.Variables, ctx)
		},
	)
	log.Debug("checkQueue Handler started ... ")
	http.HandleFunc("/sentiment", webHandler)
	http.Handle("/", http.FileServer(http.Dir("./static")))
	err := http.ListenAndServe(":9999", nil)
	if err != nil {
		fmt.Println(err)
	}
}

func webHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, %s!", r.URL.Path[1:])
	fmt.Println("Hello")
}

func handleProcess(variables map[string]camundaclientgo.Variable, ctx *processor.Context) error {
	fmt.Println("Processing task ... ")
	fmt.Printf("Variables: %+v\n", variables)
	incoming := ProcessData{}
//	err := json.Unmarshal([]byte(variables), &incoming)
	varb := ctx.Task.Variables
	incoming.Direction = fmt.Sprintf("%+v", varb["direction"].Value)
	incoming.Items = fmt.Sprintf("%+v", varb["items"].Value)
	incoming.SortString = fmt.Sprintf("%+v", varb["sort_string"].Value)
	incoming.WorkplaceSlug = fmt.Sprintf("%+v", varb["workplace_slug"].Value)
	incoming.OrbitToken = fmt.Sprintf("%+v", varb["Orbit_token"].Value)
	incoming.BaseID = fmt.Sprintf("%+v", varb["base_id"].Value)
	incoming.TableName = fmt.Sprintf("%+v", varb["table_name"].Value)
	incoming.AirtableToken = fmt.Sprintf("%+v", varb["airtable_token"].Value)
	incoming.OrbitQuery = fmt.Sprintf("%+v", varb["Orbit_query"].Value)
	fmt.Println("Processing task ... ")
	fmt.Printf("Proper Variables: %+v\n", incoming)

	orbitData, err := getOrbitData(incoming)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Data: %v\n", orbitData)
	err = processData(orbitData, incoming)
	if err != nil {
		fmt.Println(err)
	}
	// Get the Camunda variables
	err = ctx.Complete(processor.QueryComplete{Variables: &varb})
		if err != nil {
			log.Error("queuStatus: ", err)
			return err
		}
		return nil
}

func getOrbitData(incoming ProcessData) (OrbitData, error){
	var orbitData OrbitData
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Jar:     nil,
		Timeout: DefaultCamundaClientTimeout,
	}
	// Create a new HTTP request
	reqUrl := fmt.Sprintf("https://app.orbit.love/v1/%s/organizations?", incoming.WorkplaceSlug)
	if incoming.Direction != "" {
		reqUrl =  fmt.Sprintf("%sdirection=%s", reqUrl, incoming.Direction)
	}
	if incoming.Items != "" {
		if incoming.Direction != "" {
			reqUrl += "&"
		}
		reqUrl = fmt.Sprintf("%sitems=%s", reqUrl, incoming.Items)
	}
	if incoming.SortString != "" {
		if incoming.Direction != "" || incoming.Items != "" {
			reqUrl += "&"
		}
		reqUrl = fmt.Sprintf("%ssort_string=%s", reqUrl,  incoming.SortString)
	}
	fmt.Println(reqUrl)
	req, err := http.NewRequest("GET", reqUrl, nil)
	if err != nil {
		return orbitData, err
	}
	req.Header.Add("Authorization", "Bearer "+ incoming.OrbitToken)
	// Get orbit data
	response, err := httpClient.Do(req)
	if err != nil {
		return orbitData, err
	}

	// Parse orbit data
	err = json.NewDecoder(response.Body).Decode(&orbitData)
	if err != nil {
		return orbitData, err
	}

	return orbitData, nil
}

func getAirtableData(httpClient *http.Client) (AirtableData, error) {
	var airtableData AirtableData

	// Get orbit data from Airtable
	response, err := http.Get("https://api.airtable.com/v0/app4qgZqoqpvj8z7R/Orbit?api_key=key0vY8W7RZ1XKj9X")
	if err != nil {
		return airtableData, err
	}
	// Parse orbit data from Airtable
	err = json.NewDecoder(response.Body).Decode(&airtableData)
	if err != nil {
		return airtableData, err
	}
	return airtableData, nil
}

func processData(orbitData OrbitData, incoming ProcessData) ( error) {
	airtableData := AirtableData{}
	// fmt.Println("Base ID: ", base_id)
	// fmt.Println("Table Name: ", table_name)
	recordCounter := 0;
	for _, data := range orbitData.Data {
		if recordCounter > 9 {
			err := postAirtableData(http.DefaultClient, incoming, airtableData)
			if err != nil {
				return err
			}
			airtableData = AirtableData{}
			recordCounter = 0
		}
		airtableData.Records = append(airtableData.Records, struct {
			Fields struct {
				ID            string `json:"ID"`
				Type string `json:"Type"`
				Name          string `json:"Name"`
				Website       string `json:"Website"`
				MemberCount   int    `json:"MemberCount"`
				EmployeeCount int    `json:"EmployeeCount"`
				LastActive    string `json:"LastActive"`
				ActiveSince   string `json:"ActiveSince"`
			} `json:"fields"`
		}{
			Fields: struct {
				ID            string `json:"ID"`
				Type string `json:"Type"`
				Name          string `json:"Name"`
				Website       string `json:"Website"`
				MemberCount   int    `json:"MemberCount"`
				EmployeeCount int    `json:"EmployeeCount"`
				LastActive    string `json:"LastActive"`
				ActiveSince   string `json:"ActiveSince"`
			}{
				ID:            data.ID,
				Type:          data.Type,
				Name:          data.Attributes.Name,
				Website:       data.Attributes.Website,
				MemberCount:   data.Attributes.MembersCount,
				EmployeeCount: data.Attributes.EmployeesCount,
				LastActive:    data.Attributes.LastActive.Format("2006-01-02"),
				ActiveSince:   data.Attributes.ActiveSince.Format("2006-01-02"),
			},
		})
		recordCounter++
	}
	return nil
}

// func testError() error {
// 	return nil
// }

func postAirtableData(httpClient *http.Client, incoming ProcessData, airtableData AirtableData) (error) {
	// Create a new HTTP request
	x := url.PathEscape(incoming.TableName)
	fmt.Println("Table Name: ", x)
	req, err := http.NewRequest("POST", DefaultAirtableURL + incoming.BaseID + "/" +  x, nil)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", "Bearer " + incoming.AirtableToken)
	if err != nil {
		return err
	}
	fmt.Println("URL: ", req.URL)
	payload, err := json.Marshal(airtableData)
	if err != nil {
		return err
	}
	fmt.Println(string(payload))
	req.Body = ioutil.NopCloser(bytes.NewReader(payload))
	req.Header.Add("Content-Type", "application/json")
	// Get orbit data from Airtable
	response, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	fmt.Println("Status: ", response.Status)
	// Parse orbit data from Airtable
	err = json.NewDecoder(response.Body).Decode(&airtableData)
	if err != nil {
		return err
	}
	fmt.Println(response.Body)
	return nil
}