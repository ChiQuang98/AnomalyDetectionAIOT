package ksql_utils

import (
	"AnomalyDetection/models"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
)

func ExecuteStatement(query string) error  {
	bodyksql:=&models.KSQLBody{
		KSQL:             query,
		StreamsProperties: models.StreamsProperties{
			KSQLOffsetReset: "latest",
		},
	}
	client := &http.Client{}
	jsonKSQL,_:= json.Marshal(bodyksql)
	req, err := http.NewRequest(http.MethodPost, "http://192.168.136.134:8088/ksql", bytes.NewBuffer(jsonKSQL))
	if err!=nil{
		return err
	}
	req.Header.Set("Content-Type","application/json")
	//req.Header.Set("Accept","application/json")
	resKSQL, err := client.Do(req)
	defer resKSQL.Body.Close()
	if err!=nil{
		return err
	} else {
		data, _ := ioutil.ReadAll(resKSQL.Body)
		if resKSQL.StatusCode == 200{
			fmt.Println("THANH CONG")
			return nil
		} else{
			glog.Error(resKSQL.StatusCode,string(data))
			return errors.New("LOI")
		}
	}
}
