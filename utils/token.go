package utils

import (
	"AnomalyDetection/models"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func GetToken() (string,error){
	//glog.Info("Prepare to repel boarders")
	client := &http.Client{}
	account:= models.Account{
		Email:    "ttcntt@mobifone.vn",
		Password: "12345678",
	}
	jsonAccount, _ := json.Marshal(account)
	req, err := http.NewRequest(http.MethodPost, "http://10.16.150.132:8010/api-gw/v1/user/login", bytes.NewBuffer(jsonAccount))
	if err !=nil{
		fmt.Println("Fail request Set group MCU TTTM")
		//return http.StatusNotAcceptable,nil
		return "",err
	}
	req.Header.Set("Content-Type","application/json")
	//req.Header.Set("Accept","application/json")
	resTTTM, err := client.Do(req)
	if err!=nil{
		return "",err
	}
	data, err := ioutil.ReadAll(resTTTM.Body)
	if err!=nil{
		return "",err
	}
	responseToken := new(models.TokenResponse)
	err = json.Unmarshal(data,&responseToken)
	if err !=nil{
		fmt.Println("Fail request Set group MCU TTTM")
		fmt.Println(err.Error())
		//return http.StatusNotAcceptable,nil
		return "",err
	}
	return responseToken.Token,nil

}
