package utils

import (
	"AnomalyDetection/models"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func GetAllChannels() ([]models.Channel,error) {
	client := &http.Client{}
	//var channels []models.Channel
	var responseChannel models.ResponseChannel
	req, err := http.NewRequest(http.MethodGet, "http://10.16.150.132:8010/api-gw/v1/channel/list-all", nil)
	if err !=nil{
		return responseChannel.Data,err
	}
	token,err:=GetToken()
	if err!=nil{
		return nil,nil
	}
	fmt.Printf(token)
	req.Header.Set("Authorization",token)
	req.Header.Set("Accept","application/json")
	resTTTM, err := client.Do(req)
	if err!=nil{
		return responseChannel.Data,err
	}
	data, _ := ioutil.ReadAll(resTTTM.Body)
	err = json.Unmarshal(data,&responseChannel)
	if err!=nil{
		return responseChannel.Data,err
	}
	return responseChannel.Data,nil
}
