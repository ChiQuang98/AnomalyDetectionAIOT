package services

import (
	"AnomalyDetection/models"
	"AnomalyDetection/workers"
	"fmt"
	"strings"
)

func AnomalyChannel(anomaly_cfg *models.AnomalyChannel) (int, []byte) {
	wg:=workers.GetWaitGroup()
	if anomaly_cfg.Type == 1{
		workers.PushJobToChannel(*anomaly_cfg)
	} else if anomaly_cfg.Type == 2{
		 for _,channelTopic:= range anomaly_cfg.ChannelID{
			 wg.Add(3)
			 go workers.PushKillSignalChannelKafka(channelTopic,&wg)
			 go workers.PushKillSignalChannelNats(channelTopic,&wg)
			 topicAnomalyTable:=fmt.Sprintf("%sTABLEANOMALY3SECONDS",strings.ReplaceAll(strings.ToUpper(channelTopic),"-",""))
			 go workers.PushKillSignalKafkaConsumerAnomaly(topicAnomalyTable,&wg)
		 }
	}
	//fmt.Println(<-jobsChannel)
	return 200,[]byte("2")
}
