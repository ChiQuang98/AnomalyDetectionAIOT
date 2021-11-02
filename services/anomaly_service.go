package services

import (
	"AnomalyDetection/models"
	"AnomalyDetection/workers"
	"fmt"
	"strings"
)

func AnomalyChannel(anomaly_cfg *models.AnomalyChannel) (int, []byte) {
	if anomaly_cfg.Type == 1{
		workers.PushJobToChannel(*anomaly_cfg)
	} else if anomaly_cfg.Type == 2{
		 go workers.PushKillSignalChannelKafka(anomaly_cfg.ChannelID)
		 go workers.PushKillSignalChannelNats(anomaly_cfg.ChannelID)
		 topicAnomalyTable:=fmt.Sprintf("%sTABLEANOMALY3SECONDS",strings.ReplaceAll(strings.ToUpper(anomaly_cfg.ChannelID),"-",""))
		 fmt.Println("ENOAMASMAS",topicAnomalyTable)
		 go workers.PushKillSignalKafkaConsumerAnomaly(topicAnomalyTable)
	}
	//fmt.Println(<-jobsChannel)
	return 200,[]byte("2")
}
