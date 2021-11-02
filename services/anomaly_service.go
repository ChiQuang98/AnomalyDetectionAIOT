package services

import (
	"AnomalyDetection/models"
	"AnomalyDetection/workers"
)

func AnomalyChannel(anomaly_cfg *models.AnomalyChannel) (int, []byte) {
	if anomaly_cfg.Type == 1{
		workers.PushJobToChannel(*anomaly_cfg)
	} else if anomaly_cfg.Type == 2{
		 go workers.PushKillSignalChannelKafka(anomaly_cfg.ChannelID)
		 go workers.PushKillSignalChannelNats(anomaly_cfg.ChannelID)
	}
	//fmt.Println(<-jobsChannel)
	return 200,[]byte("2")
}
