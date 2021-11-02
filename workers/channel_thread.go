package workers

import "AnomalyDetection/models"
var JobsChannelCfg chan models.AnomalyChannel
var Result chan models.MessageNats
var KillsignalKafka chan string
var KillsignalNats chan string
func init() {
	// queue of jobs
	JobsChannelCfg = make(chan models.AnomalyChannel)

	// done channel lấy ra kết quả của jobs
	Result = make(chan models.MessageNats)
	// số lượng worker trong pool
	//vi` moi worker lam viec khong ket thuc, phai lang ng  he lien tuc nen so luong worker bang so luong channel
	KillsignalKafka = make(chan string)
	KillsignalNats = make(chan string)
}
func PushJobToChannel(job models.AnomalyChannel)  {
	JobsChannelCfg <- job
}
func PushResultNatsKafka(result models.MessageNats)  {
	Result <- result
}
func PushKillSignalChannelKafka(topicKill string)  {
	KillsignalKafka <- topicKill
}
func PushKillSignalChannelNats(topicKill string)  {
	KillsignalNats <- topicKill
}
func PollKillSignalChannelKafka() string {
	return <- KillsignalKafka
}
func PollKillSignalChannelNats() string {
	return <- KillsignalNats
}
func PollJobChannelNatsKafka() models.AnomalyChannel  {
	return <- JobsChannelCfg
}
func GetJobsChannel() chan models.AnomalyChannel {
	return JobsChannelCfg
}
func GetResultChannel() chan models.MessageNats {
	return Result
}
func GetKillSignalChannelKafka() chan string {
	return KillsignalKafka
}
func GetKillSignalChannelNats() chan string {
	return KillsignalNats
}