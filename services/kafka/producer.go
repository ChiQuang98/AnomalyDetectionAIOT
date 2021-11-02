package kafka

import (
	"AnomalyDetection/utils/setting"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/glog"
)

func GetProducer() (*kafka.Producer){
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": fmt.Sprintf("%s,%s,%s",setting.GetKafkaInfo().ServerAddress1,
		setting.GetKafkaInfo().ServerAddress2,setting.GetKafkaInfo().ServerAddress3)})
	if err != nil {
		panic(err)
	}
	return p
}
func PublishMessage( value []byte,topic string,p *kafka.Producer){
	err:=p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:         value,
		//Key: []byte("key_"+topic),
	}, nil)
	if err!=nil{
		glog.Error(err)
	}
}

