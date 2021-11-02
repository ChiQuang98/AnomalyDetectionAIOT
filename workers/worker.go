package workers

import (
	"AnomalyDetection/models"
	"AnomalyDetection/services/kafka_utils"
	"AnomalyDetection/utils"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/nats-io/nats.go"
	"github.com/rmoff/ksqldb-go"
	"strings"
	"sync"
)
var total uint64
func WorkerKafkaProducer(topikKillSig string,result chan models.MessageNats,ks chan string,wg *sync.WaitGroup){
	defer wg.Done()
	fmt.Println("in kafka worker")
	topicKill:= topikKillSig
	// to consume messages
	p:= kafka_utils.GetProducer()
	for{
		select{
		case msgNats:=<-result:
			//topic := strings.ReplaceAll(strings.ToUpper(msgNats.IdChannel),"-","")
			topic := strings.ToUpper(msgNats.IdChannel)
			topicKill = topic
			jsonSenMl:= models.JSONSenML{Valueksql: json.RawMessage(msgNats.MessageData)}
			s,_:=json.Marshal(jsonSenMl)
			println(string(s))
			//println(string(s))
			kafka_utils.PublishMessage(s,topic,p)
			glog.Info("Tranfered data to topic %s kafka ",topic)
		case signalKill:=<-ks:
			fmt.Println("in kafka worker----")
			if topicKill == signalKill {
				// can xu ly dong kafka neu dung luong nay
				fmt.Println(fmt.Sprintf("Worker %s killed Kafka: ",signalKill))
				return
			}
		}
	}
}
func WorkerNats(job  string, worknumber string, result chan models.MessageNats,nc *nats.Conn,ks chan string,wg *sync.WaitGroup,clientKSQL *ksqldb.Client) {
	fmt.Println(job + "In thread")
	topicKill := job
	streamOriginalName:=strings.ReplaceAll(strings.ToUpper(job),"-","")
	subNats, err := nc.QueueSubscribe("channels."+job,job, func(m *nats.Msg){
		//wg.Done()
		//fmt.Println("Dât",string(m.Data))
		var message models.Message
		err:=proto.Unmarshal(m.Data,&message)
		if err == nil{
			//fmt.Println("=>>>: ",string(message.Payload))
			result <- models.MessageNats{
				IdChannel:   job,
				MessageData: message.Payload,
			}
		} else {
			fmt.Println(err)
		}
		println(string(message.Payload))
		glog.Info(fmt.Sprintf("Received data from topic %s Nats",job))
	});
	if(err!=nil){
		glog.Error(err)
	}
	defer subNats.Unsubscribe()
	defer wg.Done()
	for true{
		select {
		case signalKill:=<-ks:
			if signalKill == topicKill {
				fmt.Println(fmt.Sprintf("Worker %s killed Nats: ",signalKill))
				//Khi nhan duoc lenh k theo doi, thi kill het stream va table cua channel da tao
				err:= clientKSQL.Execute(fmt.Sprintf("DROP TABLE %sTABLEANOMALY3SECONDS;",streamOriginalName))
				if err!=nil{
					fmt.Println("LOI XOA BANG")
					fmt.Println(err)
				} else {
					fmt.Println(fmt.Sprintf("DROP TABLE %sTABLEANOMALY3SECONDS;",streamOriginalName))
					err = clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sEXPLODE;",streamOriginalName))
					if err!=nil{
						fmt.Println("LOI XOA STREAM EXPLODE")
						fmt.Println(err)
					} else{
						fmt.Println(fmt.Sprintf("DROP STREAM %sEXPLODE",streamOriginalName))
						err = clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sORIGINAL;",streamOriginalName))
						if err !=nil{
							fmt.Println("LOI XOA STREAM ORIGINAL")
							fmt.Println(err)
						}
						fmt.Println(fmt.Sprintf("DROP STREAM %sORIGINAL",streamOriginalName))
					}
				}
				return
			}
		}
	}

}
func WorkerCreateStreamKSQL(channelCfg models.AnomalyChannel,clientKSQL *ksqldb.Client,wg *sync.WaitGroup)  {
	defer wg.Done()
	defer fmt.Println("DONE CREATE STREAM KSQL")
	//First stream for get original logs
	fmt.Println("IN create Stream")
	streamOriginalName:=strings.ReplaceAll(strings.ToUpper(channelCfg.ChannelID),"-","")
	fmt.Println(streamOriginalName)
	streamOriginalQuery:=fmt.Sprintf("CREATE STREAM %sORIGINAL (valueksql ARRAY<STRUCT<n VARCHAR, u VARCHAR, v VARCHAR, t VARCHAR>>)" +
		" WITH (KAFKA_TOPIC='%s', KEY_FORMAT='KAFKA', PARTITIONS=5, REPLICAS=2, VALUE_FORMAT='JSON');",streamOriginalName,strings.ToUpper(channelCfg.ChannelID))
	fmt.Println("StreamOriginalQuery: "+streamOriginalQuery)
	err := clientKSQL.Execute(streamOriginalQuery)
	if err!=nil{
		fmt.Println("loi roi")
		fmt.Println(err)
		return
	} else {//neu tao stream dau thanh cong, explode ra stream de count
		fmt.Println("First Stream created")
		explodeStreamQuery:= fmt.Sprintf("CREATE STREAM %sEXPLODE as SELECT " +
			"EXPLODE(valueksql)->n AS n, " +
			"EXPLODE(valueksql)->u AS u, " +
			"EXPLODE(valueksql)->v AS v, " +
			"EXPLODE(valueksql)->t AS t " +
			"FROM %sORIGINAL EMIT CHANGES;",streamOriginalName,streamOriginalName)
		err = clientKSQL.Execute(explodeStreamQuery)
		if err!=nil{
			fmt.Println("loi roi")
			fmt.Println(err)
			clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sORIGINAL",streamOriginalName))
			return
		} else{
			fmt.Println("Second Stream created")
			tableAnomalyDetectionQuery:= fmt.Sprintf("CREATE TABLE %sTABLEANOMALY3SECONDS AS SELECT N,COUNT(*) AS TOTAL " +
				"FROM %sEXPLODE WINDOW TUMBLING (SIZE %d SECONDS) GROUP BY N  HAVING COUNT(*) >%d;",streamOriginalName,streamOriginalName,20,3)
			err = clientKSQL.Execute(tableAnomalyDetectionQuery)
			if err!=nil{
				fmt.Println("loi roi")
				fmt.Println(err)
				clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sEXPLODE",streamOriginalName))
				return
			} else {
				fmt.Println("Table anomaly created")
			}
		}
	}
}
func WorkerKafkaConsumerAnomalyTable(idworker string, topic string,brokers utils.Broker,groupidKafka string, ks chan string,wg *sync.WaitGroup)  {
	defer wg.Done()
	fmt.Println(idworker + " Running....")
	fmt.Println(topic)
	cm := kafka.ConfigMap{
		"bootstrap.servers":       brokers.String(),
		"group.id":                groupidKafka,
		"auto.offset.reset":       "latest",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "1000",
		"session.timeout.ms":      "30000",
	}
	c, e := kafka.NewConsumer(&cm)
	if e = c.Subscribe(topic, nil); e != nil {
		//return fmt.Errorf("☠️ Uh oh, there was an error subscribing to the topic :\n\t%v", e)
		glog.Error("Loi kafka subscribe",e)
		return
	}
	defer c.Close()
	for {
		select {
		case id := <-ks:
			//fmt.Println("Killed Goroutine Kafka consumber ",id)
			if id == idworker {
				//fmt.Println("Killed Goroutine Kafka consumber ",id)
				glog.Info("Killed Goroutine Kafka consumber ",idworker)
				return
			}
		default:
			if e != nil {

			} else {
				ev := c.Poll(1000)
				if ev == nil {
					continue
				} else {
					switch ev.(type) {
					case *kafka.Message:
						// It's a message
						km := ev.(*kafka.Message)
						//fmt.Printf("✅ Message '%v' received from topic '%v' (partition %d at offset %d)\n")
						//fmt.Println("Key: "+string(km.Key)+" | "+string(km.Value))
						keyTable:=fmt.Sprintf("%q",km.Key)
						key:=strings.TrimSpace(keyTable[1:strings.Index(keyTable,"\\")])
						//fmt.Println(keyTable,strings.Index(keyTable,"\\"))
						//fmt.Printf("Keyqq:             %s\n", km.Key)
						fmt.Println("QUANGKEY:",key)
					}
				}
			}
		}
	}
}