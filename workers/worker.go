package workers

import (
	"AnomalyDetection/models"
	"AnomalyDetection/services/kafka"
	"AnomalyDetection/utils"
	"encoding/json"
	"fmt"
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
	p:= kafka.GetProducer()
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
			kafka.PublishMessage(s,topic,p)
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
func WorkerKafkaConsumerAnomalyTable(broker utils.Broker,wg *sync.WaitGroup)  {

}