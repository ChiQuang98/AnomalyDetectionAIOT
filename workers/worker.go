package workers

import (
	"AnomalyDetection/models"
	"AnomalyDetection/services/kafka_utils"
	"AnomalyDetection/services/ksql_utils"
	"AnomalyDetection/utils"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/rmoff/ksqldb-go"
	"strings"
	"sync"
	"time"
)
var total uint64
func WorkerKafkaProducer(topikKillSig string,result chan models.MessageNats,ks chan string,wg *sync.WaitGroup){
	defer wg.Done()
	topicKill:= topikKillSig
	// to consume messages
	p:= kafka_utils.GetProducer()
	for{
		select{
		case msgNats:=<-result:
			//topic := strings.ReplaceAll(strings.ToUpper(msgNats.IdChannel),"-","")
			topic := strings.ToUpper(msgNats.IdChannel)
			topicKill = topic
			var senMLarray []models.SenML
			err:= json.Unmarshal([]byte(msgNats.MessageData),&senMLarray)
			if err!=nil{
				glog.Error(err)
			}
			for index,senML:= range senMLarray{
				senMLarray[index].N = senML.N +"_"+ topic
			}
			jsonSenMl:= models.JSONSenML{Valueksql: senMLarray}
			s,_:=json.Marshal(jsonSenMl)
			println(string(s))
			//println(string(s))
			kafka_utils.PublishMessage(s,topic,p)
			glog.Info("Tranfered data to topic %s kafka ",topic)
		case signalKill:=<-ks:
			fmt.Println("in kafka worker----")
			if topicKill == signalKill {
				// can xu ly dong kafka neu dung luong nay
				glog.Info(fmt.Sprintf("Worker %s killed Kafka: ",signalKill))
				return
			}
		}
	}
}
func WorkerNats(id,job  string, worknumber string, result chan models.MessageNats,nc *nats.Conn,ks chan string,wg *sync.WaitGroup,clientKSQL *ksqldb.Client) {
	topicKill := job
	p:= kafka_utils.GetProducer()
	//streamOriginalName:=strings.ReplaceAll(strings.ToUpper(job),"-","")
	subNats, err := nc.QueueSubscribe("channels."+job,job, func(m *nats.Msg){
		var message models.Message
		err:=proto.Unmarshal(m.Data,&message)
		if err != nil{
			//result <- models.MessageNats{
			//	IdChannel:   job,
			//	MessageData: message.Payload,
			//}
			fmt.Println(err)
		} else {
			glog.Info(fmt.Sprintf("Received data from topic %s Nats",job))
			var senMLarray []models.SenML
			err:= json.Unmarshal([]byte( message.Payload),&senMLarray)
			if err!=nil{
				glog.Error(err)
			}
			for index,senML:= range senMLarray{
				//senMLarray[index].N = senML.N +"_"+ job
				senMLarray[index].Channel = job
				senMLarray[index].Key = senML.N+"_"+id+"_"+job
			}
			jsonSenMl:= models.JSONSenML{Valueksql: senMLarray}
			s,_:=json.Marshal(jsonSenMl)
			println(string(s))
			//println(string(s))
			kafka_utils.PublishMessage(s,"ANOMALYTOPIC1",p)
			glog.Info("Tranfered data to topic %s kafka ","ANOMALYTOPIC1")
		}

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
				glog.Info("Killed worker nats")
				//glog.Info(fmt.Sprintf("Worker %s killed Nats: ",signalKill))
				////Khi nhan duoc lenh k theo doi, thi kill het stream va table cua channel da tao
				//err:= clientKSQL.Execute(fmt.Sprintf("DROP TABLE %sTABLEANOMALY3SECONDS;",streamOriginalName))
				//if err!=nil{
				//	glog.Error("Drop Table Err: ",err)
				//}
				//glog.Info(fmt.Sprintf("DROP TABLE %sTABLEANOMALY3SECONDS;",streamOriginalName))
				//err = clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sEXPLODE;",streamOriginalName))
				//if err!=nil{
				//	glog.Error("Drop Stream Err: ",err)
				//}
				//glog.Info(fmt.Sprintf("DROP STREAM %sEXPLODE",streamOriginalName))
				//err = clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sORIGINAL;",streamOriginalName))
				//if err !=nil{
				//	glog.Error("Drop Stream Err: ",err)
				//}
				//glog.Info((fmt.Sprintf("DROP STREAM %sORIGINAL",streamOriginalName)))
				return
			}
		}
	}
}
//func WorkerCreateStreamKSQL(channelCfg models.AnomalyChannel,clientKSQL *ksqldb.Client,wg *sync.WaitGroup)  {
//	defer wg.Done()
//	defer glog.Info("DONE CREATE STREAMS KSQL")
//	for _,channelTopic:= range channelCfg.ChannelID{
//		streamOriginalName:=strings.ReplaceAll(strings.ToUpper(channelTopic),"-","")
//		streamOriginalQuery:=fmt.Sprintf("CREATE STREAM %sORIGINAL (valueksql ARRAY<STRUCT<n VARCHAR, u VARCHAR, v VARCHAR, t VARCHAR>>)" +
//			" WITH (KAFKA_TOPIC='%s', KEY_FORMAT='KAFKA', PARTITIONS=5, REPLICAS=2, VALUE_FORMAT='JSON');",streamOriginalName,strings.ToUpper(channelTopic))
//		err := clientKSQL.Execute(streamOriginalQuery)
//		if err!=nil{
//			glog.Error(err)
//			return
//		} else {//neu tao stream dau thanh cong, explode ra stream de count
//			glog.Info("First Stream created")
//			explodeStreamQuery:= fmt.Sprintf("CREATE STREAM %sEXPLODE as SELECT " +
//				"EXPLODE(valueksql)->n AS n, " +
//				"EXPLODE(valueksql)->u AS u, " +
//				"EXPLODE(valueksql)->v AS v, " +
//				"EXPLODE(valueksql)->t AS t " +
//				"FROM %sORIGINAL EMIT CHANGES;",streamOriginalName,streamOriginalName)
//			err = clientKSQL.Execute(explodeStreamQuery)
//			if err!=nil{
//				glog.Error(err)
//				clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sORIGINAL",streamOriginalName))
//				return
//			} else{
//				glog.Info("Second Stream created")
//				tableAnomalyDetectionQuery:= fmt.Sprintf("CREATE TABLE %sTABLEANOMALY3SECONDS AS SELECT N,COUNT(*) AS TOTAL " +
//					"FROM %sEXPLODE WINDOW TUMBLING (SIZE %d SECONDS) GROUP BY N  HAVING COUNT(*) >%d;",streamOriginalName,streamOriginalName,20,3)
//				err = clientKSQL.Execute(tableAnomalyDetectionQuery)
//				if err!=nil{
//					glog.Error(err)
//					clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sEXPLODE",streamOriginalName))
//					return
//				} else {
//					glog.Info("Table anomaly created")
//				}
//			}
//		}
//	}
//
//}
func WorkerCreateStreamKSQL(topic string,clientKSQL *ksqldb.Client,wg *sync.WaitGroup)  {
	defer wg.Done()
	defer glog.Info("DONE CREATE STREAMS KSQL")
	streamOriginalName:=strings.ReplaceAll(strings.ToUpper(topic),"-","")
	streamOriginalQuery:=fmt.Sprintf("CREATE STREAM %sORIGINAL(valueksql ARRAY<STRUCT<channel VARCHAR,key VARCHAR,n VARCHAR,u VARCHAR,v VARCHAR,t VARCHAR>>)" +
		" WITH (KAFKA_TOPIC='%s', VALUE_FORMAT='JSON');",streamOriginalName,strings.ToUpper(topic))
	err := ksql_utils.ExecuteStatement(streamOriginalQuery)
	if err!=nil{
		glog.Error(err)
		return
	} else {//neu tao stream dau thanh cong, explode ra stream de count
		glog.Info("First Stream created")
		explodeStreamQuery:= fmt.Sprintf("CREATE STREAM %sEXPLODE as SELECT " +
			"EXPLODE(valueksql)->channel AS channel, " +
			"EXPLODE(valueksql)->key AS key, " +
			"EXPLODE(valueksql)->n AS n, " +
			"EXPLODE(valueksql)->u AS u, " +
			"EXPLODE(valueksql)->v AS v," +
			"EXPLODE(valueksql)->t AS t FROM %sORIGINAL EMIT CHANGES;",streamOriginalName,streamOriginalName)
		err = ksql_utils.ExecuteStatement(explodeStreamQuery)
		if err!=nil{
			glog.Error(err)
			clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sORIGINAL",streamOriginalName))
			return
		} else{
			glog.Info("Second Stream created")
			tableAnomalyDetectionQuery:= fmt.Sprintf("CREATE TABLE %sTABLEANOMALY3SECONDS AS SELECT KEY,COUNT(*) AS TOTAL " +
				"FROM %sEXPLODE WINDOW TUMBLING (SIZE %d SECONDS) GROUP BY KEY  HAVING COUNT(*) >%d;",streamOriginalName,streamOriginalName,20,3)
			err = ksql_utils.ExecuteStatement(tableAnomalyDetectionQuery)
			if err!=nil{
				glog.Error(err)
				clientKSQL.Execute(fmt.Sprintf("DROP STREAM %sEXPLODE",streamOriginalName))
				return
			} else {
				glog.Info("Table anomaly created")
			}
		}
	}

}
func WorkerKafkaConsumerAnomalyTable(idworker string, topic string,brokers utils.Broker,groupidKafka string, ks chan string,wg *sync.WaitGroup,db *sql.DB)  {
	defer wg.Done()
	glog.Info(idworker + " Running....")
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
			if id == idworker {
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
						km := ev.(*kafka.Message)
						//fmt.Printf("✅ Message '%v' received from topic '%v' (partition %d at offset %d)\n")
						//fmt.Println("Key: "+string(km.Key)+" | "+string(km.Value))
						keyTable:=fmt.Sprintf("%q",km.Key)
						key:=strings.TrimSpace(keyTable[1:strings.Index(keyTable,"\\")])
						arrKey:=strings.Split(key,"_")
						//fmt.Println(keyTable,strings.Index(keyTable,"\\"))
						//fmt.Printf("Keyqq:             %s\n", km.Key)
						var resultTotalLogs models.ResultAnomalyKSQL
						json.Unmarshal(km.Value,&resultTotalLogs)
						fmt.Print("DeviceID:",key)
						fmt.Print(" | ")
						fmt.Println("Number request:",string(km.Value))
						fmt.Println("Number request:",resultTotalLogs.TOTAL)
						now := time.Now()
						var lastInsertID string
						err:=db.QueryRow("INSERT INTO public.anomaly_result(id, config_id, channel_id, device_id, total_logs, type_anomaly, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7);",
							uuid.NewString(),arrKey[1],arrKey[2],arrKey[0],resultTotalLogs.TOTAL,1, now.Unix()).Scan(&lastInsertID)
						glog.Info(err)
					}
				}
			}
		}
	}
}