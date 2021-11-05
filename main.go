package main

import (
	"AnomalyDetection/routers"
	"AnomalyDetection/services/postgresdb_utils"
	"AnomalyDetection/utils"
	"AnomalyDetection/utils/setting"
	"AnomalyDetection/workers"
	"flag"
	"fmt"
	"github.com/codegangsta/negroni"
	"github.com/golang/glog"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/rmoff/ksqldb-go"
	"github.com/rs/cors"
	"net/http"
	"os"
	"strconv"
	"time"
)

func init() {
	//glog
	//create logs folder
	os.Mkdir("./logs", 0777)
	flag.Lookup("stderrthreshold").Value.Set("[INFO|WARN|FATAL]")
	flag.Lookup("logtostderr").Value.Set("false")
	flag.Lookup("alsologtostderr").Value.Set("true")
	flag.Lookup("log_dir").Value.Set("./logs")
	glog.MaxSize = 1024 * 1024 * setting.GetGlogConfig().MaxSize
	flag.Lookup("v").Value.Set(fmt.Sprintf("%d", setting.GetGlogConfig().V))
	flag.Parse()
}

func ConnectNats()  {
	//https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-ubuntu-20-04
}
func natsErrHandler(nc *nats.Conn, sub *nats.Subscription, natsErr error) {
	fmt.Printf("error: %v\n", natsErr)
	if natsErr == nats.ErrSlowConsumer {
		pendingMsgs, _, err := sub.Pending()
		if err != nil {
			fmt.Printf("couldn't get pending messages: %v", err)
			return
		}
		fmt.Printf("Falling behind with %d pending messages on subject %q.\n",
			pendingMsgs, sub.Subject)
		// Log error, notify operations...
	}
	// check for other errors
}
func main() {
	flag.Parse()
	glog.Info("Init Redis database...")
	wg:=workers.GetWaitGroup()
	ip,err := utils.ResolveHostIp()
	if err !=nil{
		glog.Error(err)
		return
	}
	var groupIDKafka = 	fmt.Sprintf("AnomalyLogsGroup_%s",ip)
	hosts := []string{"10.16.150.138", "10.16.150.139", "10.16.150.140"}
	brokers:=utils.SetBroker(hosts,9002)
	//servers := []string{"aiot-app01:31422", "aiot-app02:31422", "aiot-app03:31422"}
	//servers := []string{"10.16.150.138:31422", "10.16.150.139:31422", "10.16.150.140:31422"}
	nc, err := nats.Connect(setting.GetNatsInfo().Host,nats.ErrorHandler(natsErrHandler),nats.PingInterval(20*time.Second), nats.MaxPingsOutstanding(5))
	clientKSQL := ksqldb.NewClient(fmt.Sprintf("http://%s",setting.GetKSQLInfo().Host),"","")
	if err != nil {
		glog.Error(err)
		return
	} else {
		glog.Info(fmt.Sprintf("Connected to Nats Cluster Server at %s","10.16.150.138,139,140:4222"))
	}
	db,err:=postgresdb_utils.SetupDB()
	if err!=nil{
		glog.Error(err)
		return
	}
	e:=nc.Flush()
	if e!=nil{
		glog.Error("Flush error")
	}
	defer nc.Close()
	// queue of jobs
	jobs := workers.GetJobsChannel()
	// done channel lấy ra kết quả của jobs
	result := workers.GetResultChannel()
	//killsignalKafka := workers.GetKillSignalChannelKafka()
	killsignalNats:= workers.GetKillSignalChannelNats()
	killsignalKafkafconsumerAnomaly:= workers.GetKillSignalKafkaConsumerAnomaly()
	fmt.Println("Start")
	wg.Add(2)
	go workers.WorkerCreateStreamKSQL("AnomalyTopic1",clientKSQL,&wg)
	go workers.WorkerKafkaConsumerAnomalyTable("ANOMALYTOPIC1TABLEANOMALY3SECONDS","ANOMALYTOPIC1TABLEANOMALY3SECONDS",
		brokers,groupIDKafka,killsignalKafkafconsumerAnomaly,&wg,db)
	go func() {
		for{
			select {
			case job:= <- jobs:
				wg.Add(1)
				go func() {
					for _,channelTopic:= range job.ChannelID{
						wg.Add(1)
						//topicAnomalyTable:=fmt.Sprintf("%sTABLEANOMALY3SECONDS",strings.ReplaceAll(strings.ToUpper(channelTopic),"-",""))
						go workers.WorkerNats(job.ID,channelTopic, channelTopic, result,nc,killsignalNats,&wg,clientKSQL)
						//go workers.WorkerKafkaProducer(channelTopic,result,killsignalKafka,&wg)
						//go workers.WorkerKafkaConsumerAnomalyTable(topicAnomalyTable,topicAnomalyTable,brokers,groupIDKafka,killsignalKafkafconsumerAnomaly,&wg)
					}
				}()

			}
		}
	}()
	routerApi := routers.InitRoutes()
	nApi := negroni.Classic()
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedHeaders: []string{"*"},
		AllowedMethods: []string{"DELETE", "PUT", "GET", "HEAD", "OPTIONS", "POST"},
	})
	nApi.Use(c)
	nApi.UseHandler(routerApi)
	listenTo := setting.GetRestfulApiHost()+":" + strconv.Itoa(setting.GetRestfulApiPort())
	fmt.Println(listenTo)
	wg.Add(1)
	go http.ListenAndServe(listenTo,nApi)
	wg.Wait()
}



