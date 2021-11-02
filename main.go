package main

import (
	"AnomalyDetection/routers"
	"AnomalyDetection/utils"
	"AnomalyDetection/utils/setting"
	"AnomalyDetection/workers"
	"flag"
	"fmt"
	"github.com/codegangsta/negroni"
	"github.com/golang/glog"
	"github.com/nats-io/nats.go"
	"github.com/rmoff/ksqldb-go"
	"github.com/rs/cors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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

// Set the error handler when creating a connection.

//var channelsAtomic struct {
//	sync.Mutex
//	channels []models.Channel
//}
func main() {
	flag.Parse()
	glog.Info("Init Redis database...")

	ip,err := utils.ResolveHostIp()
	if err !=nil{
		return
	}
	var groupIDKafka = 	fmt.Sprintf("AnomalyLogsGroup_%s",ip)
	hosts := []string{"10.16.150.138", "10.16.150.139", "10.16.150.140"}
	brokers:=utils.SetBroker(hosts,9002)
	//var channelsTmp []models.Channel
	var wg sync.WaitGroup
	//Nats
	//servers := []string{"aiot-app01:31422", "aiot-app02:31422", "aiot-app03:31422"}
	//servers := []string{"10.16.150.138:31422", "10.16.150.139:31422", "10.16.150.140:31422"}
	nc, err := nats.Connect(setting.GetNatsInfo().Host,nats.ErrorHandler(natsErrHandler),nats.PingInterval(20*time.Second), nats.MaxPingsOutstanding(5))
	clientKSQL := ksqldb.NewClient(fmt.Sprintf("http://%s",setting.GetKSQLInfo().Host),"","")
	if err != nil {
		glog.Error(err)
	} else {
		glog.Info(fmt.Sprintf("Connected to Nats Cluster Server at %s","10.16.150.138,139,140:4222"))
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
	// số lượng worker trong pool
	//vi` moi worker lam viec khong ket thuc, phai lang ng  he lien tuc nen so luong worker bang so luong channel
	killsignalKafka := workers.GetKillSignalChannelKafka()
	killsignalNats:= workers.GetKillSignalChannelNats()
	killsignalKafkafconsumerAnomaly:= workers.GetKillSignalKafkaConsumerAnomaly()
	fmt.Println("Start")
	wg.Add(1)
	go func() {
		for job:= range jobs{
			glog.Error("a job created " + job.ChannelID)
			wg.Add(4)
			topicAnomalyTable:=fmt.Sprintf("%sTABLEANOMALY3SECONDS",strings.ReplaceAll(strings.ToUpper(job.ChannelID),"-",""))
			fmt.Println(topicAnomalyTable)
			go workers.WorkerNats(job.ChannelID, job.ChannelID, result,nc,killsignalNats,&wg,clientKSQL)
			go workers.WorkerKafkaProducer(job.ChannelID,result,killsignalKafka,&wg)
			go workers.WorkerCreateStreamKSQL(job,clientKSQL,&wg)
			go workers.WorkerKafkaConsumerAnomalyTable(topicAnomalyTable,topicAnomalyTable,brokers,groupIDKafka,killsignalKafkafconsumerAnomaly,&wg)
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



