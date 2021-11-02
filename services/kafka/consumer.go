package kafka

import (
	"AnomalyDetection/utils"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Consumer(broker utils.Broker, topic, groupid string) error {
	//fmt.Printf("Starting consumer, 👀 looking for specific message:\n\t%v\n\n", message)
	cm := kafka.ConfigMap{
		"bootstrap.servers":       broker.String(),
		"group.id":                groupid,
		"auto.offset.reset":       "latest",
		"enable.auto.commit":      "true",
		"auto.commit.interval.ms": "1000",
		"session.timeout.ms":      "30000",
	}
	//"enable.partition.eof": true}
	// Variable p holds the new Consumer instance.
	c, e := kafka.NewConsumer(&cm)
	defer c.Close()
	defer fmt.Println("CLOSE")
	// Check for errors in creating the Consumer
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				return fmt.Errorf("😢 Can't create the Consumer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)
			default:
				return fmt.Errorf("😢 Can't create the Consumer (Kafka error code %d)\n\tError: %v", ec, e)
			}
		} else {
			// It's not a kafka.Error
			return fmt.Errorf("😢 Oh noes, there's a generic error creating the Consumer! %v", e.Error())
		}
	} else {
		// make sure that we close the Consumer when we exit

		// Subscribe to the topic
		if e := c.Subscribe(topic, nil); e != nil {
			return fmt.Errorf("☠️ Uh oh, there was an error subscribing to the topic :\n\t%v", e)
		}
		doTerm := false
		for !doTerm {
			ev := c.Poll(1000)
			if ev == nil {
				// the Poll timed out and we got nothin'
				//fmt.Printf("……\n")
				//a, _ := c.Assignment()
				//p, _ := c.Position(a)
				//
				//for _, x := range p {
				//	fmt.Printf("Partition %v position %v\n", x.Partition, x.Offset)
				//}

				continue
			} else {
				switch ev.(type) {
				case *kafka.Message:
					// It's a message
					km := ev.(*kafka.Message)
					//fmt.Printf("✅ Message '%v' received from topic '%v' (partition %d at offset %d)\n")
					fmt.Println("Key: "+string(km.Key)+" | "+string(km.Value))
					//string(*km.TopicPartition.Topic),
					//km.TopicPartition.Partition,
					//km.TopicPartition.Offset)
					//fmt.Println("sa")
					//fmt.println("string(km.Value)")
				}
			}
		}
		fmt.Printf("Subscribed to topic %v", topic)
		return nil
	}

}
