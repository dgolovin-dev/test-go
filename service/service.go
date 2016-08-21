package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	serve("localhost:9092", processor)
}

func processor(body []byte) []byte {
	fmt.Printf("encoded message %v \n", string(body))
	m := Msg{}
	err := json.Unmarshal(body, &m)
	fmt.Printf("decoded message %#v \n", m)
	res := []byte(fmt.Sprintf("%v", err == nil && m.Amount > 0 && m.Address != ""))
	return res
}

type Msg struct {
	Amount  float64
	Address string
}

func serve(addr string, processor func([]byte) []byte) {
	var (
		consumer sarama.Consumer
		producer sarama.AsyncProducer
		err      error
	)
	for producer == nil {
		producer, err = sarama.NewAsyncProducer([]string{addr}, nil)
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(5000 * time.Millisecond)
		}
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	for consumer == nil {
		consumer, err = sarama.NewConsumer([]string{addr}, nil)
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(5000 * time.Millisecond)
		}
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition("input", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Println("serve loop")
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			if len(msg.Value) < 17 {
				log.Printf("Consumed message %d wrong length %d\n", msg.Offset, len(msg.Value))
			} else {
				id, err := uuid.FromBytes(msg.Value[0:16])
				if err != nil {
					log.Printf("Consumed message %d wrong uiid %v\n", msg.Offset, err.Error())
				} else {
					var res []byte = processor(msg.Value[16:])
					pack := append(id.Bytes(), res...)
					msg := sarama.ProducerMessage{Topic: "output", Value: sarama.ByteEncoder(pack)}
					producer.Input() <- &msg
				}
			}
		case <-signals:
			break ConsumerLoop
		}
	}
}
