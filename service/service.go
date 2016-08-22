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

/* program read messages from kafka queue, validate  and store result in kafka */
func main() {
	addr := os.Getenv("KAFKA_ADDR")
	if addr == "" {
		addr = "localhost:9092"
	}
	fmt.Println("kafka addr = " + addr)
	Serve(addr, validate)
}

/* message validation function */
func validate(body []byte) []byte {
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

type ProcessorFunc func([]byte) []byte

/* service cycle goroutine: read messages, call processor, store result */
func Serve(addr string, processor ProcessorFunc) {
	var (
		consumer sarama.Consumer
		producer sarama.AsyncProducer
		err      error
	)
	/* connection loop for kafka (if it's turned off) */
	for producer == nil {
		producer, err = sarama.NewAsyncProducer([]string{addr}, nil)
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(5000 * time.Millisecond)
		}
	}
	/* disconnect on exit */
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
		/* It appear when topic is not exists. No reconnection is needed. It's fatal. */
		panic(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	/* channel for application exit signal */
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Println("serve loop")
	/* consumer loop */
ConsumerLoop:
	for {
		select {
		/* message consumed */
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			/* process message */
			result := processPack(msg.Value, processor)
			if result != nil {
				/* send result */
				msg := sarama.ProducerMessage{Topic: "output", Value: sarama.ByteEncoder(result)}
				producer.Input() <- &msg
			}
		/* exit signal */
		case <-signals:
			break ConsumerLoop
		}
	}
}

/*
extract id and body
process body
assemble and return response
*/
func processPack(value []byte, processor ProcessorFunc) []byte {
	/*
		first 16 bytes of message - uuid (id for calback)
		residue - body of message
	*/
	if len(value) < 17 {
		log.Printf("Consumed message wrong length %d\n", len(value))
		return nil
	}
	/* extract id */
	id, err := uuid.FromBytes(value[0:16])
	if err != nil {
		log.Printf("Consumed message wrong uuid %v\n", err.Error())
		return nil
	}
	/* extract body */
	body := value[16:]
	/* process */
	result := processor(body)
	/* assemble package (id + body) */
	pack := append(id.Bytes(), result...)
	return pack

}
