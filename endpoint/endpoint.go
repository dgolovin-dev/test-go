package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
	"io/ioutil"
)

/*

program listen 8000 port and wait user request,
authorize request with basic auth,
send message (request body) to kafka,
receive validation result from kafka and send it to user

*/
func main() {
	addr := os.Getenv("KAFKA_ADDR")
	if addr == "" {
		addr = "localhost:9092"
	}

	sender := Sender{}
	go sender.serve(addr)

	handler := mkValidationFunc(&sender)
	handler = basicAuthDecorator(handler)
	http.HandleFunc("/", handler)

	go http.ListenAndServe(":8000", nil)
	fmt.Println("listen and serve")

	/* for clean exit */
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
}

/* decorator with basic auth */
func basicAuthDecorator(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "admin" || password != "password" {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		} else {
			f(w, r)
		}
	}
}

/* validates request from user with kafka */
func mkValidationFunc(sender *Sender) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		/* read request */
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Println("cann't read request " + err.Error())
			io.WriteString(w, "false")
			return
		} else {
			fmt.Printf("read pkg %v \n", len(body))
		}

		/* send request and receive responce */
		fmt.Printf("send pkg %v \n", len(body))
		res := sender.Send(body)

		/* print and send response to user */
		fmt.Println(string(res))
		w.Write(res)
	}
}

/*

Sender service
 send message to "input" kafka queue
 read response from "output" kafka queue and return

*/
type Sender struct {
	producer          sarama.AsyncProducer
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	callbacks         map[uuid.UUID]chan []byte
	mux               sync.Mutex
}

func (v *Sender) serve(addr string) {
	var err error
	/* connection loop for kafka (if it's turned off) */
	for v.producer == nil {
		v.producer, err = sarama.NewAsyncProducer([]string{addr}, nil)
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(5000 * time.Millisecond)
		}
	}
	defer func() {
		/* disconnect on exit */
		if err := v.producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	for v.consumer == nil {
		v.consumer, err = sarama.NewConsumer([]string{addr}, nil)
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(5000 * time.Millisecond)
		}
	}
	defer func() {
		if err := v.consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	v.partitionConsumer, err = v.consumer.ConsumePartition("output", 0, sarama.OffsetNewest)
	if err != nil {
		/* It appear when topic is not exists. No reconnection is needed. It's fatal. */
		panic(err)
	}
	defer func() {
		if err := v.partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	fmt.Println("kafka connected")

	if v.callbacks == nil {
		v.callbacks = make(map[uuid.UUID]chan []byte)
	}

	/* catch app exit signal */
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	/* callback receiving loop */
ConsumerLoop:
	for {
		select {
		/* callback msg received */
		case msg := <-v.partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			v.processCallbackMsg(msg.Value)
		/* app exit */
		case <-signals:
			break ConsumerLoop
		}
	}
}

/* process callback message from output */
func (v *Sender) processCallbackMsg(value []byte) {
	/*
		first 16 bytes - uuid (callback id)
		residue - body
	*/
	if len(value) < 17 {
		log.Printf("Consumed message skipped with len %d\n ", len(value))
	} else {
		/* extract id  */
		id, err := uuid.FromBytes(value[0:16])
		if err != nil {
			log.Printf("Consumed message wrong uuid" + err.Error())
		} else {
			/* find callback */
			v.mux.Lock()
			cb, ok := v.callbacks[id]
			if !ok {
				log.Printf("Consumed message missed uuid " + id.String())
			} else {
				delete(v.callbacks, id)
			}
			v.mux.Unlock()
			/* extract body of message */
			body := value[16:]
			/* send response to callback chan */
			cb <- body
		}
	}
}

/* send msg to "input" queue and wait response from "output" queue */
func (v *Sender) Send(body []byte) []byte {
	id := uuid.NewV4()

	pack := append(id.Bytes(), body...)
	msg := sarama.ProducerMessage{Topic: "input", Value: sarama.ByteEncoder(pack)}
	callback := make(chan []byte)

	/* register callback chan */
	v.mux.Lock()
	v.callbacks[id] = callback
	v.mux.Unlock()

	v.producer.Input() <- &msg

	return <-callback
}

type Pack struct {
	body []byte
	id   []string
}
