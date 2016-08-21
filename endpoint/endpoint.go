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
)

func main() {
	addr := os.Getenv("KAFKA_ADDR")
	if addr == "" {
		addr = "localhost:9092"
	}

	sender := Sender{}
	go sender.serve(addr)

	handler := endpoint(&sender)
	handler = basicAuthDecorator(handler)
	http.HandleFunc("/", handler)

	go http.ListenAndServe(":8000", nil)
	fmt.Println("listen and serve")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
}

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

func endpoint(sender *Sender) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		body := make([]byte, 128)
		cnt, _ := r.Body.Read(body)
		if cnt == 0 {
			fmt.Println("cann't read request")
			io.WriteString(w, "false")
			return
		} else {
			fmt.Printf("read pkg %v \n", cnt)
		}
		body = body[0:cnt]

		fmt.Printf("sent pkg %v \n", cnt)
		res := sender.Send(body)

		fmt.Println(string(res))
		w.Write(res)
	}
}

type Sender struct {
	producer          sarama.AsyncProducer
	consumer          sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	callbacks         map[uuid.UUID]chan []byte
	mux               sync.Mutex
}

func (v *Sender) serve(addr string) {
	var err error
	for v.producer == nil {
		v.producer, err = sarama.NewAsyncProducer([]string{addr}, nil)
		if err != nil {
			fmt.Println(err.Error())
			time.Sleep(5000 * time.Millisecond)
		}
	}
	defer func() {
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

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:
	for {
		select {
		case msg := <-v.partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			if len(msg.Value) < 17 {
				log.Printf("Consumed message skipped %d with len %d\n ", msg.Offset, len(msg.Value))
			} else {
				id, err := uuid.FromBytes(msg.Value[0:16])
				if err != nil {
					log.Printf("Consumed message wrong uuid" + err.Error())
				} else {
					v.mux.Lock()
					cb, ok := v.callbacks[id]
					if !ok {
						log.Printf("Consumed message missed uuid " + id.String())
					} else {
						delete(v.callbacks, id)
					}
					v.mux.Unlock()
					cb <- msg.Value[16:]

				}
			}
		case <-signals:
			break ConsumerLoop
		}
	}
}

func (v *Sender) Send(body []byte) []byte {
	id := uuid.NewV4()

	pack := append(id.Bytes(), body...)
	msg := sarama.ProducerMessage{Topic: "input", Value: sarama.ByteEncoder(pack)}
	callback := make(chan []byte)

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
