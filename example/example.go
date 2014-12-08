package main

import (
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"time"

	"github.com/mediocregopher/pubsubch"
)

var channels = []string{"foo", "bar", "baz", "buz"}

func main() {
	log.Println("Dialing subscription connection")
	subConn, err := pubsubch.Dial("localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for pub := range subConn.PublishCh {
			log.Printf("Got publish '%s' from '%s'", pub.Message, pub.Channel)
		}
		log.Println("Subscription connection closed")
	}()

	log.Println("Subscribing")
	if _, err := subConn.Subscribe(channels...); err != nil {
		log.Fatalf("subscribing: %s", err)
	}

	log.Println("Dialing publish channel")
	pubConn, err := redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	for {
		for i := range channels {
			log.Printf("Publishing to %s", channels[i])
			msg := fmt.Sprintf("Hi %s!", channels[i])

			if err := pubConn.Cmd("PUBLISH", channels[i], msg).Err; err != nil {
				log.Fatal(err)
			}
			time.Sleep(1 * time.Second)
		}
	}
}
