package main

import (
	"github.com/phpyandong/gdisruptor"
	"fmt"
)
const (
	BufferSize   = 1024 * 64
	BufferMask   = BufferSize - 1
	Iterations   = 128 * 1024 * 32
	Reservations = 1 //预定个数
)

var ringBuffer = [BufferSize]int64{}
//自定义消费者
type MyCustomer struct {
}
func(cunstomer MyCustomer) Consume(lower,upper int64){
	fmt.Println("cunsume init")
	for ; lower<= upper;lower++{

		message := ringBuffer[lower&BufferMask]
		fmt.Println("消费消息 :",message)
		if message != lower {
			panic(fmt.Errorf("race contition %d %d",message,lower))
		}
	}
}

func main() {
	mydisruptor := gdisruptor.NewDisruptorOptions(
		gdisruptor.WithConsumerGroup(MyCustomer{}),
		gdisruptor.WithCapacity(1024),
	)
	go publish(mydisruptor)
	mydisruptor.Read()
}
func publish(myDisruptor gdisruptor.Disruptor){
	fmt.Println("publish init..")
	for sequence := int64(1);sequence <=Iterations;{

		sequence = myDisruptor.Reserve(Reservations)
		for lower := sequence - Reservations +1 ;lower <= sequence;lower++ {
			ringBuffer[lower&BufferMask] = lower
		}
		fmt.Println("push消息:",sequence)
		myDisruptor.Commit(sequence-Reservations+1,sequence)

	}

}