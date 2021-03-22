package gdisruptor

import (
	"time"
	"sync/atomic"
	"math"
	"io"
	"sync"
	"runtime"
)
//https://github.com/smartystreets-prototypes/go-disruptor/
//=====interface===
type Writer interface{

	//预定
	Reserve(count int64) int64

	Commit(lower,upper int64)
}
type Reader interface{
	Read()
	Close() error
}
type Consumer interface {
	Consume(lower,upper int64)
}
//屏障
type Barrier interface {
	Load() int64
}
//等待策略
type WaitStrategy interface{
	Gate (int64) //闸门
	Idle(int64) //空闲
}
//=====interface==


type Disruptor struct{
	Writer
	Reader
}
func NewDisruptor(writer Writer,reader Reader) Disruptor{
	return Disruptor{
		Writer :writer,
		Reader:reader,
	}
}

type DefaultWaitStategy struct {}
//新建等待策略
func NewWaitStategy() DefaultWaitStategy{
	return DefaultWaitStategy{}
}
func (defaultWaitStategy DefaultWaitStategy)Gate(count int64) {
	time.Sleep(time.Nanosecond)
}
func (defaultWaitStategy DefaultWaitStategy) Idle(count int64){
	time.Sleep(time.Microsecond * 50)
}
//=======================================================================
//连接器
type Wireup struct{
	waiter 	WaitStrategy
	capacity int64
	consumerGroups [][]Consumer
}
//构建连接器的读写
func (wireup *Wireup)Build()(Writer, Reader) {
	var writerSequence= NewCursor()
	readers, readerBarrier := wireup.buildReaders(writerSequence)
	return NewWriter(writerSequence,readerBarrier,wireup.capacity) , compositeReader(readers) //强转
}

func (wireup *Wireup)buildReaders(writerSequence *Cursor) (readers []Reader,upstream Barrier){
	//writerSequence  写游标外部传递，
	upstream = writerSequence  //最高位置就是此时 写游标 的位置，

	//循环连接器里的消费者列表
	for _,consumerGroup := range wireup.consumerGroups{
		var consumerGroupSequences []*Cursor
		//
		for _, consumer := range consumerGroup{
			//当前序列
			currentSequence := NewCursor()//新建一个空游标，作为当前游标，

			readers = append(readers,NewReader(currentSequence,writerSequence,upstream,wireup.waiter,consumer))
			consumerGroupSequences = append(consumerGroupSequences,currentSequence)
		}
		//多个游标
		upstream = NewComporiteBarrier(consumerGroupSequences...)
	}
	return readers,upstream
}
//=======================================================================
const defaultCursorValue = -1

type Cursor [8]int64 //通过构造64 byte的缓存行大小的data ,来避免错误的共享序列游标
					//保持单独cacheLine，避免两个cpu互相通知缓存失效啊，造成性能的影响

func NewCursor() *Cursor{
	var cursor Cursor
	cursor[0] = defaultCursorValue
	return &cursor
}
func (cursor *Cursor) Store(value int64)  {
	atomic.StoreInt64(&cursor[0],value)
}
func (cursor *Cursor) Load()(int64)  {
	return atomic.LoadInt64(&cursor[0])
}
//=======================================================================

type Option func(*Wireup)

//根据options 创建 disruptor
func NewDisruptorOptions(options ...Option) Disruptor{
	if wireup ,err := NewWireup(options...);err != nil {
		panic(err)
	}else{
		return NewDisruptor(wireup.Build())
	}
}
//给连接器wireup 添加等待策略
func WithWaitStrategy(value WaitStrategy) Option{
	return func(wireup *Wireup) {
		wireup.waiter = value
	}
}
func WithCapacity(value int64) Option{
	return func(wireup *Wireup) {
		wireup.capacity = value
	}
}
//增加一个消费者
func WithConsumerGroup(value ...Consumer) Option{
	return func( wireup *Wireup) {
		wireup.consumerGroups = append(wireup.consumerGroups,value)
	}
}
func NewWireup(options ...Option)(*Wireup,error){
	wireup := &Wireup{}
	//读取多个配置条件
	fun := WithWaitStrategy(NewWaitStategy())
	fun(wireup)
	for _,option := range options{
		option(wireup)
	}
	return wireup,nil

}
//=======================================================================

const (
	stateRunning = iota
	stateClosed
)
type DefaultReader struct {
	state 	int64
	current *Cursor //这个reader 已经 处理成功添加到这个序列
	written *Cursor //这个环型buffer 已经被写入到这个序列
	upstream Barrier //所有的readers 已经都进入到这个队列 多个cursor
	waiter		WaitStrategy//等待策略
	consumer	Consumer //消费者
}
func NewReader(current, written * Cursor,upstream Barrier,waiter WaitStrategy,consumer Consumer) *DefaultReader{
	return &DefaultReader{
		state :stateRunning,
		current :current,
		written:written,
		upstream:upstream,
		waiter :waiter,
		consumer: consumer,

	}
}
func (reader *DefaultReader) Read(){
	var gateCount int64//大门数量
	var idleCount int64//空闲数量
	var lower int64//低位位置
	var upper int64  //高位位置
	var current int64 = reader.current.Load() //当前游标中的数据
	// [1 2 3 4]
	//死循环
	for{
		lower = current+1 //上一个游标里的数加1
		upper = reader.upstream.Load() //获取一个区间最小数据
		if lower <= upper{
			reader.consumer.Consume(lower,upper) //消费这个区间
			reader.current.Store(upper)//reder中的当前读到的位置数据
			current = upper //变量赋值，读到的位置数据 用于继续循环
		}else if upper = reader.written.Load();lower <= upper{
			//写的比读快，也要等
			//读写互斥的意思？正在写的位置大于最低位置了，即写超过读了，就sleep
			gateCount++ //闸机增加
			idleCount = 0
			reader.waiter.Gate(gateCount) //sleep 1 reader 的等待者 增加睡眠时间
		}else if atomic.LoadInt64(&reader.state) == stateRunning{
			//读完了要等
			idleCount ++  //空闲加1
			gateCount = 0 //闸机为0
			reader.waiter.Idle(idleCount) //sleep 50
			//如果 状态还是运行状态 ，slepp
			//此时应该是没有数据了？ todo

		}else{
			break;
		}
	}

	if closer ,ok := reader.consumer.(io.Closer) ;ok{
		_= closer.Close()
	}

}

func (reader *DefaultReader) Close() error{
	atomic.StoreInt64(&reader.state,stateClosed )
	return nil
}

type compositeReader []Reader
func(barrier compositeReader )Read(){
	var waiter sync.WaitGroup
	waiter.Add(len(barrier))
	for _,itemReader := range barrier  {
		go func(reader Reader) {
			reader.Read()
			waiter.Done()
		}(itemReader)
	}
	waiter.Wait()
}
func (readers compositeReader) Close() error{
	for _, reader := range readers{
		_ = reader.Close()
	}
	return nil
}
type DefaultWriter struct{
	written *Cursor //ring buffer 已经写到这个序列中
	upstream Barrier //所有的readers 已经放到这个序列中
	capacity int64 //容量
	previous int64 //上一个处理的数
	gate 	 int64 //闸门 就是读队列的最新的位置【读队列1，2，3，4，5】 gate 值 依次为 12345 等于5时，说说明读完了
}

func NewWriter(written *Cursor,upstream Barrier,capacity int64) *DefaultWriter{
	return &DefaultWriter{
		written:written,
		upstream:upstream,
		capacity:capacity,
		previous:defaultCursorValue,
		gate : defaultCursorValue,
	}
}
//预定一段数组,返回最后一个位置
func(writer *DefaultWriter) Reserve(count int64) int64{
	if count <= 0 {
		panic("err")
	}
	writer.previous += count
	// 【读队列1 , 2 ,3 , 4 , 5 】 [写 6 ,7, 8 ,9 ,10]
	// 当 previous(5+ 5= 10) - 5（读队列找中最小的数为5时说明读完了） = 5 > （5 gate(1,2,3,4,5)）//gate 等于5时跳出循环
	//只有读队列读完了，才可以继续写入 distuptor 的缺点
	for spin := int64(0);writer.previous - writer.capacity > writer.gate ;spin++{
		//强制调度，否则会死掉
		if spin& (1024*16 - 1)  == 0 {
			runtime.Gosched() // LockSupport.parkNanos(1L); http://bit.ly/1xiDINZ
		}
		//取完后 最小值是-1 ？
		writer.gate = writer.upstream.Load() //将读队列一组数据中的最小数据 作为开始
	}
	return writer.previous
}
func (writer *DefaultWriter) Commit(_,upper int64){
	writerCusor := writer.written
	writerCusor.Store(upper)
}


//=======================================================================
type compositeBarrier []*Cursor //混合屏障,作为一次读写一段数据， 有多个游标;一个游标包含一个item
func NewComporiteBarrier(sequences ...*Cursor) Barrier{
	if len(sequences) == 1 {
		//如果只有一个游标，返回当前游标
		return sequences[0]
	}else{
		return compositeBarrier(sequences) //todo
	}
}
//从一段数据中取一个最小数据
func ( barrier compositeBarrier) Load() int64{
	var minimum int64 = math.MaxInt64 //最低限度
	for _,cursor := range barrier{
		if sequence := cursor.Load();sequence < minimum {
			minimum = sequence
		}
	}
	return minimum
}
//=======================================================================
