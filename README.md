# gdisruptor
每秒钟承载600万订单级别的无锁并行计算框架-Disruptor
使用go语言编码进行学习
主要核心 
 ringbuffer
 cpu 缓存伪共享，利用填充缓存行的形式，避免了数据修改在各个cpu之间的相互通知失效。
