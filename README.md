# mongodb-to-mongodb
python3用于mongodb数据库之间倒数据，特别是分片和非分片之间。

本项目是一个集合一个集合的倒.

参考了logstash，对于只增不减而且不修改的集合（基本是最大的集合）可以一直同步，阻塞同步，断点同步，提前同步到最新，可以减少切换时间。改进的地方就是：

1、单线程改成了可以控制线程数量的多线程，可以更快速的同步。

2、增加了更为详细的日志，解决多线程情况下如果程序出错，可以定位断点，然后从断点同步。

3、解决了第一条丢失的问题。

4、"_id"保持ObjectId。
