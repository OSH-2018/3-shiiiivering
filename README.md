# 3-shiiiivering
3-shiiiivering created by GitHub Classroom
文件系统采用算法如下：
首先定义一个结构体
struct tags{
    unsigned short is_free;
    void* content;
    my_bid_t bid;
    my_bid_t pre_bid;
    my_bid_t next_bid;
}*tag;
结构体中is_free表示此块是否被使用，同时若被使用则表示此块用于什么用途，content指向对应块的起始地址，下面三个分别表示此块块号，前一个，后一个块的
块号，这样就可以把各个块用链表的方式链接起来。
在init函数中，先为superblock分配一个块，这个块用于保存内存信息，见结构体memstate， 再一次性将所有块所需的tag结构体的内存分配好， 再将其串成一个链表
表示空闲块队列， 每使用一个块就将其从队列中取出，根据需要插入filenode链表或者文件内容的块链表中。
在write和read函数中，使用全局变量以记录上次读写的位置，以便提高读写效率。
