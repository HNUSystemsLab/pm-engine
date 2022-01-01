

# NSTRE汇总





## lsm的设计

​	增、删、改、查怎么解决**读放大**同时保证正确性

​	merge过程可否并行化？

​	recover？

​	使用pm heap替代pm file的优势在哪里？多个immut table与sstable的性能差异？

​	

###	LSM：

![image-20210429105532254](/Users/lab/Library/Application Support/typora-user-images/image-20210429105532254.png)

**作者代码中的设计**

主要由四部分构成，

两个B+树索引：memtable 的b+树**pm_map**，和sstable的b+树**off_map**，

文件系统的sstable：存储实际数据的指针

blocks pool：所有record实际存放的地方，这并不是一个显式的数据结构，是随时通过调用分配器接口生成的，通过上面的三个结构对它进行索引



文件系统的sstable，load过程完成后会强制进行一次merge操作，将**pm_map**中的数据全部放入文件系统，memtable的b+树的value是一个指向**实际存储数据**的指针，指向论文中variable-size blocks pool中的数据， sstable的b+树value则是一个文件中的offset，文件中对应offset的内容也是一个指向实际数据的指针，也就是说，查找存储在sstable的实际的record需要：查pm_map的b+树->查off_map的b+树->读文件获取offset数据->得到record指针

merge过程通过全局计数器控制，每次事物执行都会使计数器增加，达到阈值就执行merge(每次事物可能实际包含多个增删改查)

**我改进的设计**：

sstable的b+树value也改为**实际存储数据**的指针,去掉了中间的文件系统索引环节，load数据过程不进行merge操作，只有增，删，改操作扩充了pm_map的size时检查size是否达到了阈值，是则mark为immutable并新建一个pm_map，与他论文中设计的结构基本一致（他的代码中没有实现bloom filter，所以我也没搞这个）改进过后的查找过程：

查pm_map的b+树->查immutable_map的b+树->得到record指针

merge过程通过局部计数器控制，每个table的B+树size达到阈值就mark为immutable，当同一个table的immutable b+树同时存在两个时就进行一次merge，每次merge后size阈值*2



## MAKALU=COW性能下降解释：

​	cow_pbtree.h:mpage. copy部分调用MAKALU**全局锁**，导致多线程情况下大量线程无法完成后续分配，使操作串行化。page size为**4096**，在MAKALU中4096属于大对象分配，是通过锁进行控制的，所以page这个4096的就不对付了。

