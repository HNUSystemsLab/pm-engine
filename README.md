# On the Performance Intricacies of Persistent Memory Aware Storage Engines
持久内存感知的存储引擎的性能复杂性问题分析。本项工作对NStore框架尽心给了扩展，在真实的持久内存硬件平台上实现了基于写时复制（CoW），就地更新(In-Place Upate)和Log-structured Merge这三种更新策略的数据库存储引擎，并对它们进行了全面的性能评估。为了探究持久内存分配器对数据库存储引擎性能的影响，我们将4种主流持久内存分配器，**PDMK**开发套件中的**libpmemobj**、**NValloc**、**Makalu**和**nvm_malloc**集成到NStore框架中。
基于该项工作行成的研究论文被IEEE Transactions on Knowledge and Data Engineering (IEEE TKDE)接收。抢先版见本项目中的**TKDE2023Engine.pdf**

# Source code for persistent memory database engine and persistent memory allocator


To run this code, please first compile allocator in **pm_allocator** and then follow the instruction in **pm_engine**.

Remember to change PM path in allocator.hpp
