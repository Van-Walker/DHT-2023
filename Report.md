# DHT Report By wt

_通过 Chord 和 Kademlia 两种算法实现了 P2P 网络_


### P2P 网络
1. 每个节点平等，自我组织的系统
2. 避免中心服务，分布式处理资源
3. 效率不如 CS 网络，但稳定性强
   

### Chord
1. 环结构，倍增思想，定位复杂度 O(log n)
2. 每个节点存前驱、后继表、Finger 表(长度为 m)、数据， 保证后继绝对正确
3. 每个数据存在 key 对应后继节点中，并在该节点的后继备份
4. 新节点加入时通过两步式 Join 保证并发的正确性


### Kademlia
1. 树结构，通过 xor 运算定义距离，定位复杂度 O(log n)
2. 主要结构体：K 桶 和 K 最近表
3. 每个节点存 m 个 K 桶，下标随高度递增
4. 新节点 Join 时先生成 K 最近表，再据此生成 m 个 K 桶
5. 每个数据在 key 的 K 最近表地址对应节点，按照周期重新发布
