这个目录下入口文件是 commands.scala
RunnableCommand 是所有直接运行的命令的 trait
然后这个目录下绝大部分其他 class 都是实现了 这个 trait
RunnableCommand 这个 trait 是一个 LeafNode 的 LogicalPlan
还有少量几个command 的 LogicalPlan 在其他目录下
直接全局 搜索 "extends RunnableCommand" 就可以找出来所有