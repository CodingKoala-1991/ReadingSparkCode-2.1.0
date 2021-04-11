### 概述
Rule 是所有的规则的基类，RuleExecutor 则定义了执行这些 规则的驱动

### Rule
只需要实现 apply 方法就可以，就是这个 具体rule 的执行逻辑。apply 就是接受一个 节点，通过处理逻辑之后返回一个新节点。


### RuleExecutor
所有 树形结构的转换过程，例如 Analyzer 逻辑算子树的分析，Optimizer 逻辑算子树的优化 和 后续物理算子树的生成，依赖的驱动，都继承自 RuleExecutor。有两个重要的属性 和 一个重要的方法：
##### rules：一堆 Rule
##### strategy： 一个 Strategy，要么Once（迭代一次），要么FixedPoint（迭代的最大次数是有限制的）
##### execute 方法：按照 batches 内部 Batch 的顺序，以及 Batch 内 rules 的顺序，按照定好的 strategy，迭代处理传进来的 节点（LogicalPlan 、SparkPlan 和 Expression 之类的）

