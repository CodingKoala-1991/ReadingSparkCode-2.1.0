## 关于Expression的一些基本说明
Expression 也继承自 TreeNode 体系
### Expression 的主要子 trait
* Nondeterministic，具有不确定性的 Expression
* Unevaluable，不可执行的 Expression，例如 select *，直接列展开
* CodegenFallback，不可以生成代码的 Expression。例如第三方的UDF
* LeafExpression，没有子 Expression 的 Expression
* UnaryExpression，一个子Expression，例如Abs方法
* BinaryExpression，两个子Expression。
例如 like，name LIKE '%xxxx%', name是表的一个字段，也是一个 Expression，后面的字符串也是一个Expression。
又如sort_array函数，两个参数分别为 array 和 升降序标记，有两个参数，也是两个 Expression。
* TernaryExpression，三个子Expression，例如Substring函数
* BinaryOperator，继承自 BinaryExpression。例如 AND，就是 expression1 AND expression2，属于这个情况