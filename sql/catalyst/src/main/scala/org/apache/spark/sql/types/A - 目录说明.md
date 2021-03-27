这个目录下都是 SparkSQL 的数据类型
每一个 DataType 主要存在以下的方法

#### 1 defaultConcreteType
当强制转换一个 null 字面量变成这个 DataType 的时候，转换成的具体 DataType


#### 2 acceptsType
个人理解，就是这个 DataType 能和什么其他什么类型的 DataType 兼容，兼容返回 true

#### 3 simpleString
这个 DataType 的可读格式


## NumericType
NumericType 是数字类型的基类，有两个子类，分别是 小数类型的 FractionalType  和 整数类型的 IntegralType
### 1 FractionalType
#### 1.1 FloatType
默认 4 个字节
#### 1.2 DoubleType
默认 8 个字节
#### 1.3 DecimalType
相当于 Java 中的 BigDecimal
### 2 IntegralType
一共有4种，ByteType、ShortType、IntegerType 和 LongType，默认长度分别为 1 2 4 8 字节
