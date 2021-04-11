在 SparkSQL 中，Catalog 主要用于各种函数信息 和 元数据信息（DB，table，view，partition 和 函数）的统一管理。
SessionCatalog 是 abstract class，有两个具体class : InMemoryCatalog 和 HiveExternalCatalog，有时间可以看看这两个类的具体的实现