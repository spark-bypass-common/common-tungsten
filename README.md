## common-tungsten
common-tungsten是一个针对Spark SQL的语法糖组件.其有效地屏蔽了开发基于Spark SQL的程序中
需要管理的复杂概念,如`org.apache.spark.sql.Encoder`,`scala.reflect.api.TypeTags.TypeTag`等.
尤其是对于java来说,common-tungsten对琐碎事物的托管,使得开发者可以更加专注于核心功能的开发.

----

### common-tungsten的具体作用
common-tungsten的核心是对`Dataset`的代理:`DataSet`/`JDataSet`.
作为一种代理模式,`DataSet`/`JDataSet`对`Dataset`的功能增强在于:
1. 平滑透明地生成合适的钨丝编码`org.apache.spark.sql.Encoder`;
2. 透明地生成`scala.reflect.api.TypeTags.TypeTag`(**仅针对java**)
3. 封装`Dataset`所没有提供的强类型`XXJoin`方法与`cogroup`方法,方便业务线使用;

### common-tungsten的使用case
scala case:
```scala
def convert(str: String): T = ...
val dataSet = DataSets.readTextFromHdfs[T]("/your-file-path", convert _)
dataSet.filter(record => record != null)
  .leftOuterJoin(anotherDataSet, record => record.toString)

DataSets.writeToHdfs(dataSet, "/your-file-path")
```

java case:
```java
private T parseStr(String str) {...}
JDataSet<T> dataSet = JDataSets.readTextFromHdfs("/your-file-path", this::parseStr)
    .filter((record) -> record != null)
    .leftOuterJoin(anotherJDataSet, T::toString);

JDataSets.writeToHdfs(dataSet,"/your-file-path");
```
