## PySpark Document

### Configuration of PySpark

```python
import pyspark 
from pyspark import SparkContext
from pyspark import SparkContext as sc
from pyspark import SparkConf


conf = SparkConf().setAppName("miniProgram").setMaster("local[*]")
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd

rdd.getNumPartitions()

rdd.glom().collect()
```


```python
conf = SparkConf().setAppName("miniProgram").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)
import os
cwd = os.getcwd()
rdd = sc.texrFile("file.txt")
rdd.first()

numbersRDD = sc.parallelize(range(1, 10+1))
print(numbersRDD.collect())

squaresRDD = numbersRDD.map(lambda x: x**2)
print(squaresRDD.collect())

fileredRDD = numbersRDD.filter(lambda x: x % 2 == 0)
print(fileredRDD.collect())
```

### Basic Examples
```python
sentenceRDD = sc.parallelize(["Hello world", "My name is Patrick"])
wordsRDD = sentenceRDD.flatMap(lambda sentence: sentence.split(""))
print(wordsRDD.collect())
print(wordsRDD.count())

def doubleIfOdd(x):
    if x%2 == 1:
        return 2 * x
    else:
        return x
    
numbersRDD = sc.parallelized(range(1, 11))
resultRDD = (numbersRDD
            .map(doubleIfOdd)             # 对每个元素调用 doubleIfOdd 函数
            .filter(lambda x: x>6)        # 取大于6的元素
            .distinct())                  # 去重
resultRDD.collect()
```

### 对 key-value 类型进行 RDD 操作的函数
* `reduceByKey()`: 对所有有着相同key的items执行reduce操作
* `groupByKey()`: 返回类似(key, listOfValues)元组的RDD，后面的value List 是同一个key下面的
* `sortByKey()`: 按照key排序
* `countByKey()`: 按照key去对item个数进行统计
* `collectAsMap()`: 和collect有些类似，但是返回的是k-v的字典


```python
import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf
conf = SparkConf().setAppName("APP_Name").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

rdd = sc.parallelize(["Hello YANAN",
                      "Hello Yanan",
                      "hello yanan",
                      "Yanan",
                      "Yanan ZHOU"])
resultRDD = (rdd
             .flatMap(lambda sentence: sentence.split())  # Split the sentence and flatten
             .map(lambda word: word.lower())              # perform str.lower() for each word
             .map(lambda word: (word, 1))                 # perform the mapping word => (word, 1) for each word
             .reduceByKey(lambda x, y: x + y)             # perform reduce by key，add value with same key
             )
resultRDD.collect()                    # [("hello", 3), ("yanan", 5), ("zhou", 1)]


result = resultRDD.collectAsMap()      # collectAsMap 类似 collect 以 k-v 字典的形式返回
result                                 # {'hello': 3, "yanan": 4}

(resultRDD
 .sortByKey(ascending=True)            # sortByKey 按照 key 进行排列
 # .sortBy(lambda x:x[0], ascending=True)   # sortByKey 用法是一样的
 .take(2))                             # 取前两


print(resultRDD
      .sortBy(lambda x: x[1], ascending=True)   # 将 resultRDD 按照1个，也就是 value 进行排序
      .take(2)
)
```

### RDD 之间的操作
* rdd1.union(rdd2): 所有rdd1和rdd2中的item组合（并集）
* rdd1.intersection(rdd2): rdd1 和 rdd2的交集
* rdd1.substract(rdd2): 所有在rdd1中但不在rdd2中的item（差集）
* rdd1.cartesian(rdd2): rdd1 和 rdd2中所有的元素笛卡尔乘积（正交和）

```python
homeRDD = sc.parallelize([
    ("Shenzhen", "Daniel"),
    ("Shenzhen", "Tom"),
    ("Beijing", "Bon"),
    ("Shanghai", "Alice")
])

lifeQualityRDD = sc.parallelize([
    ("Shenzhen", 10),
    ("Shanghai", 8),
    ("Beijing", 4)
])

homeRDD.join(lifeQualityRDD).collect()
homeRDD.leftOuterJoin(lifeQualityRDD).collect()
homeRDD.rightOuterJoin(lifeQualityRDD).collect()
homeRDD.cogroup(lifeQualityRDD).collect()
(homeRDD
 .cogroup(lifeQualityRDD)
 .map(lambda x: (x[0], (list(x[1][0]), list(x[1][1]))))
 .collect()
)
```

### Spark 的特点
* Spark的一个核心概念是惰性计算。
* 当你把一个RDD转换成另一个的时候，这个转换不会立即生效执行！！！
* Spark会把它先记在心里，等到真的有actions需要取转换结果时，才会重新组织transformations(因为可能有一连串的变换)。
* 这样可以避免不必要的中间结果存储和通信。

### RDD 的 action 的操作

* collect(): 计算所有的items并返回所有的结果到driver端，接着 collect()会以Python list的形式返回结果
* first(): 和上面是类似的，不过只返回第1个item
* take(n): 类似，但是返回n个item
* count(): 计算RDD中item的个数
* top(n): 返回头n个items，按照自然结果排序
* reduce(): 对RDD中的items做聚合



```python
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local[*]").master("duqu").getOrCreate()
sc = spark.sparkContext

row = sc.textFile("File.txt")                # 使用本地读取
row = sc.textFile("file:///data/file.txt")   # 使用文件系统
row = sc.textFile("hdfs:///data/file.txt")   # 使用 HDFS 系统

data = [
    ('Alex','male',3),
    ('Nancy','female',6),
    ['Jack','male',1,80]      # 这里长短不一样
]
rdd = sc.parallelize(data)

intRDD = sc.parallelize([1, 2, 3, 4, 5])
stringRDD = sc.parallelize(['APPLE', 'Orange', 'Grape', 'Banana','Apple'])
print(intRDD.collect())
print (stringRDD.collect())
```


### Map / MapValue 操作
```python
intRDD_2 = intRDD.map(intRDD.map(lambda x: x+1))
intRDD_2.collect()

dictRDD = sc.parallelize([
    ("a", ["apple", "banana", "lemon"]), 
    ("b", ["grapes",'pig'])
    ])

def f(x):
    return len(x)

dictRDD_r = dictRDD.mapValues(f).collect()      # mapValues(fun) 内添加的函数fun是对key对应的value进行操作的
print(dictRDD_r.collect())                      # [('a', 3), ('b', 2)]
```

### filter 操作

```python
print(intRDD.filter(lambda x: x<3).collect())   # [1, 2]
print(stringRDD.filter(lambda x: "ra" in x).collect())  #['Orange', 'Grape']
```

### distinct 操作
```python
intRDD_overlapped = sc.parallelize([1, 2, 2, 3, 3, 3, 3, 2])
print(intRDD_overlapped.distinct().collect())   #  [1, 2, 3]
```

### randomSplit运算，将RDD进行切分
```python
sRDD = intRDD.randomSplit([0.4, 0.6])
print(len(sRDD))
print(sRDD[0].collect())
print(sRDD[1].collect())
```


### groupBy 和 groupByKey
```python
result = intRDD.groupBy(lambda x: x % 2).collect()
print(sorted(([x, sorted(y)]) for (x, y) in result))

dictRDD = sc.parallelize([
    ("Daniel", 100),
    ("Alice", 90),
    ("Bob", 80),
    ("Daniel", 95)
])
(dictRDD.groupByKey().mapValues(len))
```

* 在使用 RDD.groupByKey() 之后数据会变成以 value 为 list 形式的键值对
```
 {
     "Daniel": [100, 85],
     "Alice":[90],
     "Bob":[90]
 }
```
 RDD.mapValue(fun) 相当于对行程的字典中的 value 进行fun的操作


### map 和 flatMap
* flatMap 相当于在 flat 之后要进行一次 flatten 拍平的操作
```python
RDD = sc.parallelize(['i am a student','i am a teacher'])
rdd1 = RDD.map(lambda x:x.split(' '))       # [['i', 'am', 'a', 'student'], ['i', 'am', 'a', 'teacher']]
rdd2 = RDD.flatMap(lambda x:x.split(' '))   # ['i', 'am', 'a', 'student', 'i', 'am', 'a', 'teacher']
```

### reduce 和 reduceByKey
```python
RDD = sc.parallelize([1, 2, 3, 4])
rdd1 = RDD.reduce(lambda x, y: x * y)
print(rdd1.collect())   # 24

RDD = sc.parallelize(['i am a student', 'i am a teacher'])
rdd1 = (
    RDD
    .flatMap(lambda x:x.split(' '))   # 将每个 item 按照空格切分后拍平成一个list
    .map(lambda x:(x, 1))             # 将每个 item 变成tuple (item, 1)，第一个 item 变成了 key
    )

print(rdd1.collect())                
# [('i', 1), ('am', 1), ('a', 1), ('student', 1), ('i', 1), ('am', 1), ('a', 1), ('teacher', 1)]

rdd2 = (
    rdd1
    .reduceByKey(lambda x,y:x+y)      # 将有相同的 key 的 value 加起来
    )
print(rdd2.collect())
# [('i', 2), ('a', 2), ('teacher', 1), ('am', 2), ('student', 1)]
```

### zip 操作
```python
keylist = [1, 2, 3, 4]
valuelist = ['a', 'b', 'c', 'd']
pair = zip(keylist,valuelist)
rdd = sc.parallelize(pair)
print(rdd.collect())              # [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')]
print(rdd.keys().collect())       # [1, 2, 3, 4]
print(rdd.values().collect())     # ['a', 'b', 'c', 'd']
```

### RDD 转换为 DataFrame
```python
from pyspark.sql.types import StructField, StructType, StringType
spark = SparkSession.builder.master("local").appName("haha").getOrCreate()
sc = spark.sparkContext
data = [('Alex', 'male', 3), 
        ('Nancy','female', 6), 
        ['Jack','male',9]] # mixed

rdd_ = sc.parallelize(data)
schema = StructType([
        StructField("name", StringType(), True),    # true 代表不为空
        StructField("gender", StringType(), True),
        StructField("num", StringType(), True)
    ])
df = spark.createDataFrame(rdd_, schema=schema) 
```

### RDD 之间的计算

```python
intRDD1 = sc.parallelize([3,1,2,5,5])
intRDD2 = sc.parallelize([5,6])
intRDD3 = sc.parallelize([2,7])

# 并集计算, rdd1.union(rdd2)
print(intRDD1.union(intRDD2).union(intRDD3).collect())

# 交集计算, rdd1.intersection(rdd2)
print(intRDD1.intersection(intRDD2).collect())

# 差集计算, rdd1.subtract(rdd2)
print(intRDD1.subtract(intRDD2).collect())

# 笛卡尔积运算, rdd1.cartesian(rdd2)
print(intRDD1.cartesian(intRDD2).collect())
```

### Action 动作运算

```python
intRDD = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 0])

# 取第一条数据
print(intRDD.first())    # 0

# 取前两条数据
print(intRDD.take(2))    # [1, 2]

print(intRDD.stats())    # 统计
print(intRDD.min())      # 最小值
print(intRDD.max())      # 最大值
print(intRDD.stdev())    # 标准差
print(intRDD.count())    # 计数
print(intRDD.sum())      # 求和
print(intRDD.mean())     # 平均
```

### RDD Key-Value 键值对运算

```python
kvRDD = sc.parallelize([
    ("one", 1),
    ("two", 2),
    ("three", 3),
    ("four", 4),
    ("five", 5)
])

print(kvRDD.keys().collect())
print(kvRDD.values().collect())

print(kvRDD.filter(lambda x : x[0] in ("one", "two")).collect())
print(kvRDD.filter(lambda x : x[1] < 3).collect())
```

### 对 value 进行操作

```python
print(kvRDD.mapValues(lambda x:x**2).collect())   # [("one", 1), ("two", 4), ..., ("five", 25)]
print(kvRDD.sortByKey().collect())                # 按照 key 进行排序
print(kvRDD.sortByKey(True).collect())            # 按照 key 进行排序，true 代表正序
print(kvRDD.sortByKey(False).collect())           # 按照 key 进行排序，false 代表倒序
print(kvRDD.reduceByKey(lambda x, y : x + y).collect())    # 将相同的 key 的 value 加在一起
```

