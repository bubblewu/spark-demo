package com.bubble.spark

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark中的分布式弹性数据集RDD常用操作
  * 参考：http://spark.apache.org/docs/1.6.2/programming-guide.html#initializing-spark
  *
  * @author wu gang
  * @since 2019-04-09 09:51
  **/
object RDDTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 使用textFile方法获取文件内容，未设置分区数
    val rdd = sc.textFile("data/word_count.txt")
    // 使用textFile方法获取文件内容，设置分区数为6
    val rdd2 = sc.textFile("data/word_count.txt", 6)
    //    partition(rdd)
    //    partition(rdd2)
    //    dependency(rdd)
    //    iterator(sc)
    //    partitioner(rdd)
    //    create(sc)
    //    transformBase(sc, rdd)
    //    transformKey(sc, rdd)
    //    control(sc, rdd)
    action(sc, rdd)

    sc.stop()
  }

  /**
    * RDD分区
    */
  def partition(rdd: RDD[String]): Unit = {
    val count = rdd.partitions.length
    println("分区数目为：" + count)
  }

  /**
    * RDD依赖关系：窄依赖和宽依赖
    */
  def dependency(rdd: RDD[String]): Unit = {
    val wordMap = rdd.flatMap(_.split(" ")).map(x => (x, 1))
    println(wordMap)
    // wordMap的依赖关系为OneToOneDependency，属于窄依赖
    wordMap.dependencies.foreach(dep => {
      println("dependency type:" + dep.getClass)
      println("dependency RDD:" + dep.rdd)
      println("dependency partitions:" + dep.rdd.partitions)
      println("dependency partitions size:" + dep.rdd.partitions.length)
    })
    println("------------------------")
    // 使用reduceBy操作对单词进行计数
    val wordReduce = wordMap.reduceByKey(_ + _)
    println(wordReduce)
    // wordReduce的依赖类型为ShuffleDependency，属于宽依赖
    wordReduce.dependencies.foreach(dep => {
      println("dependency type:" + dep.getClass)
      println("dependency RDD:" + dep.rdd)
      println("dependency partitions:" + dep.rdd.partitions)
      println("dependency partitions size:" + dep.rdd.partitions.length)
    })
  }

  /**
    * RDD分区计算（Iterator）
    */
  def iterator(sc: SparkContext): Unit = {
    val a = sc.parallelize(1 to 9, 3)
    // mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservesPartitioning: Boolean = false): RDD[U]
    // f为输入函数，它处理每个分区里的内容。
    // 每个分区中的内容以Iterator[T]传输给输入函数f，f的输出结果是Iterator[U]。
    // 最终的RDD由所有分区经过输入函数f处理后的结果合并起来构成。
    val data = a.mapPartitions(iterFunc).collect()
    data.foreach(println)
  }

  /**
    * 把分区中的一个元素和它的下一个元素组成一个Tuple，因为分区中最后一个元素没有下一个元素了，
    * 所以(3,4)和(6,7)不在结果里。
    */
  def iterFunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next()
    while (iter.hasNext) {
      val cur = iter.next()
      res ::= (pre, cur)
      pre = cur
    }
    res.iterator
  }

  /**
    * RDD分区函数（Partitioner）:
    * Spark提供两种划分器：HashPartitioner（哈希分区划分器）和RangePartitioner（范围分区划分器）。
    * Partitioner只存在于(K, V)类型的RDD中，对于非(K, V)类型RDD的Partitioner为None
    */
  def partitioner(rdd: RDD[String]): Unit = {
    println("rdd.partitioner: " + rdd.partitioner)
    val hash = rdd.map(x => (x, x)).groupByKey(new org.apache.spark.HashPartitioner(4))
    println("hash.partitioner: " + hash.partitioner)
  }


  /**
    * 创建操作
    */
  def create(sc: SparkContext): Unit = {
    // 一、并行化集合创建操作
    // 使用parallelize操作并行化1到10数据集。
    // 如运行的Spark集群有4个Worker节点，存在4个Executor，故数据集会有4个分区
    val rdd = sc.parallelize(1 to 10)
    rdd.collect().foreach(print)
    println()
    println("rdd partition size: " + rdd.partitions.length)
    println("--------------------------")
    // 显式地设置RDD为4个分区
    val rdd2 = sc.parallelize(1 to 10, 6)
    rdd2.collect().foreach(print)
    println()
    println("rdd2 partition size: " + rdd2.partitions.length)

    // 指定1到10的首选位置为"master"，"slave1"；
    // 11到15的首选位置为"slave2"，"slave3"
    var collect = Seq((1 to 10, Seq("master", "slave1")),
      (11 to 15, Seq("slave2", "slave3")))
    var collectRdd = sc.makeRDD(collect)
    // RDD分区数为2，分区和首选位置分布一致
    println("collectRdd partition size: " + collectRdd.partitions.length)

    println("location 0:")
    val location0 = collectRdd.preferredLocations(collectRdd.partitions(0))
    location0.foreach(print)
    println()
    println("location 1:")
    val location1 = collectRdd.preferredLocations(collectRdd.partitions(1))
    location1.foreach(print)

    // 二、外部存储创建操作
    // 从本地文件创建时，集群所有节点的目录下均要有该文件，否则运行中会报"FileNotFoundException"异常。
    var localRdd = sc.textFile("data/word_count.txt")
    println("local rdd count: " + localRdd.count())
    // 从HDFS读取文件创建RDD
    //    var hdfsRdd = sc.textFile("hdfs://master:9000/test/data.txt")
    //    println("hdfs rdd count: "+ localRdd.count())
  }

  /**
    * 转换操作: 基础转换操作
    */
  def transformBase(sc: SparkContext, rdd: RDD[String]): Unit = {
    // 一、基础转换操作
    /**
      * - map对RDD中对每个元素都执行一个指定的函数来产生一个新的RDD，任何RDD中的元素在新RDD中都有且只有一个与之对应;
      * - flatMap中原RDD中的每个元素可以生成一个或多个元素来构建新的RDD。
      * - distinct，去除重复的元素;
      */
    val data = rdd.map(line => line.split("\\+s")).collect()
    data.foreach(d => d.foreach(println))
    println("----------------")

    val flatMap = rdd.flatMap(_.split("\\+s")).collect()
    flatMap.foreach(println)
    println("----------------")

    val distinctData = rdd.flatMap(line => line.split("\\+s")).distinct().collect()
    distinctData.foreach(println)

    /**
      * - coalesce和repartition都是对RDD进行重新分区。coalesce使用HashPartitioner进行重分区,可以指定是否进行Shuffle.
      * - repartition是coalesce函数第二个参数为true对实现.
      */
    val count = rdd.partitions.length
    println("rdd partitions count:" + count)
    // 重新分区的数目小于原分区的数目，可以正常执行
    val newRDD = rdd.coalesce(1, false)
    println("newRDD partitions count:" + newRDD.partitions.length)
    // 重新分区的数目大于原分区的数目，必须指定shuffle为true，否则分区数目不变
    val newRDD1 = rdd.coalesce(5, true)
    println("newRDD1 partitions count:" + newRDD1.partitions.length)
    val newRDD2 = rdd.coalesce(5, false)
    println("newRDD2 partitions count:" + newRDD2.partitions.length)


    /**
      * - randomSplit操作是根据weights权重来将一个RDD分隔为多个RDD；
      * - glom操作是RDD中每一个分区所有类型为T对数据转变为元素类型为T对数组Array(Array[T]);
      */
    val makeRDD = sc.makeRDD(1 to 10, 10)
    val splitRDDList = makeRDD.randomSplit(Array(1.0, 2.0, 3.0, 4.0))
    println("makeRDD被分隔后对个数：" + splitRDDList.length)
    // 权重参数传入4个，所以有4个分区。权重大对RDD划分对几率比较大。
    // 需注意：权重的总和为1
    splitRDDList(0).collect().foreach(print)
    println()
    splitRDDList(1).collect().foreach(print)
    println()
    splitRDDList(2).collect().foreach(print)
    println()
    splitRDDList(3).collect().foreach(print)
    println()
    // 定义一个3个分区的RDD，使用glom操作将每个分区中的元素放到一个数组中，结果就变为来3个数组
    val RDD = sc.makeRDD(1 to 10, 3)
    val arrays = RDD.glom().collect()
    arrays.foreach(as => {
      as.foreach(a => {
        print(a)
      })
      println("-------")
    })

    /**
      * - union操作，将两个RDD合并，返回两者的并集，返回元素不去重；
      * - intersection操作，类似于SQL中的inner join操作，返回两个RDD的交集，返回元素去重；
      * - subtract操作，返回在RDD中出现，且不在otherRDD中出现的元素，返回元素不去重；
      * 在intersection和subtract操作中参数numPartitions指定返回的RDD的分区数，参数partitioner用于指定分区函数
      */
    var rdd10 = sc.makeRDD(1 to 2, 1)
    var rdd11 = sc.makeRDD(2 to 3, 1)
    rdd10.union(rdd11).collect().foreach(print)
    println()
    rdd10.intersection(rdd11).collect().foreach(print)
    println()
    rdd10.subtract(rdd11).collect().foreach(print)

    /**
      * - mapPartitions操作和map操作类似，只不过映射的参数信息由RDD中的每一个元素变为了RDD中每一个分区的迭代器。
      * 其中参数preservesPartitioning表示是否保留父RDD的partitioner分区信息。
      * 如在映射过程中需要频繁创建额外的对象，使用mapPartitions比map要高效的多。
      * 比如：将RDD中数据通过JDBC写入数据库，如map要为每一个元素都创建一个connection，开销比较大；
      * 使用mapPartitions，只需对每一个分区建一个connection。
      * - mapPartitionsWithIndex类似于mapPartitions，只是输入参数多了一个分区索引
      */
    var rdd21 = sc.makeRDD(1 to 5, 2)
    var rdd22 = rdd21.mapPartitions(x => {
      var result = List[Int]()
      var i = 0
      while (x.hasNext) {
        i += x.next()
      }
      result.::(i).iterator
    })
    rdd22.collect().foreach(println)
    println("rdd22.partitions count: " + rdd22.partitions.length)

    //rdd22将rdd21中每个分区对数字累加，并且在每个区分累加结果前面加了分区对索引
    var rdd23 = rdd21.mapPartitionsWithIndex((x, iter) => {
      var result = List[String]()
      var i = 0
      while (iter.hasNext) {
        i += iter.next()
      }
      result.::(x + "|" + i).iterator
    })
    rdd23.collect().foreach(println)

    /**
      * - zip操作，将两个RDD组合成Key/Value形式对RDD，默认两个RDD对partition数量和元素数量都相同，否则会异常。
      * - zipPartitions操作，将多个RDD按照partitions组合成新的RDD，需要组合对RDD分区数须相同，每个分区内对元素数没要求。
      */
    val rdd31 = sc.makeRDD(1 to 5, 2)
    val rdd32 = sc.makeRDD(Seq("A", "B", "C", "D", "E"), 2)
    rdd31.zip(rdd32).collect().foreach(println)
    // 分区数或元素数不同会异常
    //    val rdd311 = sc.makeRDD(1 to 5, 2)
    //    val rdd322 = sc.makeRDD(Seq("A", "B", "C", "D"), 2)
    //    rdd311.zip(rdd322).collect().foreach(println)

    /**
      * - zipWithIndex操作，将RDD中元素和这个元素在RDD中对索引ID组成键值对；
      * - zipWithUniqueId操作，将RDD中元素和一个唯一ID组合成键值对；该唯一ID对生成算法为：
      * 1. 每个分区中第一个元素对唯一ID值为：该分区对索引ID；
      * 2. 每个分区中第N个元素对唯一ID值为：前一个元素对唯一ID值 + 该RDD总的分区数
      *
      * zipWithIndex需要启动一个Spark作业来计算每个分区的索引ID，而zipWithUniqueId不需要。
      */
    rdd31.zipWithIndex().collect().foreach(println)
    println("-------------------")
    rdd31.zipWithUniqueId().collect().foreach(println)
  }

  /**
    * 转换操作: 键值转换操作
    */
  def transformKey(sc: SparkContext, rdd: RDD[String]): Unit = {
    /**
      * - partitionBy操作，根据partitioner函数生成新的ShuffleRDD，将原RDD重新分区。
      * - mapValues操作，类似于map，只不过它是针对[K, V]中的V值进行map操作。
      * - flatMapValues操作，类似于flatMap，针对[K, V]中的V值进行flatMap操作。
      */
    var rdd1 = sc.makeRDD(Array((1, "A"), (2, "B"), (3, "C"), (4, "D")), 2)
    mapPartitionsWithIndex(rdd1).foreach(println)
    println("-----------")
    // 重分区
    var rdd2 = rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))
    mapPartitionsWithIndex(rdd2).foreach(println)

    rdd1.mapValues(_ + "_").collect().foreach(println)
    println("-----flatMapValues-----")
    rdd1.flatMapValues(_ + "_").collect().foreach(println)

    /**
      * - combineByKey操作，将RDD[K,V]转换为RDD[K,C]，其中V类型和C类型可以相同也可不同。
      * 参数createCombiner，表示组合器函数，将类型V转为C，输入参数为RDD[K,V]中的V，输出为C；
      * 参数mergeValue，表示合并值函数，将一个C类型和一个V类型值合并为C类型；
      * 参数mergeCombiner，合并组合器函数，将两个C类型值合并为一个C类型；
      * 参数numPartitions，结果RDD分区数，默认保持原有的分区数；
      * 参数partitioner，分区函数，默认为HashPartitioner；
      * 参数mapSideCombine，是否需要在Map端进行combine操作，默认为true；
      * - foldByKey操作，将RDD[K,V]根据K将V做折叠、合并处理；
      * 其中参数zeroValue表示先根据映射函数将zeroValue应用于V，进行初始化V，再将映射函数应用于初始化后的V。
      */
    val rdd20 = sc.makeRDD(Array(("A", 1), ("A", 2), ("B", 2), ("B", 1), ("C", 1)))
    rdd20.combineByKey(
      (v: Int) => v + "_",
      (c: String, v: Int) => c + "@" + v,
      (c1: String, c2: String) => c1 + "$" + c2
    ).collect().foreach(println)
    //    输出：
    //    (A,1_$2_)
    //    (B,2_$1_)
    //    (C,1_)

    // 将rdd20中每个key对应的V进行累加；
    // zeroValue=0，需要先初始化V，映射函数为"+"加法操作，
    // 如先把zeroValue用于(A,1)和(A,2)，得到(A,1+0)和(A,2+0)，即(A,1)和(A,2)；
    // 再将映射函数用于初始化后的V，得到(A,1+2)，即(A,3)，同理得到(B,3)和(C,1)
    rdd20.foldByKey(0)(_ + _).collect().foreach(println)
    rdd20.foldByKey(2)(_ + _).collect().foreach(println)
    // zeroValue=0，需要先初始化V，映射函数为"*"乘法操作，
    rdd20.foldByKey(0)(_ * _).collect().foreach(println)
    rdd20.foldByKey(1)(_ * _).collect().foreach(println)

    /**
      * - groupByKey操作，用于将RDD[K,V]中每个K对应的V值合并到一个集合Iterable[V]中。
      * - reduceByKey操作，将RDD[K,V]中每个K对应的V值根据映射函数来运算。
      * - reduceByKeyLocally操作，类似于reduceByKey，它把运算结果映射到一个Map[K,V]中，而不是RDD[K,V]
      */
    rdd20.groupByKey().collect().foreach(println)
    rdd20.reduceByKey((x, y) => x + y).collect().foreach(println)
    rdd20.reduceByKeyLocally((x, y) => x + y).foreach(println)

    /**
      * - cogroup操作，相当于SQL中的全外关联，返回左右RDD中的记录，关联不上的为空。
      */
    val rdd30 = sc.makeRDD(Array(("A", 1), ("B", 2), ("A", 3), ("C", 1)))
    val rdd31 = sc.makeRDD(Array(("C", 1), ("D", 2), ("D", 3), ("A", 3)))
    val rdd32 = rdd30.cogroup(rdd31)
    println(rdd32.partitions.length)
    rdd32.collect().foreach(println)

    /**
      * - join、fullOuterJoin、leftOuterJoin和rightOuterJoin都是针对RDD[K,V]中K值相等的连接操作。
      * 分别为内连接、全连接、左连接和右连接，这些操作都是调用cogroup函数实现的。
      * - subtractByKey操作，类似于subtract，只是针对的是键值操作。返回在RDD中出现，且不在otherRDD中出现的元素，返回元素不去重；
      */
    println("----内连接----")
    rdd30.join(rdd31).collect().foreach(println)
    println("----全连接----")
    rdd30.fullOuterJoin(rdd31).collect().foreach(println)
    println("----左连接----")
    rdd30.leftOuterJoin(rdd31).collect().foreach(println)
    println("----右连接----")
    rdd30.rightOuterJoin(rdd31).collect().foreach(println)
    //针对key，返回在RDD中出现，且不在otherRDD中出现的元素，返回元素不去重；
    rdd30.subtractByKey(rdd31).collect().foreach(println)
  }

  /**
    * 输出RDD下各分区的索引和元素组合，eg: (part_0,List((4,D), (2,B)))
    */
  def mapPartitionsWithIndex(rdd: RDD[(Int, String)]): Array[(String, List[(Int, String)])] = {
    rdd.mapPartitionsWithIndex((index, iter) => {
      var partMap = scala.collection.mutable.Map[String, List[(Int, String)]]()
      while (iter.hasNext) {
        var partName = "part_" + index
        var elem = iter.next()
        if (partMap.contains(partName)) {
          var elems = partMap(partName)
          elems ::= elem
          partMap(partName) = elems
        } else {
          partMap(partName) = List[(Int, String)] {
            elem
          }
        }
      }
      partMap.iterator
    }).collect()
  }

  /**
    * 控制操作
    */
  def control(sc: SparkContext, rdd: RDD[String]): Unit = {
    /**
      * 将RDD持久化到内存或磁盘文件：
      * - cache操作，是persist的特例，即persist的StorageLevel为MEMORY_ONLY时。
      * - persist操作，可以指定StorageLevel参数。
      * - checkpoint检查点，将切断与该RDD之前的依赖"血统"关系，对包含宽依赖对长血统对RDD非常有用，
      * 可以避免占用过多对系统资源和节点失败情况下重新计算成本过高的问他。
      */
    rdd.cache()
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    // 设置检查点的存储位置在HDFS，并使用checkpoint设置检查点，该操作属于懒加载。
    sc.setCheckpointDir("hdfs://master:9000/test/checkpoint")
    rdd.checkpoint()
    // 在遇到行动操作时，进行检查点操作，检查点前为ParallelCollectionRDD[0]，而检查点后为ParallelCollectionRDD[1]
    rdd.dependencies.head.rdd

    val startTime = Calendar.getInstance.getTimeInMillis.toInt
    println(rdd.count())
    val endTime = Calendar.getInstance.getTimeInMillis.toInt
    println("costs time: " + (endTime - startTime))
    println(rdd.count())
    println("costs time: " + (Calendar.getInstance.getTimeInMillis.toInt - endTime))
  }

  /**
    * 行动操作
    */
  def action(sc: SparkContext, rdd: RDD[String]): Unit = {
    //一、集合标量行动操作
    /**
      * - first():T，返回RDD中对第一个元素，不排序。
      * - count():Long，返回RDD中对元素个数。
      * - reduce(f:(T,T)=>T):T，根据映射函数f对RDD中对元素进行计算。
      * - collect():Array[T]，将RDD转换为数组。
      * - take(num:Int):Array[T]，获取RDD中从0到num-1下标对元素，不排序。
      * - top(num:Int):Array[T]，从RDD中按指定排序规则（默认降序）返回前num个元素。
      * - takeOrdered(num:Int):Array[T]，类似于top，和top相反(升序)对顺序返回元素。
      */
    rdd.first()
    rdd.count()
    rdd.reduce(_ + _)
    rdd.collect()
    rdd.take(1)
    rdd.top(2)
    rdd.takeOrdered(2)

    /**
      * - aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U；
      * aggregate接收两个函数，和一个初始化值。seqOp函数用于聚集每一个分区，combOp用于聚集所有分区聚集后的结果。
      * 每一个分区的聚集，和最后所有分区的聚集都需要初始化值的参与。
      * - fold(zeroValue: T)(op: (T, T) => T): T；
      * flod()函数相比reduce()加了一个初始值参数，之后根据输入函数进行计算。
      */
    // 定义rdd1，设置第一个分区包含5、4、3、2、1，第二个分区包含10、9、8、7、6。
    var rdd1 = sc.makeRDD(1 to 10, 2)
    rdd1.mapPartitionsWithIndex((index, iter) => {
      var map = scala.collection.mutable.Map[String, List[Int]]()
      while (iter.hasNext) {
        val name = "part_" + index
        val elem = iter.next()
        if (map.contains(name)) {
          var elems = map(name)
          elems ::= elem
          map(name) = elems
        } else {
          map(name) = List[Int](elem)
        }
      }
      map.iterator
    }).collect().foreach(println)

    // 先在每个分区中迭代执行(x:Int, y:Int) => x + y，并且zeroValue的值为1，
    // 即part_0中zeroValue+5+4+3+2+1=16；part_1中zeroValue+10+9+8+7+6=41；
    // 再将两个分区的结果合并(a: Int, b: Int) => a + b，并且zeroValue的值为1，
    // 即zeroValue+part_0+part_1=1+16+41=58
    val result = rdd1.aggregate(1)(
      { (x: Int, y: Int) => x + y },
      { (a: Int, b: Int) => a + b }
    )
    println(result) // 58
    // 结果同上面aggregate的第一个例子一样
    val result2 = rdd1.fold(1)(
      { (x: Int, y: Int) => x + y }
    )
    println(result2) // 58

    /**
      * - lookup(key: K): Seq[V] 用于(K,V)类型的RDD，指定K值，返回RDD中该K对应的所有V值。
      * - countByKey():Map[K,Long] 统计RDD[K,V]中每个K的数量。
      * - foreach[U](f: A => U): Unit 遍历RDD，将f函数应用于每一个元素。
      * 如对RDD执行foreach只会在Executor端有效而不是Driver端。
      * 如rdd.foreach(println)，只会在Executor端对stdout打印输出，而在Driver端是看不到的。
      * - foreachPartition(f: Iterator[T] => Unit): Unit 类似于foreach，只不过是对每一个分区使用f函数。
      * - sortBy[K](f: (T) => K, ascending: Boolean = true, numPartitions: Int = this.partitions.length) implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
      * 根据给定对排序k函数将RDD中对元素进行排序
      **/
    val rdd20 = sc.makeRDD(Array(("A", 0), ("A", 2), ("B", 1), ("B", 2), ("C", 1)))
    rdd20.lookup("A").foreach(println)

    // Accumulator累加器
    var cnt = sc.accumulator(0)
    rdd1.foreach(x => cnt += x)
    println(cnt)
    var cnt2 = sc.accumulator(0)
    rdd1.foreachPartition(x => cnt2 += x.size)
    println(cnt2) // 0 + 5 + 5 = 10

    // 定义RDD为键值类型，分别按照键升序排列和值降序排列
    rdd20.sortBy(x => x).collect().foreach(println)
    rdd20.sortBy(x => x._2, ascending = false).collect().foreach(println)


    // 二、存储行动操作
    /**
      * - saveAsTextFile 将RDD以文本文件对格式存储到文件系统，codec参数可以指定压缩的类名。
      * - saveAsObjectFile 将RDD中的元素序列化成对象存储到文件，对于HDFS，默认采用SequenceFile保存。
      * -
      */
    //    rdd1.saveAsTextFile("")
    //    rdd1.saveAsObjectFile("")

  }

}
