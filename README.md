# MapReduce

## Mapreduce 工作机制

### Split

数据切片Split 是对Map Task 进行任务分配，一个File 可以对应多个Split ，一个Split 可以对应多个Block。
    
    /* FileSplit hadoop 260 */
    private Path file; // HDFS file
    private long start; // start position in the file
    private long length; // length
    private String[] hosts; // file position on HDFS 

根据这些信息去读取Split，Split 的大小最好和Block 大小相等,否则会造成不必要的网络IO。

### Map

#### Mapper 的map 方法执行之前

Map Task 调用`InputFormat`（默认实现 `TextInputFormat`）的`getSplits`方法获取Split 信息，然后从HDFS 读取File,在`createRecordReader` 方法用于创建一个`RecordReader`。

    public abstract class InputFormat<K, V> {
        
        public abstract 
            List<InputSplit> getSplits(JobContext context
                                    ) throws IOException, InterruptedException;
        
        public abstract 
            RecordReader<K,V> createRecordReader(InputSplit split,
                                                TaskAttemptContext context
                                                ) throws IOException, 
                                                        InterruptedException;
    }

`InputFormat` 的`createRecordReader` 方法得到一个`RecordReader`（默认实现 `LineRecordReader`），通过reader 的`nextKeyValue`方法创建一组`<key, value>`，然后通过`getCurrentKey` 和`getCurrentValue` 读取键值对。`LineRecordReader` 创建出的key 为`LongWritable` 类型的文件行偏移量，value 为`Text` 类型的一行文本数据。


    public abstract class RecordReader<KEYIN, VALUEIN> implements Closeable {

        public abstract 
        boolean nextKeyValue() throws IOException, InterruptedException;

        public abstract
        KEYIN getCurrentKey() throws IOException, InterruptedException;
        
        public abstract 
        VALUEIN getCurrentValue() throws IOException, InterruptedException;
    
    }


`LineRecordReader` 会在`initialize` 方法中依赖一个`LineReader`实例来读取一行数据，当`readLine`方法在遇到跨Split 的文件时,对于每一个Split来说，在结尾多读一行数据，从第二个Split 开始，都从第二行开始读数据，这样就可以解决文件数据可能不连续的问题。

#### Mapper 的map 方法执行

Map Task 得到一组`<key, value>` 输入之后，将执行`map` 方法中的逻辑，将结果`<outputKey, outputValue>` 写入`context` 上下文中。Context 中的数据先回放到内存中的环形缓冲区中，当达到一定量后，会发生溢出，将内存中的数据`spill` 到磁盘上的溢出文件中。

### Shuffle

    /**
    * Serialize the key, value to intermediate storage.
    * When this method returns, kvindex must refer to sufficient unused
    * storage to store one METADATA.
    */
    public synchronized void collect(K key, V value, final int partition
                                    ) throws IOException {
                                        ...
                                        ...
                                    }


`MapOutputCollector` 的`collect` 方法会将`<k,v>` 分区（`自定义Class extends Partitioner，override getPartion方法`）且排序（`根据key 的 compareTo方法`）的存放在内存中，当环形缓冲区(`默认最大值 100M`)中的数据量达到溢出比（`80%`）的时候,会发生`spill`。

`Partition` 默认是对`Key` 进行`hash`,然后`mod reduce task sum`,将来会分配到指定的`reduce task`

在`spill` 也就是`SpillRecord 的writeToFile` 之前可以自定义一个`Combiner`进行一次本地的reduce，输出为`<outputKey,outputValues>`用来减少网络IO，但是在不影响全局逻辑的前提下，比如求平均值等类似场景就不太适用。

之后在本地会发生一次本地的`merge`，会根据`key 的 compareTo方法`进行多路归并排序,将所有spill 文件合并起来。合并之后的spill 文件按照partition 顺序存储，对应的索引文件为`file.out.index` 记录着partition 的偏移量。

### Reduce

当Map Task任务完成数超过`5%` 之后，Reduce Task 会通过HTTP GET 的方式去拉取对应的`partition` 数据，同样也是先放在内存缓冲区，当超过一定阀值后再写入磁盘，写入磁盘时会进行一次`merge` 操作。

Reduce Task端可以自定义`GroupingComparator Class extends WritableComparator,要调用父类构造器创建Key.class 实例，并且要override compare(Writable a, Writable b) 方法`。

之后会调用Reducer 的reduce 方法，处理每对`<key,values>`，然后`OutputFormat` 默认为（`TextOutputFormat`）通过`getRecordWriter`方法创建`RecordWriter`,并调用writer的`write` 方法将Record 写到HDFS 的`/output_path/part-r-xxxxx`文件中。

## Mapreduce 自定义Bean

### WritableComparable 接口

`key` 对应类必须实现该接口，重写`Writable 的write 和readFields方法` 和 `Comparable<T> 的compareTo 方法` 


    public interface WritableComparable<T> extends Writable, Comparable<T> {
    }

    public interface Comparable<T> {
        /*
        1. sort, merge 会用到该方法
        2. reduce task 根据key 分组也可能用到，不过分组可以自定义GroupingComparator 方式实现
        */
        public int compareTo(T o);
    }

    /* serialize and deserialize */
    public interface Writable {
    void write(DataOutput out) throws IOException;

    void readFields(DataInput in) throws IOException;
    }

## 再谈split

### 小文件场景下，默认的切片机制会造成大量的map task 处理很小的split，效率很低，如何解决？

1. 上传文件到HDFS 之前，进行预处理，合并再上传
2. 在HDFS 上将小文件合并
3. 修改`InputFormat` 的`getSplits` 方法

### 为什么要有切片？如何切割？

1. split 是为了给map task 分配任务，也就是为整个Job 做map task 的并行度规划
2. 默认的切片机制是`InputFormat` 的`getSplits` 方法,它的逻辑是对`input_path` 中的所有文件进行切割，`splitSize = block.size，remain/splitSize <= 1.1 为一个split`
3. 一个split 会交给一个map task 处理

### 谁来做Split？FileSplit到哪里了？

1. Job.submit 后会交给Client 来做Split
2. Client 会将FileSplit serialize 到/tmp/staging/ 目录下的job.split 文件中，并和jar，configuration文件一起发给MRAppMaster
3. MRAppMaster 来根据split 信息分配任务

## 并行度经验之谈

### map

1. `2*12 core, 64G` 服务器跑`20 - 100 map task`，单个map task 执行时间尽量大于一分钟，原因是每个task 的setup 与加入调度器就要几秒钟，任务执行时间太短，就会效率低下，所以要么减少task 数量，也可以配置JVM 重用，配置hadoop 参数`mapred.job.reuse.jvm.num.tasks > 1`，表示属于同一job 的task可以共享一个JVM，也就是说第二轮的map可以重用前一轮的JVM，而不是第一轮结束后关闭JVM，第二轮再启动新的JVM。将其配置为`-1`，则表示只要job 相同，task都可以重用JVM。

2. 如果Input File 很大1 TB 以上，则可以考虑将Block Size 扩大为256 MB 或者更大。

### Reduce

1. 数据倾斜时，根据业务思考解决方法，尽量从业务层面优化partition。对于全局汇总业务，尽量一个Reduce Task。

## mapreduce 数据压缩

### 概述

数据压缩是mapreduce 的一种优化策略，通过压缩编码`gzip bzip2 等`对mapper 或者 reducer 的输出结果进行压缩，减少网络IO带宽消耗和输出的数据体积，提高MR 程序的运行效率，但是同时也增加了CPU 的计算压力。

### 使用原则

1. 运算密集型JOB，较少使用压缩
2. IO密集型JOB，可以使用压缩

### 使用方法

#### mapper 输出

1. 

    mapreduce.map.output.compress=false
    mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.DefaultCodec

2. 

    conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
    conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);

#### reducer 输出

1. 

    mapreduce.output.fileoutputformat.compress=false
    mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.DefaultCodec
    mapreduce.output.fileoutputformat.compress.type=RECORD

2. 

    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, (Class<? extends CompressionCodec>) Class.forName(""));

#### 读取

Hadoop自带的InputFormat类内置支持压缩文件的读取，在其initialize 方法中会先根据文件后缀名判断相应的codec，然后判断是否属于可切片压缩编码类型`SplittableCompressionCodec`,如果是则创建一个`CompressedSplitLineReader` 读取数据，如果不是可切片压缩编码类型或者压根就不是压缩文件则创建`SplitLineReader`读取数据。

## 计数器应用

### 概述

计数器可以用来记录JOB 执行进度和状态，通常我们可以在某个程序中插入一个计数器,用来记录程序执行状态等,比日志更加便于我们分析程序。

### 使用方法

    public class MultiOutputs {
        //通过枚举形式定义自定义计数器
        enum MyCounter{MALFORORMED,NORMAL}

        static class CommaMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                String[] words = value.toString().split(",");

                for (String word : words) {
                    context.write(new Text(word), new LongWritable(1));
                }
                //对枚举定义的自定义计数器加1
                context.getCounter(MyCounter.MALFORORMED).increment(1);
                //通过动态设置自定义计数器加1
                context.getCounter("counterGroupa", "countera").increment(1);
            }
        }
    }

## Mapreduce 的DistributedCache

[参考 LogJoinByMapper](https://github.com/LiPenglin/mapreduce_practice/tree/master/src/main/java/logJoinByMapper)

## 多Job 串联
[参考 LogJoinByMapper Main](https://github.com/LiPenglin/mapreduce_practice/blob/master/src/main/java/logJoinByMapper/Main.java)











