package heap;

import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 学习url: https://time.geekbang.org/column/article/69913
 *
 * @Description
 * @Author keyuefei
 * @Date 2020/7/7
 * @Time 11:11
 */
public class Main {


    public static void main(String[] args) throws InterruptedException {

//        heapSort();
//        topK();
//        dynamicMedian();
//        percent99();
//        hotSearchKeyWord();
        delayQueue();
    }


    /**
     * 延时队列： 可参考java的delayQueue，内部使用锁与优先队列（底层为堆）实现。
     * 场景：
     * 1. 超过30分钟未支付，订单自动作废。
     * 2. 用户下单后延时短信提醒。
     * <p>
     * <p>
     * 超过30分钟未支付，订单自动作废
     * 注意： DelayQueue.take()等待时间单位是ns； 计算expire时，需要加L，否则会当成int计算而溢出。
     */
    private static void delayQueue() throws InterruptedException {
        DelayQueue delayQueue = new DelayQueue();

        //过期时间 10 s
        long expire = 10 * 1000 * 1000 * 1000L;

        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        class CancelOrderTask implements Delayed, Runnable {

            private long createNanoSeconds;
            private String name;

            CancelOrderTask(String name, long createTime) {
                //纳秒
                this.createNanoSeconds = createTime * 1000 * 1000L;
                this.name = name;
            }

            @Override
            public long getDelay(TimeUnit unit) {
                return (createNanoSeconds + expire) - LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli() * 1000 * 1000;
            }

            @Override
            public int compareTo(Delayed o) {
                CancelOrderTask c = (CancelOrderTask) o;
                //创建时间在前的排在堆顶。
                return (int) (this.createNanoSeconds - c.createNanoSeconds);
            }

            @Override
            public void run() {
                System.out.println(LocalDateTime.now().format(df) + "\t" + name);
            }
        }


        delayQueue.add(new CancelOrderTask("t1", LocalDateTime.of(2020, 7, 8, 18, 42, 10).toInstant(ZoneOffset.of("+8")).toEpochMilli()));
        delayQueue.add(new CancelOrderTask("t2", LocalDateTime.of(2020, 7, 8, 18, 42, 20).toInstant(ZoneOffset.of("+8")).toEpochMilli()));
        //delayQueue.poll(); poll没有满足条件的直接返回null
        //模拟线程池，循环从其中调用，并执行任务
        while (true) {
            //没有符合条件的，会循环取到为止。
            Runnable task = (Runnable) delayQueue.take();
            new Thread(task).start();

        }

    }


    /**
     * 热门搜索key的topK,
     * 实际场景：10亿个搜索关键词的日志文件，如何快速获取到 Top 10 最热门的搜索关键词
     * 思路：假设平均关键字占50个字节， 10 亿条搜索关键词中不重复的有 1 亿条，使用hash表存储访问次数，
     * 则10*10000*10000 * 50 ~= 5GB；而内存仅有1GB；所以需要分片，将10亿数据分10份存储在文件中，
     * 去掉重复的数据，每个文件在内存中占的内存约为 500MB，然后分别对10个文件进行TopK，最后合并统计排名前10的关键词
     */
    private static void hotSearchKeyWord() {

        class KeyWord {
            String key;
            int count;
        }

        Comparator<KeyWord> comparator = Comparator.comparingInt(o -> o.count);


        //10亿关键词的日志文件。
        String[] base = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
        List<String> keys = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            int j = (int) (random.nextDouble() * base.length);
            keys.add(base[j]);
        }


        //分片 10片
        int partitionCount = 10;
        Map<Integer, List<String>> partitions = new HashMap<>(16);

        for (String key : keys) {
            int p = key.hashCode() % partitionCount;

            if (partitions.get(p) == null) {
                partitions.put(p, new ArrayList<>());
            } else if (partitions.containsKey(p)) {
                partitions.get(p).add(key);
            }
        }

        //依次求得分片中的Top 10
        Map<Integer, Heap> partition2Top10 = new HashMap<>(16);
        for (Map.Entry<Integer, List<String>> partition : partitions.entrySet()) {
            List<String> partitionKeys = partition.getValue();
            int partitionIndex = partition.getKey();

            Map<String, KeyWord> keyWordMap = new HashMap(16);

            for (String partitionKey : partitionKeys) {
                if (keyWordMap.containsKey(partitionKey)) {
                    KeyWord keyWord = keyWordMap.get(partitionKey);
                    keyWord.count++;
                } else {
                    KeyWord keyWord = new KeyWord();
                    keyWord.key = partitionKey;
                    keyWord.count = 1;
                    keyWordMap.put(partitionKey, keyWord);
                }
            }

            Heap maxHeap = new Heap(10, comparator);
            for (KeyWord keyWord : keyWordMap.values()) {
                maxHeap.push(keyWord);
            }
            partition2Top10.put(partitionIndex, maxHeap);
        }

        //找出分片统计后的 10个TOP 10中最多搜索的10个关键词

        List<KeyWord> top10 = new ArrayList<>();


        for (Heap<KeyWord> heap : partition2Top10.values()) {
            KeyWord keyWord;
            while ((keyWord = heap.pop()) != null) {
                top10.add(keyWord);
            }
        }

        top10.sort(comparator.reversed());

        System.out.println("top 10热搜词汇：");
        for (int i = 0; i < 10; i++) {
            KeyWord keyWord = top10.get(i);
            System.out.println(keyWord.key + " : " + keyWord.count);
        }
    }

    /**
     * 接口的 99% 响应时间：即99%的接口响应时间低于某一个值
     * <p>
     * 类比动态求解中位数。
     */
    private static void percent99() {
        int requestTimes = 100;
        double percent = 0.99;
        DecimalFormat df = new DecimalFormat("0.00");


        Heap<Double> maxHeap = new Heap(Comparator.naturalOrder());
        Heap<Double> minHeap = new Heap(Comparator.reverseOrder());

        List<Double> out = new ArrayList<>();

        Random random = new Random();
        for (int i = 0; i < requestTimes; i++) {
            //10 - 200 ms
            double rt = 10 + 200 * (random.nextDouble() - 0.05);
            rt = Double.valueOf(df.format(rt));
            out.add(rt);


            if (maxHeap.count() == 0 || rt <= maxHeap.peek()) {
                maxHeap.insert(rt);
            } else {
                minHeap.insert(rt);
            }

            //adjust
            int count = minHeap.count() + maxHeap.count();

            int maxHeapAdjustCount = (int) (count * percent);

            while (maxHeap.count() != maxHeapAdjustCount) {
                if (maxHeap.count() > maxHeapAdjustCount) {
                    minHeap.insert(maxHeap.pop());
                } else {
                    maxHeap.insert(minHeap.pop());
                }
            }
            //输出99%的值
            out.sort(Comparator.naturalOrder());
            System.out.println("各请求响应：" + Arrays.deepToString(out.toArray()));
            System.out.println("下标索引：" + maxHeapAdjustCount);
            System.out.println("99%的值：" + maxHeap.peek());
        }

    }


    /**
     * 中位数： 给定一个数组，将数组排序；
     * 若数组长度n为奇数，则中位数为 n/2 + 1,
     * 若数组长度n为偶数，则中位数为 n/2 或 n/2 + 1 任意一个。
     * <p>
     * 若是静态数组，则每次查询都给定固定值；尽管排序的代价比较大，但是边际成本会很小。
     * <p>
     * 动态中位数：来源数组是动态的，若使用上述排序后，找中位数的方式，每来一个元素都会重新排序，效率非常低。
     * 思路： 维护 一个最小堆 和 一个最大堆；
     */
    private static void dynamicMedian() {
        /**
         * 原数组
         */
        Integer[] source = new Integer[]{6, 1, 5};
        //先将源数据从小到大排序
        Arrays.sort(source);
        //控制台输出
        List<Integer> out = new ArrayList(Arrays.asList(source));
        /**
         * 模拟动态数组
         */
        Integer[] dynamicSource = new Integer[]{4, 3, 2, 7, 8, 9, 10};


        //若是数组长度为偶数，将 0 - n/2 放入最大堆; 将 n/2 + 1 - n 放入最小堆,
        //中位数为 最大堆与最小堆 堆顶元素

        //若是数组长度为奇数，将 0 - n/2 放入最大堆; 将 n/2 + 1 - n 放入最小堆
        //中位数最小堆堆顶元素

        Heap<Integer> maximumHeap = new Heap(Comparator.naturalOrder());
        Heap<Integer> minimumHeap = new Heap(Comparator.reverseOrder());
        for (int i = 0; i < source.length / 2; i++) {
            maximumHeap.insert(source[i]);
        }
        for (int i = source.length / 2; i < source.length; i++) {
            minimumHeap.insert(source[i]);
        }
        //动态输入新的元素
        for (Integer ds : dynamicSource) {
            out.add(ds);
            if (ds >= minimumHeap.peek()) {
                minimumHeap.insert(ds);
            } else {
                maximumHeap.insert(ds);
            }

            //求此时的中位数：
            //因为动态插入元素，可能导致中位数不在堆顶，所以需要动态调整，
            //若数组长度为奇数，将最小堆调整为 n/2 + 1个元素； 若数组长度为偶数，将最小堆调整为 n/2 个元素
            int count = maximumHeap.count() + minimumHeap.count();
            boolean isOdd = count % 2.0 != 0;
            int adjustCount;
            if (isOdd) {
                adjustCount = count / 2 + 1;
            } else {
                adjustCount = count / 2;
            }
            while (minimumHeap.count() != adjustCount) {
                if (minimumHeap.count() > adjustCount) {
                    maximumHeap.insert(minimumHeap.pop());
                } else {
                    minimumHeap.insert(maximumHeap.pop());
                }
            }

            out.sort(Integer::compareTo);
            System.out.println("当前数组：" + Arrays.deepToString(out.toArray()));
            System.out.println("中位数为：" + minimumHeap.peek());
        }
    }


    /**
     * topK：在n个元素中取出最大（最小）的三个元素
     * 思路： 维护一个固定大小的堆（最大堆，如大小k），向其中放入元素，当满时，与堆顶元素比较；
     * 若小于，则替换，并从上至下堆化（时间复杂度：O(logk)，n个元素的时间复杂度：O(n*logk)），
     * 依此，遍历完所有元素，则得出最小的k个元素。
     * <p>topK 中的元素并不保证有序。因为最大（小）堆只能保证父节点大（小）于子节点</p>
     */
    private static void topK() {
        //在source中找到最小的3个数
        int[] source = new int[]{6, 1, 5, 4, 3, 2};

        Heap<Integer> maximumHeap = new Heap(3, Comparator.naturalOrder());


        for (int s : source) {
            maximumHeap.push(s);
        }

        Integer r;
        while ((r = maximumHeap.pop()) != null) {
            System.out.print(r + "\t");
        }

    }


    /**
     * 堆排序
     * 时间复杂度：O(n*logn)
     */
    public static void heapSort() {
        Heap<Integer> heap = new Heap(3, Comparator.naturalOrder(),
                3, 1, 2, 3, 4, 5, 6, 7);
        //1. 排序。
        Integer r;
        while ((r = heap.pop()) != null) {
            System.out.print(r + "\t");
        }
    }

}