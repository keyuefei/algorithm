package heap;

import java.util.Comparator;

/**
 *
 * @Description 最大堆
 * 堆排序包括建堆和排序两个操作，
 * 建堆过程的时间复杂度是 O(n)，排序过程的时间复杂度是 O(nlogn)，
 * 所以，堆排序整体的时间复杂度是 O(nlogn)。
 * <p> 结论1: 完全二叉树满足：下标从 2n​+1 到 n 的节点都是叶子节点。</p>
 * <p> 结论2: 数组中下标为 i 的节点的左子节点，就是下标为 i∗2 的节点，
 * 右子节点就是下标为 i∗2+1 的节点，父节点就是下标为 2i​ 的节点</p>
 * <p> 结论3: 堆排序时间复杂度 O(nlogn)、原地排序、不稳定排序（排序时将未排序的最后一个节点与堆顶互换，会导致数据原有顺序被破坏）</p>
 * todo <p> 结论4:堆排序与快排的比较 </p>
 * <p> TOP K</p>
 *
 *
 * <p>mysql中: 使用 order by field limit n 会导致mysql使用堆排序，从而导致分页数据重复；
 * 原因堆排序是不稳定排序算法，举例：https://my.oschina.net/linxxbaobao/blog/1628027
 * 解决办法：1. 在排序字段上加索引 2. 增加具有唯一性的排序字段，例如主键id。
 * </p>
 * @Author keyuefei
 * @Date 2020/7/7
 * @Time 11:11
 */
class Heap<E> {
    /**
     * 存储堆数组
     */
    private Object[] data;

    /**
     * 已存储的个数
     */
    private int count;

    /**
     * 容量
     */
    private int size;

    /**
     * 比较器
     */
    private Comparator<E> comparator;

    public Heap(Comparator<E> comparator) {
        this(16, comparator);
    }

    public Heap(int capacity, Comparator<E> comparator) {
        if (comparator == null) {
            throw new RuntimeException("必须初始化比较器，才能决定是最大/小堆");
        }
        this.comparator = comparator;
        data = new Object[capacity + 1];
        size = capacity;
        count = 0;
    }


    public Heap(int capacity, Comparator comparator, E... ds) {
        this(capacity, comparator);
        insert(ds);
    }


    /**
     * 时间复杂度：O(nlogn)
     *
     * @param ds
     */
    public void insert(E... ds) {
        if (ds == null) {
            return;
        }
        for (E d : ds) {
            insert(d);
        }
    }


    /**
     * 插入堆化
     * 放到数组最后，从下往上与父节点比较、交换。
     * 插入一个元素的时间复杂度：O(logn)
     *
     * @param d
     */
    public void insert(E d) {
        if (count >= size) {
            resize();
        }
        ++count;
        data[count] = d;
        int i = count;

        while (i / 2 > 0 && comparator.compare(d, (E) data[i / 2]) > 0) {
            //自下往上堆化
            swap(i, i / 2);
            i = i / 2;
        }
    }

    private void resize() {
        size = size * 2;
        Object[] newData = new Object[size + 1];
        System.arraycopy(data, 0, newData, 0, data.length);
        data = newData;
    }

    private void swap(int i, int j) {
        Object tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }


    /**
     * 需要从上至下堆化。保证仍是最大堆
     *
     * @param i
     * @param j
     */
    private void heapify(int i, int j) {
        //左 2*i、  右 2*i + 1
        int maxPos = i;
        while (true) {
            int tmpMaxPos = maxPos;
            int leftPos = 2 * maxPos;
            int rightPos = 2 * maxPos + 1;


            if (rightPos <= j && comparator.compare((E) data[maxPos], (E) data[rightPos]) < 0) {
                maxPos = rightPos;
            }

            if (leftPos <= j && comparator.compare((E) data[maxPos], (E) data[leftPos]) < 0) {
                maxPos = leftPos;
            }
            //叶子节点， 或者左右子节点值已比自己小，则退出
            if (maxPos == tmpMaxPos) {
                break;
            }
            swap(tmpMaxPos, maxPos);
        }
    }


    public E peek() {
        return count == 0 ? null : (E) data[1];
    }


    public E pop() {
        if (count == 0) {
            return null;
        }
        E result = (E) data[1];
        swap(1, count);
        count--;
        heapify(1, count);
        return result;
    }


    public void push(E d) {
        if (count >= size) {
            //元素满了
            //与堆顶元素比较大小，小于最大元素，则删除堆顶，从上至下堆化
            if (comparator.compare(d, (E) data[1]) < 0) {
                data[1] = d;
                heapify(1, count);
            }
            return;
        }
        insert(d);
    }

    public int count() {
        return count;
    }
}