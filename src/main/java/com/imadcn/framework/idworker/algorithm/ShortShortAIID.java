package com.imadcn.framework.idworker.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Snowflake的结构如下(每部分用-分开):
 * <br>0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000
 * <br><b> · </b>1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0
 * <br><b> · </b>41位时间戳(毫秒级)，注意，41位时间戳不是存储当前时间的时间戳，而是存储时间戳的差值（当前时间戳 - 开始时间戳)得到的值），这里的的开始时间戳，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下下面程序epoch属性）。41位的时间戳，可以使用69年
 * <br><b> · </b>10位的数据机器位，可以部署在1024个节点，包括5位datacenterId和5位workerId
 * <br><b> · </b>12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间戳)产生4096个ID序号
 * 加起来刚好64位，为一个Long型。
 * <p>
 * SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞(由数据中心ID和机器ID作区分)，并且效率较高，经测试，SnowFlake每秒能够产生26万ID左右。
 * <p>
 * 注意这里进行了小改动:
 * <br><b> · </b>Snowflake是5位的datacenter加5位的机器id; 这里变成使用10位的机器id (b)
 * <br><b> · </b>对系统时间的依赖性非常强，需关闭ntp的时间同步功能。当检测到ntp时间调整后，将会拒绝分配id
 *
 * @author yangchao
 * @since 1.0.0
 */
public class ShortShortAIID implements IdGene {

//    private static final Logger logger = LoggerFactory.getLogger(ShortShortAIID.class);

    /**
     * 业务标识
     */
    private final int bizFlag;
    /**
     * 序列在id中占的位数
     */
    private final long sequenceBits = 22L;
    /**
     * 生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095)，12位
     */
    private final long sequenceMask = -1L ^ -1L << this.sequenceBits;
    /**
     * 上次生成ID的时间戳
     */
    private long lastTimestamp = -1L;

    private final int HUNDRED_K = 100_000;

    private Supplier<Long> globalSequenceProvider;

    private ShortShortAIID(Supplier<Long> globalSequenceProvider, int bizFlag) {
        if (globalSequenceProvider == null) {
            throw new IllegalArgumentException("globalSequenceProvider can't be null");
        }
        if (bizFlag < 0 || bizFlag > 100) {
            String message = String.format("bizFlag Id can't be greater than %d or less than 0", 100);
            throw new IllegalArgumentException(message);
        }

        this.bizFlag = bizFlag;
        this.globalSequenceProvider = globalSequenceProvider;
    }

    /**
     * Snowflake Builder
     *
     * @return Snowflake Instance
     */
    public static ShortShortAIID create(Supplier<Long> globalSequenceProvider) {
        return create(globalSequenceProvider, 0);
    }

    /**
     * Snowflake Builder
     *
     * @param bizFlag bizFlag
     * @return Snowflake Instance
     */
    public static ShortShortAIID create(Supplier<Long> globalSequenceProvider, int bizFlag) {
        return new ShortShortAIID(globalSequenceProvider, bizFlag);
    }

    /**
     * 批量获取ID
     *
     * @param size 获取大小，最多10万个
     * @return SnowflakeId
     */
    public long[] nextId(int size) {
        if (size <= 0 || size > HUNDRED_K) {
            String message = String.format("Size can't be greater than %d or less than 0", HUNDRED_K);
            throw new IllegalArgumentException(message);
        }
        long[] ids = new long[size];
        for (int i = 0; i < size; i++) {
            ids[i] = nextId();
        }
        return ids;
    }

    /**
     * 获得ID
     *
     * @return SnowflakeId
     */
    public synchronized long nextId() {
        long timestamp = tilNextMillis(this.lastTimestamp);
        // 如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        if (timestamp < this.lastTimestamp) {
            String message = String.format("Clock moved backwards. Refusing to generate id for %d milliseconds.", (this.lastTimestamp - timestamp));
//            logger.error(message);
            throw new RuntimeException(message);
        }

        this.lastTimestamp = timestamp;

        long sequence = globalSequenceProvider.get() & sequenceMask;

        // 移位并通过或运算拼到一起组成64位的ID
        long id = sequence;
        return id + bizFlag * Double.valueOf(Math.pow(10, getSize() - 1)).longValue();
    }

    /**
     * 等待下一个毫秒的到来, 保证返回的毫秒数在参数lastTimestamp之后
     *
     * @param lastTimestamp 上次生成ID的时间戳
     * @return
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 获得系统当前毫秒数
     */
    private long timeGen() {
        return System.currentTimeMillis();
    }

    public int getSize() {
        return 8;
    }

    public static void main(String[] args) {
        long id = ShortShortAIID.create(()->1L).nextId();
        System.out.println(Long.toBinaryString(id));
        System.out.println(Long.highestOneBit(id));
        System.out.println(Long.numberOfLeadingZeros(id));
        System.out.println(Long.numberOfTrailingZeros(id));
        ShortShortAIID shortShortSnowflake = ShortShortAIID.create(()->6L);
        System.out.println(shortShortSnowflake.nextId());
        System.out.println(shortShortSnowflake.nextId());
        System.out.println(shortShortSnowflake.nextId());
        ShortShortAIID shortShortSnowflake1 = ShortShortAIID.create(()->6L);
        System.out.println(shortShortSnowflake1.nextId());
        System.out.println(shortShortSnowflake1.nextId());
        System.out.println(shortShortSnowflake1.nextId());
        ShortShortAIID shortShortSnowflake2 = ShortShortAIID.create(()->1L, 60);
        System.out.println(shortShortSnowflake2.nextId());
        System.out.println(shortShortSnowflake2.nextId());
        System.out.println(shortShortSnowflake2.nextId());
    }
}