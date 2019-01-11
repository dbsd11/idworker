package com.imadcn.system.test.performance;

import com.imadcn.framework.idworker.generator.SnowflakeGenerator;
import com.imadcn.system.test.spring.AbstractZookeeperJUnit4SpringContextTests;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Hinsteny
 * @version $ID: LockSnowFlakeGeneratorTest 2019-01-09 19:35 All rights reserved.$
 */
@ContextConfiguration(locations = "classpath:META-INF/idworker-ctx-new-xsd.xml")
public class SnowFlakeGeneratorTest extends AbstractZookeeperJUnit4SpringContextTests {

    @Autowired
    private SnowflakeGenerator snowflakeGenerator;

    private int step = 100000;

    private int count = 10000000;

    @Test
    public void produceBatchIds() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count/step; i++) {
            long[] ids = snowflakeGenerator.nextId(step);
        }
        System.out.println(String.format("Create %d ids consume %d millisecond", count, System.currentTimeMillis() - start));
    }

    @Test
    public void muiltThreadProduceBatchIds() throws InterruptedException {
        int threadCount = count / step;
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                long[] ids = snowflakeGenerator.nextId(step);
                latch.countDown();
            }).start();
        }
        latch.await();
        System.out.println(String.format("Create %d ids consume %d millisecond", count, System.currentTimeMillis() - start));
    }

}
