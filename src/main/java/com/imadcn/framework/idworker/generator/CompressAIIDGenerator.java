package com.imadcn.framework.idworker.generator;

import com.imadcn.framework.idworker.algorithm.IdGene;
import com.imadcn.framework.idworker.algorithm.ShortShortAIID;
import com.imadcn.framework.idworker.exception.RegException;
import com.imadcn.framework.idworker.register.GeneratorConnector;
import com.imadcn.framework.idworker.register.zookeeper.ZookeeperConnectionStateListener;
import com.imadcn.framework.idworker.register.zookeeper.ZookeeperWorkerRegister;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Snowflake算法生成工具
 *
 * @author yangchao
 * @since 1.0.0
 */
public class CompressAIIDGenerator implements IdGenerator, GeneratorConnector {

    static final String FIXED_STRING_FORMAT = "%019d";

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 自增 算法
     */
    private IdGene idGene;
    /**
     * 自增 注册
     */
    private ZookeeperWorkerRegister register;

    private Supplier<Long> globalSequenceProvider;
    /**
     *
     */
    /**
     * 是否正在工作
     */
    private volatile boolean initialized = false;

    private volatile boolean working = false;

    private volatile boolean connecting = false;

    private ConnectionStateListener listener;

    public CompressAIIDGenerator(ZookeeperWorkerRegister register, Supplier<Long> globalSequenceProvider) {
        this.register = register;
        this.globalSequenceProvider = globalSequenceProvider;
    }

    @Override
    public synchronized void init() {
        if (!initialized) {
            listener = new ZookeeperConnectionStateListener(this);
            // 添加监听
            register.addConnectionListener(listener);
            // 连接与注册workerId
            connect();
            initialized = true;
        }
    }

    /**
     * 初始化
     */
    @Override
    public void connect() {
        if (!isConnecting()) {
            working = false;
            connecting = true;
            long workerId = register.register();
            int bizFlag = 0;
            String groupName = register.getNodePath().getGroupName();
            if (!StringUtils.isEmpty(groupName) && groupName.contains("_")) {
                bizFlag = Integer.valueOf(groupName.substring(groupName.lastIndexOf("_") + 1, groupName.length()));
            }

            if (workerId >= 0) {
                idGene = ShortShortAIID.create(globalSequenceProvider, bizFlag);
                working = true;
                connecting = false;
            } else {
                throw new RegException("failed to get worker id");
            }
        } else {
            logger.info("worker is connecting, skip this time of register.");
        }
    }

    @Override
    public long[] nextId(int size) {
        if (isWorking()) {
            return idGene.nextId(size);
        }
        throw new IllegalStateException("worker isn't working, reg center may shutdown");
    }

    @Override
    public long nextId() {
        if (isWorking()) {
            return idGene.nextId();
        }
        throw new IllegalStateException("worker isn't working, reg center may shutdown");
    }

    @Override
    public String nextStringId() {
        return String.valueOf(nextId());
    }

    @Override
    public String nextFixedStringId() {
        int bizFlag = 0;
        String groupName = register.getNodePath().getGroupName();
        if (!StringUtils.isEmpty(groupName) && groupName.contains("_")) {
            bizFlag = Integer.valueOf(groupName.substring(groupName.lastIndexOf("_") + 1, groupName.length()));
        }

        String idFormat = String.format("%%0%dd", idGene.getSize() + Math.min(bizFlag, 10) / 10);
        return String.format(idFormat, nextId());
    }

    @Override
    public void suspend() {
        this.working = false;
    }

    @Override
    public synchronized void close() throws IOException {
        // 关闭，先重置状态(避免ZK删除 workerId，其他机器抢注，会导致workerID 重新生成的BUG)
        reset();
        register.logout();
    }

    @Override
    public boolean isWorking() {
        return this.working;
    }

    @Override
    public boolean isConnecting() {
        return this.connecting;
    }

    /**
     * 重置连接状态
     */
    protected void reset() {
        initialized = false;
        working = false;
        connecting = false;
    }

}
