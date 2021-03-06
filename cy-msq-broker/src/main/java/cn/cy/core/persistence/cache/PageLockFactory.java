package cn.cy.core.persistence.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cy.common.ConcurrentFinalCache;

/**
 * page cache lock
 * 使用暴力重构策略
 * 每过{@link PageLockFactory#rebuildInterval}时间, 进行一次暴力重构
 * <p>
 * 使用 {@link PageLockFactory#flag} 来模拟读写锁的功能
 */
public class PageLockFactory extends ConcurrentFinalCache<Integer, Lock> {

    private Logger LOGGER = LoggerFactory.getLogger(getClass());

    /**
     * flag == {@link PageLockFactory#REBUILDING} , 意味着重构线程正在执行
     * flag > 0 , 意味着有线程在读取, 不可重构
     */
    protected AtomicInteger flag = new AtomicInteger(0);

    protected final Integer INIT = 0;

    protected final Integer REBUILDING = -1;

    /**
     * 重构时间间隔, 默认300s
     */
    protected Integer rebuildInterval;

    /**
     * 每一次rebuild 轮数+1
     */
    public Integer round = 0;

    public PageLockFactory() {
        this(300);
    }

    public PageLockFactory(Integer rebuildInterval) {
        this.rebuildInterval = rebuildInterval;
    }

    public void initialize() {

        new Thread(() -> {
            try {
                rebuild();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * 重构
     *
     * @throws InterruptedException
     */
    protected void rebuild() throws InterruptedException {

        while (true) {

            Thread.sleep(rebuildInterval * 1000);

            rebuild0();
        }
    }

    protected void rebuild0() {
        LOGGER.debug("rebuild awaiting ... ");

        // cas
        // 由于netty线程模型问题
        // 所以不会出现海量线程争抢
        // rebuild任务饥饿的概率较小
        while (!flag.compareAndSet(INIT, REBUILDING)) {
        }

        LOGGER.debug("set into rebuild status ... ");

        futureMap = new ConcurrentHashMap<>();

        round++;

        // 这时一定只有这个线程在运行, 所以直接设置回去就行
        flag.set(INIT);
    }

    /**
     * 给外部调用一把锁, 注意使用完之后归还, {@link PageLockFactory#returnLock(Lock)}
     *
     * @param key
     *
     * @return
     */
    public Lock offerLock(Integer key) throws ExecutionException, InterruptedException {
        // cas
        while (flag.updateAndGet(prev -> prev >= 0 ? prev + 1 : prev) < 0) {
        }

        return compute(key, ReentrantLock::new);
    }

    /**
     * 归还锁
     *
     * @param lock
     */
    public void returnLock(Lock lock) {
        flag.decrementAndGet();
    }
}
