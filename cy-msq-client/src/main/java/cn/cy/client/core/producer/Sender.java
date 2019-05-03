package cn.cy.client.core.producer;

import cn.cy.client.core.channel.IChannel;
import cn.cy.io.vo.BaseInfo;
import cn.cy.io.vo.RequestType;
import cn.cy.io.vo.request.CommitRequest;
import com.alibaba.fastjson.JSON;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 处理消息发送的可执行任务
 */
public class Sender implements Runnable{

    private volatile boolean isRunning;

    private ConcurrentLinkedQueue<Package> pendingQueue;

    public Sender() {
        this.isRunning = true;
        this.pendingQueue = new ConcurrentLinkedQueue<>();
    }

    private BaseInfo<CommitRequest> construct(String message) {
        CommitRequest request = new CommitRequest(message);
        return new BaseInfo<>(RequestType.MESSAGE_COMMIT.id, null, UUID.randomUUID().toString(), request);
    }

    /**
     * 将消息放入待发送队列中
     * @param message 消息
     * @param dst 要发送至的channel
     */
    public void send(String message, IChannel dst) {
        Package pac = new Package(construct(message), dst);
        pendingQueue.offer(pac);
    }

    @Override
    public void run() {
        while(isRunning) {
            if (pendingQueue.isEmpty()) {
                continue;
            }
            Package pac = pendingQueue.poll();
            if (pac == null) {
                continue;
            }
            IChannel dst = pac.getDst();
            dst.asyncWrite(JSON.toJSONString(pac.getContent()));
        }
    }

    public int getPendingCount() {
        return pendingQueue.size();
    }

    /**
     * 修改isRunning变量为false，使发送线程停止发送队列中的消息
     *
     * @return 是否发生修改, 如isRunning已经为false则返回false
     */
    public boolean pause() {
        if (isRunning) {
            isRunning = false;
            return true;
        }
        return false;
    }
}
