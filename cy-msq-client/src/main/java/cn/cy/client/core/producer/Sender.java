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

    private volatile boolean isRunning = true;

    private ConcurrentLinkedQueue<Package> pendingQueue = new ConcurrentLinkedQueue<>();

    public static final Sender INSTANCE = new Sender();

    private Sender() {
    }

    private BaseInfo<CommitRequest> construct(String message) {
        CommitRequest request = new CommitRequest(message);
        return new BaseInfo<>(RequestType.MESSAGE_COMMIT.id, null, UUID.randomUUID().toString(), request);
    }

    public void send(String message, IChannel dst) {
        Package pac = new Package(construct(message), dst);
        pendingQueue.add(pac);
    }

    @Override
    public void run() {
        while(isRunning) {
            pollPending();
        }
    }

    private void pollPending() {
        if (!pendingQueue.isEmpty()) {
            Package pac = pendingQueue.poll();
            if (pac == null) {
                return;
            }
            IChannel dst = pac.getDst();
            dst.asyncWrite(JSON.toJSONString(pac.getContent()));
        }
    }
}
