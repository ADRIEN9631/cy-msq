package cn.cy.client.core.producer;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.fastjson.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.cy.client.core.NettyClient;
import cn.cy.client.core.channel.CyChannel;
import cn.cy.client.core.channel.IChannel;
import cn.cy.client.exceptions.ClientException;
import cn.cy.client.exceptions.ConnectionFailException;

public class CyProducer implements IProducer {

    private static final Logger logger = LoggerFactory.getLogger(CyProducer.class);

    private String id;

    private IChannel channel;

    private Sender sender;

    //执行发送任务的线程
    private ExecutorService ioThread = Executors.newSingleThreadExecutor();

    public CyProducer(CyChannel channel) {
        this.id = UUID.randomUUID().toString();
        this.channel = channel;
        this.sender = Sender.INSTANCE;
        ioThread.submit(sender);
    }

    public CyProducer(String host, int port) {
        try {
            this.id = UUID.randomUUID().toString();
            this.channel = NettyClient.INSTANCE.connect(host, port);
            this.sender = Sender.INSTANCE;
            ioThread.submit(sender);
        } catch (InterruptedException | ConnectionFailException e) {
            logger.error("Create producer fail, cannot connect to host {}, port {}", host, port);
            throw new ClientException();
        }
    }

    @Override
    public void send(String message) {
        try {
            this.sender.send(JSON.toJSONString(message), channel);
        } catch (Exception e) {
            logger.error("Error when sending message, {}", message);
        }
    }

    @Override
    public IChannel getChannel() {
        return this.channel;
    }

    @Override
    public String getId() {
        return this.id;
    }

    public Sender getSender() {
        return sender;
    }
}
