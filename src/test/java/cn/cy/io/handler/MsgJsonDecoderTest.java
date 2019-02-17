package cn.cy.io.handler;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import cn.cy.io.vo.json.BaseInfo;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.json.JsonObjectDecoder;

public class MsgJsonDecoderTest extends BaseTest {

    private List<BaseInfo> mqDatas;

    @Before
    public void init() {
        // 准备数据
        int x = 1000;
        mqDatas = Lists.newArrayList();

        for (int i = 0; i < x; i++) {

            BaseInfo baseInfo = new BaseInfo();
            baseInfo.setRequestId(String.valueOf(i));
            baseInfo.setType(0);

            mqDatas.add(baseInfo);
        }
    }

    @Test
    public void channelRead() {

        EmbeddedChannel embeddedChannel = new EmbeddedChannel(
                new JsonObjectDecoder(),
                new MsgJsonDecoder()
        );

        writeAndFlushDatas(mqDatas, embeddedChannel);

        // 测试解出来的消息对不对
        int idx = 0;
        while (true) {
            BaseInfo baseInfo = embeddedChannel.readInbound();
            if (baseInfo == null) {
                break;
            }
            Assert.assertEquals(baseInfo.getRequestId(), String.valueOf(idx));
            idx++;
        }

        Assert.assertEquals(idx, mqDatas.size());
    }
}