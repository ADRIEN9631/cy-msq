package cn.cy.io.handler;

import java.util.List;

import cn.cy.io.vo.json.BaseInfo;
import org.junit.Assert;

import com.alibaba.fastjson.JSON;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

public class BaseTest {

    /**
     * 数据打进去
     *
     * @param infos
     * @param embeddedChannel
     */
    protected void writeAndFlushDatas(List<BaseInfo> infos, EmbeddedChannel embeddedChannel) {
        ByteBuf byteBuf = Unpooled.buffer();

        for (BaseInfo mqData : infos) {
            byteBuf.writeBytes(JSON.toJSONBytes(mqData));
        }

        Assert.assertTrue(embeddedChannel.writeInbound(byteBuf.retain()));
        Assert.assertTrue(embeddedChannel.finish());
    }

}
