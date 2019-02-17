package cn.cy.io.handler;

import cn.cy.io.handler.dispatch.CommitRequestDispatchHandler;
import cn.cy.io.vo.protobuf.BaseInfoOuterClass;
import com.google.common.collect.Lists;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ProtobufCommitRequestTest {

    private List<BaseInfoOuterClass.BaseInfo> mqDatas;

    private String testStr = "test1";

    @Before
    public void init() {
        // 准备数据
        int x = 10;
        mqDatas = Lists.newArrayList();

        for (int i = 0; i < x; i++) {
            BaseInfoOuterClass.BaseInfo.Builder baseInfo = BaseInfoOuterClass.BaseInfo.newBuilder();
            baseInfo.setRequestId(String.valueOf(i));
            baseInfo.setType(1);
            baseInfo.setData(Any.pack(StringValue.of(testStr)));
            baseInfo.setDesc(testStr);
            mqDatas.add(baseInfo.build());
        }
    }


    @Test
    public void protobufDispatch() throws InvalidProtocolBufferException {

        //为了方便测试去除了ProtobufVarint32FrameDecoder
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(
                new ProtobufDecoder(BaseInfoOuterClass.BaseInfo.getDefaultInstance()),
                new ProtobufVarint32LengthFieldPrepender(),
                new ProtobufEncoder(),
                new CommitRequestDispatchHandler()
        );

        writeAndFlushDatas(mqDatas, embeddedChannel);

        // 测试解出来的消息对不对
        int idx = 0;
        while (true) {
            BaseInfoOuterClass.BaseInfo baseInfo = embeddedChannel.readInbound();
            if (baseInfo == null) {
                break;
            }
            Assert.assertEquals(baseInfo.getRequestId(), String.valueOf(idx));
            Assert.assertEquals(baseInfo.getData().unpack(StringValue.class).getValue(), testStr);
            idx++;
        }

        Assert.assertEquals(idx, mqDatas.size());
    }

    protected void writeAndFlushDatas(List<BaseInfoOuterClass.BaseInfo> infos, EmbeddedChannel embeddedChannel) {
        ByteBuf byteBuf = Unpooled.buffer();

        for (BaseInfoOuterClass.BaseInfo mqData : infos) {
            byteBuf.writeBytes(mqData.toByteArray());
        }
        Assert.assertTrue(embeddedChannel.writeInbound(byteBuf.retain()));
        Assert.assertTrue(embeddedChannel.finish());
    }
}