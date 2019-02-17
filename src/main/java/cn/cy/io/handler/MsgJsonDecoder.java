package cn.cy.io.handler;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import cn.cy.io.vo.json.BaseInfo;
import cn.cy.io.vo.RequestType;
import cn.cy.io.vo.json.request.CommitRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * 这个 {@link JsonObjectDecoder} 只会把东西按照括号分组
 * 分组完了之后，会<b>依次</b>触发ctx的channelRead事件, 也就是把这些对象接着往下传递
 * <p>
 * 这里是把那些对象,变成业务可以理解的对象的过程
 */
@ChannelHandler.Sharable
public class MsgJsonDecoder extends ChannelInboundHandlerAdapter {

    /**
     * 处理真实的对象
     *
     * @param ctx ctx
     * @param msg ByteBuf对象
     *
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 到这里的时候, 已经是分好组的json, 每个都是ByteBuf
        if (msg instanceof ByteBuf) {
            ByteBuf input = (ByteBuf) msg;

            String rawStr = (String) input.readCharSequence((input).readableBytes(), Charset.defaultCharset());

            BaseInfo baseInfo = JSON.parseObject(rawStr, BaseInfo.class);

            baseInfo = buildRealObject(baseInfo, rawStr);

            attachMsg(ctx, baseInfo);

            ctx.fireChannelRead(baseInfo);
        } else {
            throw new IllegalArgumentException(
                    "invalid type is received by the MsgJsonDecoder, type is " + msg.getClass());
        }
    }

    private void attachMsg(ChannelHandlerContext ctx, BaseInfo baseInfo) {

        Attribute<List<BaseInfo>> attribute = ctx.channel().attr(AttributeKey.valueOf("msg"));

        List<BaseInfo> messageList = attribute.get();

        if (messageList == null) {
            messageList = new ArrayList<>();
            messageList.add(baseInfo);
            attribute.set(messageList);
        } else {
            messageList.add(baseInfo);
        }

    }

    /**
     * 实例化对象
     *
     * @param rawStr
     *
     * @return
     */
    private BaseInfo buildRealObject(BaseInfo request, String rawStr) {

        // 就这样干吧..
        if (request.getType() == RequestType.MESSAGE_COMMIT.id) {

            BaseInfo<CommitRequest> commitRequest = JSON.parseObject(rawStr, new
                    TypeReference<BaseInfo<CommitRequest>>() {
                    });

            return commitRequest;

        } else if (request.getType() == RequestType.MESSAGE_PULL.id) {

        }

        return null;
    }
}
