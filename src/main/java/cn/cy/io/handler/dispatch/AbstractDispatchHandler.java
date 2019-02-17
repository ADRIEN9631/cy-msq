package cn.cy.io.handler.dispatch;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import cn.cy.io.vo.protobuf.BaseInfoOuterClass.BaseInfo;

/**
 * 这里使用一个责任链模式, 消息种类不多不影响效率
 * 在这里判断消息类型 {@link cn.cy.io.vo.RequestType}，并且触发相对应的核心逻辑
 */
public abstract class AbstractDispatchHandler extends ChannelInboundHandlerAdapter {

    /**
     * 这时候的msg已经是 {@link cn.cy.io.vo.protobuf.BaseInfoOuterClass.BaseInfo} 类型的了, 区别就在于泛型的不同
     *
     * @param ctx
     * @param msg
     *
     * @throws Exception
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        BaseInfo baseInfo = (BaseInfo) msg;

        if (accepted(baseInfo)) {
            handle(baseInfo, ctx);
        } else {
            ctx.fireChannelRead(msg);
        }

    }

    /**
     * 执行核心的逻辑, 由于netty线程模型, <b>千万不要在其中做耗时操作, 长时间阻塞</b>
     */
    protected abstract void handle(BaseInfo baseInfo, ChannelHandlerContext ctx);

    /**
     * 是否接受这种消息
     *
     * @return
     */
    protected abstract boolean accepted(BaseInfo baseInfo);
}
