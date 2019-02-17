package cn.cy.io.handler.dispatch;

import cn.cy.io.vo.protobuf.BaseInfoOuterClass.BaseInfo;
import io.netty.channel.ChannelHandlerContext;

/**
 * 这里处理 {@link cn.cy.io.vo.request.CommitRequest} 这样类型的信息
 */
public class CommitRequestDispatchHandler extends AbstractDispatchHandler {

    @Override
    protected void handle(BaseInfo baseInfo, ChannelHandlerContext ctx) {

    }

    @Override
    protected boolean accepted(BaseInfo baseInfo) {
        return false;
    }

}
