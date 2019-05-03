package cn.cy.client.core.producer;

import cn.cy.client.core.channel.IChannel;
import cn.cy.io.vo.BaseInfo;

public class Package {

    private IChannel dst;

    private BaseInfo content;

    public Package(BaseInfo content, IChannel dst) {
        this.dst = dst;
        this.content = content;
    }

    public IChannel getDst() {
        return dst;
    }

    public BaseInfo getContent() {
        return content;
    }
}