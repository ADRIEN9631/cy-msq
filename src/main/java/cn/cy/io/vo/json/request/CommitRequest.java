package cn.cy.io.vo.json.request;

/**
 * producer生产消息
 */
public class CommitRequest {

    /**
     * 消息本体
     */
    private String msg;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
