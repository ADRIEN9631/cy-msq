package cn.cy.io.vo.json.response;

/**
 * 消息提交之后的回应，可以理解为ack
 */
public class CommitResponse {

    /**
     * 消息是否成功投递
     */
    private boolean success;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
