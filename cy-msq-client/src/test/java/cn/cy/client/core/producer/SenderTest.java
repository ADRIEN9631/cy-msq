package cn.cy.client.core.producer;

import cn.cy.io.vo.BaseInfo;
import cn.cy.io.vo.request.CommitRequest;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class SenderTest {

    @Test
    public void construct() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String testMsg = "Hello";
        Method testMethod = Sender.class.getDeclaredMethod("construct", String.class);
        testMethod.setAccessible(true);
        BaseInfo<CommitRequest> result = (BaseInfo<CommitRequest>) testMethod.invoke(new Sender(), testMsg);
        assertEquals("Hello", result.getData().getMsg());
    }

}