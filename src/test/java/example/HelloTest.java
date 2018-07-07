package example;

import org.junit.Assert;
import org.junit.Test;

public class HelloTest {
    @Test
    public void testHello() {
        Hello hello = new Hello();
        Assert.assertEquals("246", hello.handleRequest(123, null));
    }
}
