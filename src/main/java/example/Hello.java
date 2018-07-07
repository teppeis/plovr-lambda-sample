package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Hello implements RequestHandler<Integer, String>{
    @Override
    public String handleRequest(Integer input, Context context) {
        return String.valueOf(input * 2);
    }
}