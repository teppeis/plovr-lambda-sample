package example;

import com.google.common.collect.Lists;
import org.plovr.cli.Command;

import java.io.IOException;
import java.util.ArrayList;

public class PlovrSample {
    public static void main(String[] args) {
        final Command command = Command.getCommandForName("build");
        final ArrayList<String> argsList = Lists.newArrayList("src/test/resources/js/plovr.json");
        try {
            int status = command.execute(argsList.toArray(new String[argsList.size()]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
