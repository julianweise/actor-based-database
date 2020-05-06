package de.hpi.julianweise.master.io;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ADBResultWriter extends AbstractBehavior<ADBResultWriter.Command> {

    public interface Command {}

    private final static Logger LOG = LoggerFactory.getLogger(ADBResultWriter.class);
    private static final String RESULT_DIR = System.getProperty("user.dir") + "/" + "results";
    private static final String RESULT_FILE_NAME = "results.txt";

    public static Behavior<Command> create() {
        return Behaviors.setup(ADBResultWriter::new);
    }

    public static String ensureResultDirExists(int requestId, int transactionId) {
        String path = RESULT_DIR + "/REQ# " + requestId + "_TX#" + transactionId;
        File directory = new File(path);
        if (!directory.exists()){
            if (!directory.mkdirs()) {
                LOG.error("Unable to create result directory for query results");
            }
        }
        return path;
    }

    @AllArgsConstructor
    public static class Persist implements Command {
        private final int transactionId;
        private final int requestId;
        private final Object[] results;
    }

    public ADBResultWriter(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Persist.class, this::handlePersist)
                .build();
    }

    private Behavior<Command> handlePersist(Persist command) throws IOException {
        String resultDir = ADBResultWriter.ensureResultDirExists(command.requestId, command.transactionId);
        File file = new File(resultDir + "/" + RESULT_FILE_NAME);
        FileOutputStream outputStream = new FileOutputStream(file);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
        for (Object element : command.results) {
            bufferedWriter.write(element.toString());
            bufferedWriter.newLine();
        }
        bufferedWriter.flush();
        bufferedWriter.close();
        outputStream.flush();
        outputStream.close();
        return Behaviors.same();
    }

}
