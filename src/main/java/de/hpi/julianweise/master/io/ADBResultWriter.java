package de.hpi.julianweise.master.io;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.settings.Settings;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public class ADBResultWriter extends AbstractBehavior<ADBResultWriter.Command> {

    public interface Command {}
    public interface Response {}

    private final int transactionId;
    private final String writerId = UUID.randomUUID().toString();
    private final FileOutputStream outputStream;
    private final BufferedWriter bufferedWriter;
    private final File resultFile;

    public static Behavior<Command> create(int transactionId) {
        return Behaviors.setup(ctx -> new ADBResultWriter(ctx, transactionId));
    }

    @AllArgsConstructor
    public static class Persist implements Command {
        private final Object[] results;
    }

    @AllArgsConstructor
    public static class FinalizeAndReturnResultLocation implements Command {
        ActorRef<ResultLocation> respondTo;
    }

    @AllArgsConstructor
    @Getter
    public static class ResultLocation implements Response {
        private final int transactionId;
        private final String resultLocation;
    }

    public ADBResultWriter(ActorContext<Command> context, int transactionId) throws IOException {
        super(context);
        this.transactionId = transactionId;
        this.resultFile = this.getResultFile();
        this.outputStream = new FileOutputStream(this.resultFile);
        this.bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Persist.class, this::handlePersist)
                .onMessage(FinalizeAndReturnResultLocation.class, this::handleFinalize)
                .build();
    }

    private Behavior<Command> handlePersist(Persist command) throws IOException {
        for (Object element : command.results) {
            this.writeElement(bufferedWriter, element);
        }
        return Behaviors.same();
    }

    public void writeElement(BufferedWriter bufferedWriter, Object element) throws IOException {
        bufferedWriter.write(element.toString());
        bufferedWriter.newLine();
    }

    public File getResultFile() throws IOException {
        if (!this.ensureResultDirectoryExists()) {
            this.getContext().getLog().error("Unable to create result directory!");
            return new File(System.getProperty("userDir"));
        }
        String resultFileName = String.format("TX#%s_%s", this.transactionId, this.writerId);
        Path filePath = Paths.get(Settings.SettingsProvider.get(getContext().getSystem()).RESULT_BASE_DIR, resultFileName);
        File resultFile = filePath.toFile();
        if (!resultFile.exists()){
            if (!resultFile.createNewFile()) {
                this.getContext().getLog().error("Unable to create result file " + resultFileName);
            }
        }
        return resultFile;
    }

    private boolean ensureResultDirectoryExists() {
        File resultDirectory = new File(Settings.SettingsProvider.get(getContext().getSystem()).RESULT_BASE_DIR);
        if (resultDirectory.exists()) {
            return true;
        }
        return resultDirectory.mkdirs();
    }

    private Behavior<Command> handleFinalize(FinalizeAndReturnResultLocation command) throws IOException {
        command.respondTo.tell(new ResultLocation(this.transactionId, this.resultFile.getAbsolutePath()));
        this.bufferedWriter.close();
        this.outputStream.close();
        return Behaviors.stopped();
    }

}
