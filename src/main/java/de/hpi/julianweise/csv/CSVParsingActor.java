package de.hpi.julianweise.csv;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class CSVParsingActor extends AbstractBehavior<CSVParsingActor.Command> {

    private final CsvParser csvParser;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());

    public interface Command {}

    public interface Response {}

    @AllArgsConstructor
    @Getter
    public static class ParseNextCSVChunk implements CSVParsingActor.Command {
        private final ActorRef<Response> client;
    }

    @Getter
    @AllArgsConstructor
    public static class CSVDataChunk implements Response {
        private final ObjectList<Record> chunk;
        private final boolean finalChunk;
    }

    @Getter
    @AllArgsConstructor
    public static class CSVFullyParsed implements Response {}

    protected CSVParsingActor(ActorContext<CSVParsingActor.Command> context, String filePath) {
        super(context);
        CsvParserSettings settings = new CsvParserSettings();
        settings.detectFormatAutomatically();
        settings.setNullValue(" ");
        this.csvParser = new CsvParser(settings);
        this.csvParser.beginParsing(this.locateCSVFile(filePath));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ParseNextCSVChunk.class, this::handleParseNextCSVChunk)
                .build();
    }

    @SneakyThrows(FileNotFoundException.class)
    private FileReader locateCSVFile(String csvPath) {
        File csvFile = new File(csvPath);
        return new FileReader(csvFile);
    }

    private Behavior<Command> handleParseNextCSVChunk(ParseNextCSVChunk command) {
        int counter = 0;
        ObjectList<Record> chunk = new ObjectArrayList<>(this.settings.CSV_CHUNK_SIZE);
        Record record;
        while ((record = this.csvParser.parseNextRecord()) != null) {
            chunk.add(record);
            if (++counter >= this.settings.CSV_CHUNK_SIZE) {
                command.getClient().tell(new CSVDataChunk(chunk, false));
                return Behaviors.same();
            }
        }
        if (chunk.size() > 0) command.getClient().tell(new CSVDataChunk(chunk, true));
        command.getClient().tell(new CSVFullyParsed());
        return Behaviors.same();
    }
}
