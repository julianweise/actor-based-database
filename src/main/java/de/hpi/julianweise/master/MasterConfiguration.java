package de.hpi.julianweise.master;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.hpi.julianweise.utility.ConfigurationBase;
import de.hpi.julianweise.utility.FileValidator;
import lombok.Getter;

import java.nio.file.Path;
import java.nio.file.Paths;

@Parameters(commandDescription = "start a master actor system")
@Getter
public class MasterConfiguration implements ConfigurationBase {

    private static class StringToPathConverter implements IStringConverter<Path> {
        @Override
        public Path convert(String path) {
            return Paths.get(path);
        }
    }

    @Parameter(names = {"-i", "--input"}, description = "data to handle", validateValueWith = FileValidator.class,
               converter = StringToPathConverter.class)
    Path inputFile;

    @Parameter(names = {"-p", "--port"}, description = "port to run application on")
    int port;

    public OperationRole role() {
        return OperationRole.MASTER;
    }
}
