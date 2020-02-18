package de.hpi.julianweise.slave;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.hpi.julianweise.utility.ConfigurationBase;
import lombok.Getter;

@Getter
@Parameters(commandDescription = "start a slave actor system")
public class SlaveConfiguration implements ConfigurationBase {

    @Parameter(names = {"-p", "--port"}, description = "port to run application on")
    int port;

    public OperationRole role() {
        return OperationRole.SLAVE;
    }
}
