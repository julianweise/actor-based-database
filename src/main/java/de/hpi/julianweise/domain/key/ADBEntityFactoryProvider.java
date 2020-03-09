package de.hpi.julianweise.domain.key;

import de.hpi.julianweise.domain.ADBEntityFactory;

public class ADBEntityFactoryProvider {

    private static ADBEntityFactory Instance;

    public ADBEntityFactoryProvider(ADBEntityFactory factory) {
        if (ADBEntityFactoryProvider.Instance == null) {
            ADBEntityFactoryProvider.Instance = factory;
        }
    }

    public static ADBEntityFactory getInstance() {
        if (ADBEntityFactoryProvider.Instance == null) {
            throw new IllegalStateException("ADBEntityFactory has not been instantiated yet");
        }
        return ADBEntityFactoryProvider.Instance;
    }
}
