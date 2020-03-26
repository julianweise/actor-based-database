package de.hpi.julianweise.settings;

import akka.actor.AbstractExtensionId;
import akka.actor.ExtendedActorSystem;
import akka.actor.ExtensionIdProvider;

public class Settings extends AbstractExtensionId<SettingsImpl> implements ExtensionIdProvider {

    public static final Settings SettingsProvider = new Settings();

    private Settings() {
    }

    public Settings lookup() {
        return Settings.SettingsProvider;
    }

    public SettingsImpl createExtension(ExtendedActorSystem system) {
        return new SettingsImpl(system.settings().config());
    }
}