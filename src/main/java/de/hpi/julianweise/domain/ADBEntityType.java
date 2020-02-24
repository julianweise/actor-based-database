package de.hpi.julianweise.domain;

import de.hpi.julianweise.utility.CborSerializable;

public interface ADBEntityType extends CborSerializable {
    Comparable<?> getPrimaryKey();
}