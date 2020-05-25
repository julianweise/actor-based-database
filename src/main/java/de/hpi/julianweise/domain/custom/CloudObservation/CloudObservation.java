package de.hpi.julianweise.domain.custom.CloudObservation;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class CloudObservation extends ADBEntity {

    private int timestamp;
    private boolean skyBrightnessIndicator;
    private short latitude;
    private short longitude;
    private int stationNumber;
    private boolean landIndicator;
    private byte presentWeather;
    private byte totalCloudCover;
    private byte lowerCloudAmount;
    private byte lowerCloudBasedHeight;
    private byte lowCloudType;
    private byte middleCloudType;
    private byte highCloudType;
    private short middleCloudAmount;
    private short highCloudAmount;
    private byte nonOverheadMiddleCloudAmount;
    private byte nonOverheadHighCloudAmount;
    private byte changeCode;
    private short solarAltitude;
    private byte relativeLunarIlluminance;
    private short seaLevelPressure;
    private short windSpeed;
    private short windDirection;
    private short airTemperature;
    private short dewPointDepression;
    private short stationElevation;
    private byte windSpeedIndicator;
    private byte seaLevelPressureFlag;


    @Override
    public ADBKey getPrimaryKey() {
        return null;
    }

    @Override
    public int getSize() {
        return 2 * Integer.BYTES + 11 * Short.BYTES + 13 * Byte.BYTES + 2 + 2;
    }

    @Override
    public String toString() {
        return "[CloudObservation] at date " + timestamp + " at station " + stationNumber + " in " + landIndicator;
    }
}
