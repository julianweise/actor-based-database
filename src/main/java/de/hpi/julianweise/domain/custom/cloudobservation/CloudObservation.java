package de.hpi.julianweise.domain.custom.cloudobservation;

import de.hpi.julianweise.domain.key.ADBStringKey;
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

    public int _timestamp;
    public boolean skyBrightnessIndicator;
    public short latitude;
    public int longitude;
    public int stationNumber;
    public boolean landIndicator;
    public byte presentWeather;
    public byte totalCloudCover;
    public byte lowerCloudAmount;
    public byte lowerCloudBasedHeight;
    public byte lowCloudType;
    public byte middleCloudType;
    public byte highCloudType;
    public short middleCloudAmount;
    public short highCloudAmount;
    public byte nonOverheadMiddleCloudAmount;
    public byte nonOverheadHighCloudAmount;
    public byte changeCode;
    public short solarAltitude;
    public byte relativeLunarIlluminance;
    public short seaLevelPressure;
    public short windSpeed;
    public short windDirection;
    public short airTemperature;
    public short dewPointDepression;
    public short stationElevation;
    public byte windSpeedIndicator;
    public byte seaLevelPressureFlag;


    @Override
    public ADBKey getPrimaryKey() {
        return new ADBStringKey(this._timestamp + "" + this.stationNumber);
    }

    @Override
    public int getSize() {
        return 3 * Integer.BYTES + 10 * Short.BYTES + 13 * Byte.BYTES + 2 + 2;
    }

    @Override
    public String toString() {
        return "[CloudObservation] at date " + _timestamp + " at station " + stationNumber + " in " + landIndicator;
    }
}
