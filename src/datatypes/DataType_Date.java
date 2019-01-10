package datatypes;

import java.text.SimpleDateFormat;
import java.util.Date;

import common.DatabaseConstants;
import datatypes.base.DataType_Numeric;


public class DataType_Date extends DataType_Numeric<Long> {
	
	public DataType_Date() {
        this(0, true);
    }

    public DataType_Date(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public DataType_Date(long value, boolean isNull) {
        super(DatabaseConstants.DATE_SERIAL_TYPE_CODE, DatabaseConstants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Long.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    public String getStringValue() {
        Date date = new Date(this.value);
        return new SimpleDateFormat("MM-dd-yyyy").format(date);
    }

    @Override
    public void increment(Long value) {
        this.value += value;
    }

    @Override
    public boolean compare(DataType_Numeric<Long> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case DataType_Numeric.EQUALS:
                return value == object2.getValue();

            case DataType_Numeric.GREATER_THAN:
                return value > object2.getValue();

            case DataType_Numeric.LESS_THAN:
                return value < object2.getValue();

            case DataType_Numeric.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case DataType_Numeric.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }

}
