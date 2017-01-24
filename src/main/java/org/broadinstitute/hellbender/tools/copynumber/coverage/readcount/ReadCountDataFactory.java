package org.broadinstitute.hellbender.tools.copynumber.coverage.readcount;

import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.exome.Target;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;

import java.util.*;

/**
 * Utility class to construct {@link ReadCountData} objects
 *
 * @author Andrey Smirnov &lt;asmirnov@broadinstitute.org&gt;
 */
public final class ReadCountDataFactory {

    /**
     * Enum class representing different types of coverage collection
     */
    public enum ReadCountType {
        RAW("RAW");

        private final String readCountTypeName;

        ReadCountType(String name) {
            readCountTypeName = name;
        }

        public String getReadCountTypeName() {
            return readCountTypeName;
        }
    }

    private static final Map<ReadCountType, TableColumnCollection> keyToColumnsMap = new EnumMap<>(ReadCountType.class);
    private static final Map<String, ReadCountType> nameToReadCountTypeMap = new HashMap<>();

    //populate the map between all read count types and their corresponding columns, and a map between read count type names
    //and their corresponding enum instances
    static {
        final Target dummyTarget = new Target("DUMMY");
        Arrays.stream(ReadCountType.values()).forEach(key ->
                keyToColumnsMap.put(key, ReadCountDataFactory.getReadCountDataObject(key, dummyTarget).getReadCountDataColumns()));
        Arrays.stream(ReadCountType.values()).forEach(key ->
                nameToReadCountTypeMap.put(key.getReadCountTypeName(), key));
    }

    // private constructor to prevent initializing
    private ReadCountDataFactory() {}

    /**
     * Construct a new instance of {@link ReadCountData} object
     *
     * @param type type of the read count data
     * @return empty instance of read count data object
     */
    public static ReadCountData getReadCountDataObject(ReadCountType type, Target target) {
        switch(type) {
            case RAW:
                return new RawReadCountData(target);
            default:
                throw new UserException.BadInput(String.format(" %s is not a recognized read count type.", type));
        }
    }

    /**
     *
     * @param type
     * @param target
     * @param columnValues
     * @param <N>
     * @return
     */
    public static <N extends Number> ReadCountData getReadCountDataObject(ReadCountType type, Target target, Map<String, N> columnValues) {
        switch(type) {
            case RAW:
                Integer rawReadCount = Utils.nonNull((Integer) columnValues.get(RawReadCountData.RAW_COLUMN),
                        String.format(" %s column value is required to construct %s read count data ", RawReadCountData.RAW_COLUMN, ReadCountType.RAW.toString()));
                return new RawReadCountData(target, rawReadCount);
            default:
                throw new UserException.BadInput(String.format(" %s is not a recognized read count type.", type));
        }
    }

    /**
     * Get read count columns for a particular read count type
     *
     * @param readCountType type of the read count data
     * @return list of columns
     */
    public static TableColumnCollection getColumnsOfReadCountType (ReadCountType readCountType) {
        return keyToColumnsMap.get(readCountType);
    }


    /**
     * Get the read count type by its name
     * @param name name
     * @return read count type
     */
    public static ReadCountType getReadCountTypeByName (String name) {
        return nameToReadCountTypeMap.get(name);
    }
}
