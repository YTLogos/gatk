package org.broadinstitute.hellbender.tools.copynumber.coverage.readcount;

import org.broadinstitute.barclay.utils.Utils;
import org.broadinstitute.hellbender.tools.exome.Target;
import org.broadinstitute.hellbender.utils.param.ParamUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.tsv.DataLine;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;

import java.util.*;
import java.util.stream.Collector;

/**
 * Stores raw read count for a single interval
 *
 * @author Andrey Smirnov &lt;asmirnov@broadinstitute.org&gt;
 */
public class RawReadCountData extends ReadCountData {

    private Target target;
    private int count;
    protected final static String RAW_COLUMN = "RAW";

    /**
     * Construct an empty instance of a raw read count
     *
     * @param target corresponding target
     */
    public RawReadCountData(Target target) {
        this.target = Utils.nonNull(target, "Target cannot be null. ");
        count = 0;
    }

    /**
     * Construct an instance of raw read count given target and the raw value
     *
     * @param target corresponding target
     * @param countValue raw read count value
     */
    protected RawReadCountData(Target target, int countValue) {
        this.target = Utils.nonNull(target, "Target cannot be null. ");
        count = ParamUtils.isPositiveOrZero(countValue, "Raw read count value cannot be negative");
    }

    @Override
    public Target getTarget() {
        return target;
    }

    @Override
    public void updateReadCount(GATKRead read) {
        count++;
    }

    @Override
    public void appendCountsTo(DataLine dataLine) {
        dataLine.append(count);
    }

    @Override
    public TableColumnCollection getReadCountDataColumns() {
        return new TableColumnCollection(Arrays.asList(RAW_COLUMN));
    }

    /**
     *
     * @return
     */
    public int getCount() {
        return count;
    }
}
