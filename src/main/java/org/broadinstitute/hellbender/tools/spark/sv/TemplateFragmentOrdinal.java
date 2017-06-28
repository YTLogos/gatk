package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

/**
 * Created by valentin on 6/29/17.
 */ // Typically UNPAIRED (single read from the template), PAIRED_FIRST (first read from the template), or
// PAIRED_LAST (second read from the template).  The SAM format, however, describes other weird possibilities,
// and to avoid lying, we also allow the pairedness to be unknown, or for a read to be paired, but neither
// first nor last (interior).
public enum TemplateFragmentOrdinal {

    UNPAIRED(""), PAIRED_UNKNOWN("/?"), PAIRED_FIRST("/1"), PAIRED_SECOND("/2"), PAIRED_INTERIOR("/0");

    TemplateFragmentOrdinal(final String nameSuffix) {
        this.nameSuffix = nameSuffix;
    }

    @Override
    public String toString() {
        return nameSuffix;
    }

    public String nameSuffix() {
        return nameSuffix;
    }

    private final String nameSuffix;

    public static TemplateFragmentOrdinal forRead(final GATKRead read) {
        Utils.nonNull(read);
        if (read.isPaired()) {
            if (read.isFirstOfPair()) {
                return read.isSecondOfPair() ? PAIRED_UNKNOWN : PAIRED_FIRST;
            } else {
                return read.isSecondOfPair() ? PAIRED_SECOND : PAIRED_INTERIOR;
            }
        } else {
            return UNPAIRED;
        }
    }
}
