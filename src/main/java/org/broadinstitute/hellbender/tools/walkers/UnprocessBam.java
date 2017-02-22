package org.broadinstitute.hellbender.tools.walkers;


import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMTag;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.read.SAMFileGATKReadWriter;

import java.io.File;
import java.util.Collections;
import java.util.List;


@CommandLineProgramProperties(summary = "unprocess a production file to it's most original state",
        oneLineSummary = "unmark duplicates, revert bqsr, and sort in queryname order",
        programGroup = ReadProgramGroup.class)
public class UnprocessBam extends ReadWalker {

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file")
    public File OUTPUT;

    private SAMFileGATKReadWriter outputWriter;

    @Override
    public void onTraversalStart() {
        outputWriter = createSAMWriter(OUTPUT, false);
    }

    @Override
    public List<ReadFilter> getDefaultReadFilters() {
        return Collections.singletonList(ReadFilterLibrary.ALLOW_ALL_READS);
    }

    @Override
    public void apply(GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext) {
        ReadUtils.resetOriginalBaseQualities(read);
        read.clearAttribute(SAMTag.OQ.name());
        read.setIsDuplicate(false);
        outputWriter.addRead(read);
    }

    @Override
    protected SAMFileHeader getHeaderForSAMWriter() {
        final SAMFileHeader header = super.getHeaderForSAMWriter().clone();
        header.setSortOrder(SAMFileHeader.SortOrder.queryname);
        return header;
    }

    @Override
    public Object onTraversalSuccess() {
        logger.info("Closing writer, prepare for sorting!");
        outputWriter.close();
        return null;
    }
}
