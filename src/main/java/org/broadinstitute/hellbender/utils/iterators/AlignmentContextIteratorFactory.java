package org.broadinstitute.hellbender.utils.iterators;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.engine.AlignmentContext;
import org.broadinstitute.hellbender.engine.ReferenceDataSource;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.locusiterator.LIBSDownsamplingInfo;
import org.broadinstitute.hellbender.utils.locusiterator.LocusIteratorByState;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;

/**
 * Create an iterator for traversing alignment contexts in a specified manner.  This class should be able to support both spark and non-spark LocusWalker
 */
public class AlignmentContextIteratorFactory {

    protected static final Logger logger = LogManager.getLogger(AlignmentContextIteratorFactory.class);

    private AlignmentContextIteratorFactory() {}

    /**
     *  Create the appropriate instance of an alignment context spliterator based on the input parameters.
     *
     *  Please note that this wrapper is still tied to {@link LocusIteratorByState} and some parameters are being passed directly to that class.
     *
     * @param intervalsForTraversal the intervals to generate alignment contexts over.
     * @param header SAM file header to use
     * @param readIterator iterator of sorted GATK reads
     * @param dictionary the SAMSequenceDictionary being used for this traversal.  This can be the same as the reference.  {@code null} is supported, but will often lead to invalid parameter combinations.
     * @param downsamplingInfo how to downsample (for {@link LocusIteratorByState})
     * @param reference the reference being used for this traversal, {@code null} if no reference being used.
     * @param emitEmptyLoci whether loci with no coverage should be emitted.  In this case, the AlignmentContext will be empty (not null).
     * @param isKeepUniqueReadListInLibs if true, we will keep the unique reads from the samIterator and make them
     *                                       available via the transferReadsFromAllPreviousPileups interface (this parameter is specific to {@link LocusIteratorByState})
     * @param isIncludeDeletions include reads with deletion on the loci in question
     * @param isIncludeNs include reads with N on the loci in question
     * @return Spliterator that produces AlignmentContexts ready for consumption (e.g. by a {@link org.broadinstitute.hellbender.engine.LocusWalker})
     */
    public static Iterator<AlignmentContext> createAlignmentContextIterator(final List<SimpleInterval> intervalsForTraversal,
                                                                            final SAMFileHeader header,
                                                                               final Iterator<GATKRead> readIterator,
                                                                               final SAMSequenceDictionary dictionary,
                                                                               final LIBSDownsamplingInfo downsamplingInfo,
                                                                               final SAMSequenceDictionary reference,
                                                                               boolean emitEmptyLoci,
                                                                               boolean isKeepUniqueReadListInLibs,
                                                                               boolean isIncludeDeletions,
                                                                               boolean isIncludeNs) {

        // get the samples from the read groups
        final Set<String> samples = header.getReadGroups().stream()
                .map(SAMReadGroupRecord::getSample)
                .collect(Collectors.toSet());

        // get the LIBS
        final LocusIteratorByState libs = new LocusIteratorByState(readIterator, downsamplingInfo, isKeepUniqueReadListInLibs, samples, header, isIncludeDeletions, isIncludeNs);
        Iterator<AlignmentContext> iterator;

        List<SimpleInterval> finalIntervals = intervalsForTraversal;
        validateEmitEmptyLociParameters(emitEmptyLoci, dictionary, intervalsForTraversal, reference);
        if (emitEmptyLoci) {

            // If no intervals were specified, then use the entire reference (or best available sequence dictionary).
            if (!hasIntervals(finalIntervals)) {
                finalIntervals = IntervalUtils.getAllIntervalsForReference(dictionary);
            }
            final IntervalLocusIterator intervalLocusIterator = new IntervalLocusIterator(finalIntervals.iterator());
            iterator =  new IntervalAlignmentContextIterator(libs, intervalLocusIterator, header.getSequenceDictionary());

        } else {
            // prepare the iterator
            iterator = hasIntervals(finalIntervals) ? new IntervalOverlappingIterator<>(libs, finalIntervals, header.getSequenceDictionary()) : libs;
        }

        return iterator;
    }

    private static boolean hasIntervals(final List<SimpleInterval> finalIntervals) {
        return finalIntervals != null;
    }

    private static boolean hasReference(final SAMSequenceDictionary reference) {
        return reference != null;
    }

    /**
     *  The emit empty loci parameter comes with several pitfalls when used incorrectly.  Here we check and either give
     *   warnings or errors.
     */
    static private void validateEmitEmptyLociParameters(boolean emitEmptyLoci, final SAMSequenceDictionary dictionary, final List<SimpleInterval> intervals, final SAMSequenceDictionary reference) {
        if (emitEmptyLoci) {
            if ((dictionary == null) && !hasReference(reference)) {
                throw new UserException.MissingReference("No sequence dictionary nor reference specified.  Therefore, emitting empty loci is impossible and this tool cannot be run.  The easiest fix here is to specify a reference dictionary.");
            }
            if (!hasReference(reference) && !hasIntervals(intervals)) {
                logger.warn("****************************************");
                logger.warn("* Running this tool without a reference nor intervals can yield unexpected results, since it will emit results for loci with no reads.  A sequence dictionary has been found and the intervals will be derived from this.  The easiest way avoid this message is to specify a reference.");
                logger.warn("****************************************");
            }
        }
    }
}
