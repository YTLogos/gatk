package org.broadinstitute.hellbender.utils.spark;

import autovalue.shaded.com.google.common.common.base.Function;
import autovalue.shaded.com.google.common.common.collect.Iterators;
import com.google.common.collect.ImmutableList;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.downsampling.DownsamplingMethod;
import org.broadinstitute.hellbender.utils.iterators.IntervalOverlappingIterator;
import org.broadinstitute.hellbender.utils.locusiterator.LocusIteratorByState;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import scala.Tuple3;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class LocusWalkerSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    private static final int SHARD_SIZE = 10000;
    private static final int SHARD_PADDING = 1000;

    @Argument(doc = "whether to use the shuffle implementation or not", shortName = "shuffle", fullName = "shuffle", optional = true)
    public boolean shuffle = false;

    private FeatureManager features; // TODO: move up to GATKSparkTool?

    @Override
    protected void runPipeline(JavaSparkContext sparkContext) {
        initializeFeatures();
        super.runPipeline(sparkContext);
    }

    void initializeFeatures() {
        features = new FeatureManager(this);
        if ( features.isEmpty() ) {  // No available sources of Features discovered for this tool
            features = null;
        }
    }

    /**
     * Loads alignments and the corresponding reference and features into a {@link JavaRDD} for the intervals specified.
     *
     * If no intervals were specified, returns all the alignments.
     *
     * @return all alignments from as a {@link JavaRDD}, bounded by intervals if specified.
     */
    public JavaRDD<Tuple3<AlignmentContext, ReferenceContext, FeatureContext>> getAlignments(JavaSparkContext ctx) {
        SAMSequenceDictionary sequenceDictionary = getBestAvailableSequenceDictionary();
        List<SimpleInterval> intervals = hasIntervals() ? getIntervals() : IntervalUtils.getAllIntervalsForReference(sequenceDictionary);
        final List<ShardBoundary> intervalShards = intervals.stream()
                .flatMap(interval -> Shard.divideIntervalIntoShards(interval, SHARD_SIZE, SHARD_SIZE, SHARD_PADDING, sequenceDictionary).stream())
                .collect(Collectors.toList());
        JavaRDD<Shard<GATKRead>> shardedReads = SparkUtils.shard(ctx, getReads(), GATKRead.class, sequenceDictionary, intervalShards, shuffle);
        Broadcast<ReferenceMultiSource> bReferenceSource = hasReference() ? ctx.broadcast(getReference()) : null;
        Broadcast<FeatureManager> bFeatureManager = features == null ? null : ctx.broadcast(features);
        return shardedReads.flatMap(getAlignmentsFunction(bReferenceSource, bFeatureManager, sequenceDictionary, getHeaderForReads()));
    }

    private static FlatMapFunction<Shard<GATKRead>, Tuple3<AlignmentContext, ReferenceContext, FeatureContext>> getAlignmentsFunction(
            Broadcast<ReferenceMultiSource> bReferenceSource, Broadcast<FeatureManager> bFeatureManager,
            SAMSequenceDictionary sequenceDictionary, SAMFileHeader header) {
        return (FlatMapFunction<Shard<GATKRead>, Tuple3<AlignmentContext, ReferenceContext, FeatureContext>>) shardedRead -> {
            SimpleInterval interval = shardedRead.getInterval();
            SimpleInterval paddedInterval = shardedRead.getPaddedInterval();
            Iterator<GATKRead> readIterator = shardedRead.iterator();
            ReferenceDataSource reference = bReferenceSource == null ? null :
                    new ReferenceMemorySource(bReferenceSource.getValue().getReferenceBases(null, paddedInterval), sequenceDictionary);
            FeatureManager fm = bFeatureManager == null ? null : bFeatureManager.getValue();

            final Set<String> samples = header.getReadGroups().stream()
                    .map(SAMReadGroupRecord::getSample)
                    .collect(Collectors.toSet());
            LocusIteratorByState libs = new LocusIteratorByState(readIterator, DownsamplingMethod.NONE, false, false, false, samples, header);
            IntervalOverlappingIterator<AlignmentContext> alignmentContexts = new IntervalOverlappingIterator<>(libs, ImmutableList.of(interval), sequenceDictionary);
            Iterator<Tuple3<AlignmentContext, ReferenceContext, FeatureContext>> transform = Iterators.transform(alignmentContexts, new Function<AlignmentContext, Tuple3<AlignmentContext, ReferenceContext, FeatureContext>>() {
                @Nullable
                @Override
                public Tuple3<AlignmentContext, ReferenceContext, FeatureContext> apply(@Nullable AlignmentContext alignmentContext) {
                    final SimpleInterval alignmentInterval = new SimpleInterval(alignmentContext);
                    return new Tuple3<>(alignmentContext, new ReferenceContext(reference, alignmentInterval), new FeatureContext(fm, alignmentInterval));
                }
            });
            return () -> transform;
        };
    }
}
