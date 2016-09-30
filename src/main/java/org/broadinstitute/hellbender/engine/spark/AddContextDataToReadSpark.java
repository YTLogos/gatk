package org.broadinstitute.hellbender.engine.spark;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.engine.ReadContextData;
import org.broadinstitute.hellbender.engine.Shard;
import org.broadinstitute.hellbender.engine.ShardBoundary;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipList;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.spark.SparkUtils;
import org.broadinstitute.hellbender.utils.variant.GATKVariant;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * AddContextDataToRead pairs reference bases and overlapping variants with each GATKRead in the RDD input.
 * The variants are obtained from a local file (later a GCS Bucket). The reference bases come from the Google Genomics API.
 *
 * This transform is intended for direct use in pipelines.
 *
 * This transform will filter out any unmapped reads.
 *
 * The reference bases paired with each read can be customized by passing in a reference window function
 * inside the {@link org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource} argument to {@link #add}. See
 * {@link org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions} for examples.
 */
public class AddContextDataToReadSpark {
    public static JavaPairRDD<GATKRead, ReadContextData> add(
            final JavaRDD<GATKRead> reads, final ReferenceMultiSource referenceDataflowSource,
            final JavaRDD<GATKVariant> variants, final JoinStrategy joinStrategy) {
        // TODO: this static method should not be filtering the unmapped reads.  To be addressed in another issue.
        JavaRDD<GATKRead> mappedReads = reads.filter(read -> ReadFilterLibrary.MAPPED.test(read));
        JavaPairRDD<GATKRead, Tuple2<Iterable<GATKVariant>, ReferenceBases>> withVariantsWithRef;
        if (joinStrategy.equals(JoinStrategy.BROADCAST)) {
            // Join Reads and Variants
            JavaPairRDD<GATKRead, Iterable<GATKVariant>> withVariants = BroadcastJoinReadsWithVariants.join(mappedReads, variants);
            // Join Reads with ReferenceBases
            withVariantsWithRef = BroadcastJoinReadsWithRefBases.addBases(referenceDataflowSource, withVariants);
        } else if (joinStrategy.equals(JoinStrategy.SHUFFLE)) {
            // Join Reads and Variants
            JavaPairRDD<GATKRead, Iterable<GATKVariant>> withVariants = ShuffleJoinReadsWithVariants.join(mappedReads, variants);
            // Join Reads with ReferenceBases
            withVariantsWithRef = ShuffleJoinReadsWithRefBases.addBases(referenceDataflowSource, withVariants);
        } else {
            throw new UserException("Unknown JoinStrategy");
        }
        return withVariantsWithRef.mapToPair(in -> new Tuple2<>(in._1(), new ReadContextData(in._2()._2(), in._2()._1())));
    }

    private static final int SHARD_SIZE = 10000;
    private static final int SHARD_PADDING = 1000;

    public static JavaPairRDD<GATKRead, ReadContextData> add(
            final JavaSparkContext ctx,
            final JavaRDD<GATKRead> reads, final ReferenceMultiSource referenceSource,
            final JavaRDD<GATKVariant> variants, final SAMSequenceDictionary sequenceDictionary) {
        JavaRDD<GATKRead> mappedReads = reads.filter(read -> ReadFilterLibrary.MAPPED.test(read));

        final List<SimpleInterval> intervals = IntervalUtils.getAllIntervalsForReference(sequenceDictionary);
        final List<ShardBoundary> intervalShards = intervals.stream()
                .flatMap(interval -> Shard.divideIntervalIntoShards(interval, SHARD_SIZE, SHARD_SIZE, SHARD_PADDING, sequenceDictionary).stream())
                .collect(Collectors.toList());

        final Broadcast<ReferenceMultiSource> bReferenceSource = ctx.broadcast(referenceSource);
        final IntervalsSkipList<GATKVariant> variantSkipList = new IntervalsSkipList<>(variants.collect());
        final Broadcast<IntervalsSkipList<GATKVariant>> variantsBroadcast = ctx.broadcast(variantSkipList);

        JavaRDD<Shard<GATKRead>> shardedReads = SparkUtils.shard(ctx, mappedReads, GATKRead.class, sequenceDictionary, intervalShards);
        return shardedReads.flatMapToPair(new PairFlatMapFunction<Shard<GATKRead>, GATKRead, ReadContextData>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<GATKRead, ReadContextData>> call(Shard<GATKRead> shard) throws Exception {
                // get reference bases for this shard (padded)
                ReferenceBases referenceBases = bReferenceSource.getValue().getReferenceBases(null, shard.getPaddedInterval());
                final IntervalsSkipList<GATKVariant> intervalsSkipList = variantsBroadcast.getValue();
                Iterator<Tuple2<GATKRead, ReadContextData>> transform = Iterators.transform(shard.iterator(), new Function<GATKRead, Tuple2<GATKRead, ReadContextData>>() {
                    @Nullable
                    @Override
                    public Tuple2<GATKRead, ReadContextData> apply(@Nullable GATKRead r) {
                        List<GATKVariant> overlappingVariants;
                        if (SimpleInterval.isValid(r.getContig(), r.getStart(), r.getEnd())) {
                            overlappingVariants = intervalsSkipList.getOverlapping(new SimpleInterval(r));
                        } else {
                            //Sometimes we have reads that do not form valid intervals (reads that do not consume any ref bases, eg CIGAR 61S90I
                            //In those cases, we'll just say that nothing overlaps the read
                            overlappingVariants = Collections.emptyList();
                        }
                        return new Tuple2<>(r, new ReadContextData(referenceBases, overlappingVariants));
                    }
                });
                // only include reads that start in the shard
                return () -> Iterators.filter(transform, r -> r._1().getStart() >= shard.getStart());
            }
        });
    }
}
