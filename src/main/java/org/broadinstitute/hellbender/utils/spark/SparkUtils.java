package org.broadinstitute.hellbender.utils.spark;

import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.collect.AbstractIterator;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMTextHeaderCodec;
import htsjdk.samtools.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.broadinstitute.hellbender.engine.Shard;
import org.broadinstitute.hellbender.engine.ShardBoundary;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.broadinstitute.hellbender.utils.Utils;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;


import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.broadinstitute.hellbender.utils.IntervalUtils.overlaps;

/**
 * Miscellaneous Spark-related utilities
 */
public final class SparkUtils {

    private SparkUtils() {}

    /**
     * Converts a headerless Hadoop bam shard (eg., a part0000, part0001, etc. file produced by
     * {@link org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink}) into a readable bam file
     * by adding a header and a BGZF terminator.
     *
     * This method is not intended for use with Hadoop bam shards that already have a header -- these shards are
     * already readable using samtools. Currently {@link ReadsSparkSink} saves the "shards" with a header for the
     * {@link ReadsWriteFormat#SHARDED} case, and without a header for the {@link ReadsWriteFormat#SINGLE} case.
     *
     * @param bamShard The headerless Hadoop bam shard to convert
     * @param header header for the BAM file to be created
     * @param destination path to which to write the new BAM file
     */
    public static void convertHeaderlessHadoopBamShardToBam( final File bamShard, final SAMFileHeader header, final File destination ) {
        try ( FileOutputStream outStream = new FileOutputStream(destination) ) {
            writeBAMHeaderToStream(header, outStream);
            FileUtils.copyFile(bamShard, outStream);
            outStream.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
        }
        catch ( IOException e ) {
            throw new UserException("Error writing to " + destination.getAbsolutePath(), e);
        }
    }

    /**
     * Private helper method for {@link #convertHeaderlessHadoopBamShardToBam} that takes a SAMFileHeader and writes it
     * to the provided `OutputStream`, correctly encoded for the BAM format and preceded by the BAM magic bytes.
     *
     * @param samFileHeader SAM header to write
     * @param outputStream stream to write the SAM header to
     */
    private static void writeBAMHeaderToStream( final SAMFileHeader samFileHeader, final OutputStream outputStream ) {
        final BlockCompressedOutputStream blockCompressedOutputStream = new BlockCompressedOutputStream(outputStream, null);
        final BinaryCodec outputBinaryCodec = new BinaryCodec(new DataOutputStream(blockCompressedOutputStream));

        final String headerString;
        final Writer stringWriter = new StringWriter();
        new SAMTextHeaderCodec().encode(stringWriter, samFileHeader, true);
        headerString = stringWriter.toString();

        outputBinaryCodec.writeBytes(ReadUtils.BAM_MAGIC);

        // calculate and write the length of the SAM file header text and the header text
        outputBinaryCodec.writeString(headerString, true, false);

        // write the sequences binarily.  This is redundant with the text header
        outputBinaryCodec.writeInt(samFileHeader.getSequenceDictionary().size());
        for (final SAMSequenceRecord sequenceRecord: samFileHeader.getSequenceDictionary().getSequences()) {
            outputBinaryCodec.writeString(sequenceRecord.getSequenceName(), true, true);
            outputBinaryCodec.writeInt(sequenceRecord.getSequenceLength());
        }

        try {
            blockCompressedOutputStream.flush();
        } catch (final IOException ioe) {
            throw new RuntimeIOException(ioe);
        }
    }

    /**
     * Determine if the <code>targetPath</code> exists.
     * @param ctx JavaSparkContext
     * @param targetPath the <code>org.apache.hadoop.fs.Path</code> object to check
     * @return true if the targetPath exists, otherwise false
     */
    public static boolean pathExists(final JavaSparkContext ctx, final Path targetPath) {
        Utils.nonNull(ctx);
        Utils.nonNull(targetPath);
        try {
            final FileSystem fs = targetPath.getFileSystem(ctx.hadoopConfiguration());
            return fs.exists(targetPath);
        } catch (IOException e) {
            throw new UserException("Error validating existence of path " + targetPath + ": " + e.getMessage());
        }
    }

    /**
     * Create an RDD of {@link Shard} from an RDD of coordinate sorted {@link Locatable} <i>without using a shuffle</i>.
     * Each shard contains the {@link Locatable} objects that overlap it.
     * @param ctx the Spark Context
     * @param locatables the RDD of {@link Locatable}, must be coordinate sorted
     * @param locatableClass the class of the {@link Locatable} objects in the RDD
     * @param sequenceDictionary the sequence dictionary to use to find contig lengths
     * @param intervals the {@link ShardBoundary} objects to create shards for, must be coordinate sorted
     * @param <L> the {@link Locatable} type
     * @return an RDD of {@link Shard} of overlapping {@link Locatable} objects
     */
    public static <L extends Locatable> JavaRDD<Shard<L>> shard(JavaSparkContext ctx, JavaRDD<L> locatables, Class<L> locatableClass,
                                                                SAMSequenceDictionary sequenceDictionary, List<ShardBoundary> intervals) {
        return shard(ctx, locatables, locatableClass, sequenceDictionary, intervals, false);
    }

    /**
     * Create an RDD of {@link Shard} from an RDD of coordinate sorted {@link Locatable}, optionally using a shuffle.
     * A shuffle is typically only needed for correctness testing, since it usually has a significant performance impact.
     * @param ctx the Spark Context
     * @param locatables the RDD of {@link Locatable}, must be coordinate sorted
     * @param locatableClass the class of the {@link Locatable} objects in the RDD
     * @param sequenceDictionary the sequence dictionary to use to find contig lengths
     * @param intervals the {@link ShardBoundary} objects to create shards for, must be coordinate sorted
     * @param useShuffle whether to use a shuffle or not
     * @param <L> the {@link Locatable} type
     * @return an RDD of {@link Shard} of overlapping {@link Locatable} objects
     */
    public static <L extends Locatable> JavaRDD<Shard<L>> shard(JavaSparkContext ctx, JavaRDD<L> locatables, Class<L> locatableClass,
                                                                SAMSequenceDictionary sequenceDictionary, List<ShardBoundary> intervals, boolean useShuffle) {

        class ShardBoundaryShard implements Shard<L> {
            private final ShardBoundary shardBoundary;
            private final Iterable<L> locatables;
            ShardBoundaryShard(ShardBoundary shardBoundary, Iterable<L> locatables) {
                this.shardBoundary = shardBoundary;
                this.locatables = locatables;
            }
            @Override
            public SimpleInterval getInterval() {
                return shardBoundary.getInterval();
            }

            @Override
            public SimpleInterval getPaddedInterval() {
                return shardBoundary.getPaddedInterval();
            }

            @Override
            public Iterator<L> iterator() {
                return locatables.iterator();
            }
        }
        if (useShuffle) {
            OverlapDetector<ShardBoundary> overlapDetector = OverlapDetector.create(intervals);
            Broadcast<OverlapDetector<ShardBoundary>> overlapDetectorBroadcast = ctx.broadcast(overlapDetector);
            JavaPairRDD<ShardBoundary, L> intervalsToReads = locatables.flatMapToPair(locatable -> {
                Set<ShardBoundary> overlaps = overlapDetectorBroadcast.getValue().getOverlaps(locatable);
                return overlaps.stream().map(key -> new Tuple2<>(key, locatable)).collect(Collectors.toList());
            });
            JavaPairRDD<ShardBoundary, Iterable<L>> grouped = intervalsToReads.groupByKey();
            return grouped.map((org.apache.spark.api.java.function.Function<Tuple2<ShardBoundary, Iterable<L>>, Shard<L>>) value -> new ShardBoundaryShard(value._1(), value._2()));
        }
        return joinOverlapping(ctx, locatables, locatableClass, sequenceDictionary, intervals, new MapFunction<Tuple2<ShardBoundary, Iterable<L>>, Shard<L>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Shard<L> call(Tuple2<ShardBoundary, Iterable<L>> value) throws Exception {
                return new ShardBoundaryShard(value._1(), value._2());
            }
        });
    }

    /**
     * Join an RDD of reads with a set of intervals, and apply a function to process the reads that overlap each interval.
     * @param ctx the Spark Context
     * @param reads the reads RDD, must be coordinate sorted
     * @param readClass the class of the reads, must be a subclass of {@link Locatable}
     * @param sequenceDictionary the sequence dictionary to use to find contig lengths
     * @param intervals the collection of intervals to apply the function to
     * @param f the function to process intervals and overlapping reads with
     * @param <R> the read type
     * @param <I> the interval type
     * @param <T> the return type of <code>f</code>
     * @return
     */
    private static <R extends Locatable, I extends Locatable, T> JavaRDD<T> joinOverlapping(JavaSparkContext ctx, JavaRDD<R> reads, Class<R> readClass,
                                                                                           SAMSequenceDictionary sequenceDictionary, List<I> intervals,
                                                                                           MapFunction<Tuple2<I, Iterable<R>>, T> f) {
        return joinOverlapping(ctx, reads, readClass, sequenceDictionary, intervals,
                (FlatMapFunction2<Iterator<R>, Iterator<I>, T>) (readsIterator, shardsIterator) -> () -> Iterators.transform(readsPerShard(readsIterator, shardsIterator, sequenceDictionary), new Function<Tuple2<I,Iterable<R>>, T>() {
                    @Nullable
                    @Override
                    public T apply(@Nullable Tuple2<I, Iterable<R>> input) {
                        try {
                            return f.call(input);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
    }

    /**
     * Join an RDD of reads with a set of intervals, and apply a function to process the reads that overlap each interval.
     * This differs from {@link #joinOverlapping(JavaSparkContext, JavaRDD, Class, SAMSequenceDictionary, List, MapFunction)}
     * in that the function to apply is given two iterators: one over intervals, and one over reads (for the partition),
     * and it is up to the function implemention to find overlaps between intervals and reads.
     * @param ctx the Spark Context
     * @param reads the reads RDD, must be coordinate sorted
     * @param readClass the class of the reads, must be a subclass of {@link Locatable}
     * @param sequenceDictionary the sequence dictionary to use to find contig lengths
     * @param intervals the collection of intervals to apply the function to
     * @param f the function to process intervals and overlapping reads with
     * @param <R> the read type
     * @param <I> the interval type
     * @param <T> the return type of <code>f</code>
     * @return
     */
    private static <R extends Locatable, I extends Locatable, T> JavaRDD<T> joinOverlapping(JavaSparkContext ctx, JavaRDD<R> reads, Class<R> readClass,
                                                                                           SAMSequenceDictionary sequenceDictionary, List<I> intervals,
                                                                                           FlatMapFunction2<Iterator<R>, Iterator<I>, T> f) {

        List<PartitionLocatable<SimpleInterval>> partitionReadExtents = computePartitionReadExtents(reads, sequenceDictionary);

        // For each interval find which partition it starts and ends in.
        // An interval is processed in the partition it starts in. However, we need to make sure that
        // subsequent partitions are coalesced if needed, so for each partition p find the latest subsequent
        // partition that is needed to read all of the intervals that start in p.
        List<Integer> maxEndPartitionIndexes = new ArrayList<>();
        for (int i = 0; i < reads.getNumPartitions(); i++) {
            maxEndPartitionIndexes.add(i);
        }
        OverlapDetector<PartitionLocatable<SimpleInterval>> overlapDetector = OverlapDetector.create(partitionReadExtents);
        List<PartitionLocatable<I>> indexedIntervals = new ArrayList<>();
        for (I interval : intervals) {
            int[] partitionIndexes = overlapDetector.getOverlaps(interval).stream()
                    .mapToInt(PartitionLocatable::getPartitionIndex).toArray();
            if (partitionIndexes.length == 0) {
                // interval does not overlap any partition - skip it
                continue;
            }
            Arrays.sort(partitionIndexes);
            int startIndex = partitionIndexes[0];
            int endIndex = partitionIndexes[partitionIndexes.length - 1];
            indexedIntervals.add(new PartitionLocatable<I>(startIndex, interval));
            if (endIndex > maxEndPartitionIndexes.get(startIndex)) {
                maxEndPartitionIndexes.set(startIndex, endIndex);
            }
        }

        JavaRDD<R> coalescedRdd = coalesce(reads, readClass, new RangePartitionCoalescer(maxEndPartitionIndexes));

        // Create an RDD of intervals with the same number of partitions as the reads, and where each interval
        // is in its start partition.
        JavaRDD<I> intervalsRdd = ctx.parallelize(indexedIntervals)
                .mapToPair(interval ->
                        new Tuple2<>(interval.getPartitionIndex(), interval.getLocatable()))
                .partitionBy(new KeyPartitioner(reads.getNumPartitions())).values();

        // zipPartitions on coalesced read partitions and intervals, and apply the function f
        return coalescedRdd.zipPartitions(intervalsRdd, f);
    }

    /**
     * Turn a pair of iterators over intervals and reads, into a single iterator over pairs made up of an interval and
     * the reads that overlap it. Intervals with no overlapping reads are dropped.
     */
    static <R extends Locatable, I extends Locatable> Iterator<Tuple2<I, Iterable<R>>> readsPerShard(Iterator<R> reads, Iterator<I> shards, SAMSequenceDictionary sequenceDictionary) {
        if (!shards.hasNext()) {
            return Collections.emptyIterator();
        }
        PeekingIterator<R> peekingReads = Iterators.peekingIterator(reads);
        PeekingIterator<I> peekingShards = Iterators.peekingIterator(shards);
        Iterator<Tuple2<I, Iterable<R>>> iterator = new AbstractIterator<Tuple2<I, Iterable<R>>>() {
            // keep track of current and next, since reads can overlap two shards
            I currentShard = peekingShards.next();
            I nextShard = peekingShards.hasNext() ? peekingShards.next() : null;
            List<R> currentReads = Lists.newArrayList();
            List<R> nextReads = Lists.newArrayList();

            @Override
            protected Tuple2<I, Iterable<R>> computeNext() {
                if (currentShard == null) {
                    return endOfData();
                }
                while (peekingReads.hasNext()) {
                    if (toRightOf(currentShard, peekingReads.peek(), sequenceDictionary)) {
                        break;
                    }
                    R read = peekingReads.next();
                    if (overlaps(currentShard, read)) {
                        currentReads.add(read);
                    }
                    if (nextShard != null && overlaps(nextShard, read)) {
                        nextReads.add(read);
                    }
                }
                // current shard is finished, either because the current read is to the right of it, or there are no more reads
                Tuple2<I, Iterable<R>> tuple = new Tuple2<>(currentShard, currentReads);
                currentShard = nextShard;
                nextShard = peekingShards.hasNext() ? peekingShards.next() : null;
                currentReads = nextReads;
                nextReads = Lists.newArrayList();
                return tuple;
            }
        };
        return Iterators.filter(iterator, input -> input._2().iterator().hasNext());
    }

    /**
     * @return <code>true</code> if the read is to the right of the given interval
     */
    private static <I extends Locatable, R extends Locatable> boolean toRightOf(I interval, R read, SAMSequenceDictionary sequenceDictionary) {
        int intervalContigIndex = sequenceDictionary.getSequenceIndex(interval.getContig());
        int readContigIndex = sequenceDictionary.getSequenceIndex(read.getContig());
        return (intervalContigIndex == readContigIndex && interval.getEnd() < read.getStart()) // read on same contig, to the right
                || intervalContigIndex < readContigIndex; // read on subsequent contig
    }

    /**
     * For each partition, find the interval that spans it.
     */
    static <R extends Locatable> List<PartitionLocatable<SimpleInterval>> computePartitionReadExtents(JavaRDD<R> reads, SAMSequenceDictionary sequenceDictionary) {
        // Find the first read in each partition. This is very efficient since only the first record in each partition is read.
        List<R> splitPoints = reads.mapPartitions((FlatMapFunction<Iterator<R>, R>) it -> ImmutableList.of(it.next())).collect();
        List<PartitionLocatable<SimpleInterval>> extents = new ArrayList<>();
        for (int i = 0; i < splitPoints.size(); i++) {
            Locatable current = splitPoints.get(i);
            int intervalContigIndex = sequenceDictionary.getSequenceIndex(current.getContig());
            final Locatable next;
            final int nextContigIndex;
            if (i < splitPoints.size() - 1) {
                next = splitPoints.get(i + 1);
                nextContigIndex = sequenceDictionary.getSequenceIndex(next.getContig());
            } else {
                next = null;
                nextContigIndex = sequenceDictionary.getSequences().size();
            }
            if (intervalContigIndex == nextContigIndex) { // same contig
                addPartitionReadExtent(extents, i, current.getContig(), current.getStart(), next.getEnd()); // assumes reads are same size
            } else {
                // complete current contig
                int contigEnd = sequenceDictionary.getSequence(current.getContig()).getSequenceLength();
                addPartitionReadExtent(extents, i, current.getContig(), current.getStart(), contigEnd);
                // add any whole contigs up to next (exclusive)
                for (int contigIndex = intervalContigIndex + 1; contigIndex < nextContigIndex; contigIndex++) {
                    SAMSequenceRecord sequence = sequenceDictionary.getSequence(contigIndex);
                    addPartitionReadExtent(extents, i, sequence.getSequenceName(), 1, sequence.getSequenceLength());
                }
                // add start of next contig
                if (next != null) {
                    addPartitionReadExtent(extents, i, next.getContig(), 1, next.getEnd()); // assumes reads are same size
                }
            }
        }
        return extents;
    }

    private static void addPartitionReadExtent(List<PartitionLocatable<SimpleInterval>> extents, int partitionIndex, String contig, int start, int end) {
        SimpleInterval extent = new SimpleInterval(contig, start, end);
        extents.add(new PartitionLocatable<>(partitionIndex, extent));
    }

    private static <T> JavaRDD<T> coalesce(JavaRDD<T> rdd, Class<T> cls, PartitionCoalescer partitionCoalescer) {
        RDD<T> coalescedRdd = new CoalescedRDD<>(rdd.rdd(), rdd.getNumPartitions(), partitionCoalescer, cls);
        ClassTag<T> tag = ClassTag$.MODULE$.apply(cls);
        return new JavaRDD<>(coalescedRdd, tag);
    }

    private static class KeyPartitioner extends Partitioner {

        private static final long serialVersionUID = 1L;

        private int numPartitions;

        public KeyPartitioner(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            return (Integer) key;
        }

    }

    static class PartitionLocatable<L extends Locatable> implements Locatable {
        private static final long serialVersionUID = 1L;

        private final int partitionIndex;
        private final L interval;


        public PartitionLocatable(int partitionIndex, L interval) {
            this.partitionIndex = partitionIndex;
            this.interval = interval;
        }

        public int getPartitionIndex() {
            return partitionIndex;
        }

        public L getLocatable() {
            return interval;
        }

        @Override
        public String getContig() {
            return interval.getContig();
        }

        @Override
        public int getStart() {
            return interval.getStart();
        }

        @Override
        public int getEnd() {
            return interval.getEnd();
        }

        @Override
        public String toString() {
            return "PartitionLocatable{" +
                    "partitionIndex=" + partitionIndex +
                    ", interval='" + interval + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PartitionLocatable<?> that = (PartitionLocatable<?>) o;

            if (partitionIndex != that.partitionIndex) return false;
            return interval.equals(that.interval);

        }

        @Override
        public int hashCode() {
            int result = partitionIndex;
            result = 31 * result + interval.hashCode();
            return result;
        }
    }
}
