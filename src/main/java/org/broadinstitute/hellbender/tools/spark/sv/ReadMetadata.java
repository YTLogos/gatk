package org.broadinstitute.hellbender.tools.spark.sv;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMSequenceRecord;
import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.utils.IntHistogram;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;

/**
 * A bag of data about reads:  contig name to id mapping, fragment length statistics by read group, mean length.
 * The fragment length statistics pertain to a library, but they're accessed by read group.  (I.e., in the
 * readGroupToFragmentStatistics map, all read groups that are derived from a given library point to the same set of
 * statistics.)
 */
@DefaultSerializer(ReadMetadata.Serializer.class)
public class ReadMetadata {
    private final Set<Integer> crossContigIgnoreSet;
    private final Map<String, Integer> contigNameToID;
    private final Map<String, String> readGroupToLibrary;
    private final long nReads;
    private final long maxReadsInPartition;
    private final int coverage;
    private final PartitionBounds[] partitionBounds;
    private final Map<String, IntHistogram.CDF> libraryToFragmentStatistics;
    private final Map<String, ZCalc> libraryToZCalc;
    private final static String NO_GROUP = "NoGroup";

    public ReadMetadata( final Set<Integer> crossContigIgnoreSet,
                         final SAMFileHeader header,
                         final int maxTrackedFragmentLength,
                         final JavaRDD<GATKRead> unfilteredReads,
                         final SVReadFilter filter ) {
        this.crossContigIgnoreSet = crossContigIgnoreSet;
        contigNameToID = buildContigNameToIDMap(header);
        readGroupToLibrary = buildGroupToLibMap(header);
        final Map<String, String> grpToLib = readGroupToLibrary;
        final List<PartitionStatistics> perPartitionStatistics =
                unfilteredReads
                    .mapPartitions(readItr ->
                        SVUtils.singletonIterator(
                                new PartitionStatistics(readItr, filter, maxTrackedFragmentLength, grpToLib)))
                    .collect();
        nReads = perPartitionStatistics.stream().mapToLong(PartitionStatistics::getNReads).sum();
        maxReadsInPartition = perPartitionStatistics.stream().mapToLong(PartitionStatistics::getNReads).max().orElse(0L);
        final long nReadBases = perPartitionStatistics.stream().mapToLong(PartitionStatistics::getNBases).sum();
        final long nRefBases = header.getSequenceDictionary().getSequences()
                .stream().mapToLong(SAMSequenceRecord::getSequenceLength).sum();
        coverage = (int)((nReadBases + nRefBases/2) / nRefBases); // rounding the coverage number
        final int nPartitions = perPartitionStatistics.size();
        partitionBounds = new PartitionBounds[nPartitions];
        for ( int idx = 0; idx != nPartitions; ++idx ) {
            final PartitionStatistics stats = perPartitionStatistics.get(idx);
            final Integer firstContigID = contigNameToID.get(stats.getFirstContig());
            final Integer lastContigID = contigNameToID.get(stats.getLastContig());
            partitionBounds[idx] = new PartitionBounds(
                    firstContigID==null ? PartitionBounds.UNMAPPED : firstContigID,
                    stats.getFirstLocation(),
                    lastContigID==null ? PartitionBounds.UNMAPPED : lastContigID,
                    stats.getLastLocation());
        }
        final Map<String, IntHistogram> combinedMaps =
                perPartitionStatistics.stream()
                    .map(PartitionStatistics::getLibraryToFragmentSizeHistogram)
                    .reduce(new HashMap<>(), ReadMetadata::combineMaps);
        libraryToFragmentStatistics = new HashMap<>(SVUtils.hashMapCapacity(combinedMaps.size()));
        for ( final Map.Entry<String, IntHistogram> entry : combinedMaps.entrySet() ) {
            libraryToFragmentStatistics.put(entry.getKey(), new IntHistogram.CDF(entry.getValue()));
        }
        libraryToZCalc = new HashMap<>(SVUtils.hashMapCapacity(libraryToFragmentStatistics.size()));
    }

    @VisibleForTesting
    ReadMetadata( final Set<Integer> crossContigIgnoreSet, final SAMFileHeader header,
                  final IntHistogram.CDF stats, final PartitionBounds[] partitionBounds,
                  final long nReads, final long maxReadsInPartition, final int coverage ) {
        this.crossContigIgnoreSet = crossContigIgnoreSet;
        contigNameToID = buildContigNameToIDMap(header);
        readGroupToLibrary = buildGroupToLibMap(header);
        this.nReads = nReads;
        this.maxReadsInPartition = maxReadsInPartition;
        this.coverage = coverage;
        this.partitionBounds = partitionBounds;
        libraryToFragmentStatistics = new HashMap<>(6);
        libraryToFragmentStatistics.put(null, stats);
        for ( final SAMReadGroupRecord readGroupRecord : header.getReadGroups() ) {
            libraryToFragmentStatistics.put(readGroupRecord.getLibrary(), stats);
        }
        libraryToZCalc = new HashMap<>(SVUtils.hashMapCapacity(libraryToFragmentStatistics.size()));
    }

    private ReadMetadata( final Kryo kryo, final Input input ) {
        final int crossContigIgnoreSetSize = input.readInt();
        this.crossContigIgnoreSet = new HashSet<>(SVUtils.hashMapCapacity(crossContigIgnoreSetSize));
        for ( int idx = 0; idx != crossContigIgnoreSetSize; ++idx ) {
            crossContigIgnoreSet.add(input.readInt());
        }

        final int groupMapSize = input.readInt();
        readGroupToLibrary = new HashMap<>(SVUtils.hashMapCapacity(groupMapSize));
        for ( int idx = 0; idx != groupMapSize; ++idx ) {
            final String groupName = input.readString();
            final String libName = input.readString();
            readGroupToLibrary.put(groupName, libName);
        }

        final int contigMapSize = input.readInt();
        contigNameToID = new HashMap<>(SVUtils.hashMapCapacity(contigMapSize));
        for ( int idx = 0; idx != contigMapSize; ++idx ) {
            final String contigName = input.readString();
            final int contigId = input.readInt();
            contigNameToID.put(contigName, contigId);
        }

        nReads = input.readLong();
        maxReadsInPartition = input.readLong();
        coverage = input.readInt();

        final int nPartitions = input.readInt();
        partitionBounds = new PartitionBounds[nPartitions];
        final PartitionBounds.Serializer boundsSerializer = new PartitionBounds.Serializer();
        for ( int idx = 0; idx != nPartitions; ++idx ) {
            partitionBounds[idx] = boundsSerializer.read(kryo, input, PartitionBounds.class);
        }

        final int libMapSize = input.readInt();
        final IntHistogram.CDF.Serializer cdfSerializer = new IntHistogram.CDF.Serializer();
        libraryToFragmentStatistics = new HashMap<>(SVUtils.hashMapCapacity(libMapSize));
        for ( int idx = 0; idx != libMapSize; ++idx ) {
            final String libraryName = input.readString();
            final IntHistogram.CDF cdf = cdfSerializer.read(kryo, input, IntHistogram.CDF.class);
            libraryToFragmentStatistics.put(libraryName, cdf);
        }
        libraryToZCalc = new HashMap<>(SVUtils.hashMapCapacity(libraryToFragmentStatistics.size()));
    }

    private void serialize( final Kryo kryo, final Output output ) {
        output.writeInt(crossContigIgnoreSet.size());
        for ( final Integer tigId : crossContigIgnoreSet ) {
            output.writeInt(tigId);
        }

        output.writeInt(readGroupToLibrary.size());
        for ( final Map.Entry<String, String> entry : readGroupToLibrary.entrySet() ) {
            output.writeString(entry.getKey());
            output.writeString(entry.getValue());
        }

        output.writeInt(contigNameToID.size());
        for ( final Map.Entry<String, Integer> entry : contigNameToID.entrySet() ) {
            output.writeString(entry.getKey());
            output.writeInt(entry.getValue());
        }

        output.writeLong(nReads);
        output.writeLong(maxReadsInPartition);
        output.writeInt(coverage);

        output.writeInt(partitionBounds.length);
        final PartitionBounds.Serializer boundsSerializer = new PartitionBounds.Serializer();
        for ( final PartitionBounds bounds : partitionBounds ) {
            boundsSerializer.write(kryo, output, bounds);
        }

        output.writeInt(libraryToFragmentStatistics.size());
        final IntHistogram.CDF.Serializer cdfSerializer = new IntHistogram.CDF.Serializer();
        for ( final Map.Entry<String, IntHistogram.CDF> entry : libraryToFragmentStatistics.entrySet() ) {
            output.writeString(entry.getKey());
            cdfSerializer.write(kryo, output, entry.getValue());
        }
    }

    public boolean ignoreCrossContigID( final int contigID ) { return crossContigIgnoreSet.contains(contigID); }
    @VisibleForTesting Set<Integer> getCrossContigIgnoreSet() { return crossContigIgnoreSet; }

    public Map<String, Integer> getContigNameMap() {
        return Collections.unmodifiableMap(contigNameToID);
    }

    public int getContigID( final String contigName ) {
        final Integer result = contigNameToID.get(contigName);
        if ( result == null ) {
            throw new GATKException("No such contig name: " + contigName);
        }
        return result;
    }

    public String getLibraryName( final String readGroupName ) {
        if ( readGroupName == null ) return null;
        if ( !readGroupToLibrary.containsKey(readGroupName) ) {
            throw new GATKException("No such read group in header: "+readGroupName);
        }
        return readGroupToLibrary.get(readGroupName);
    }

    @VisibleForTesting Map<String, String> getReadGroupToLibraryMap() { return readGroupToLibrary; }

    public static final class ZCalc {
        private final int median;
        private final float negativeMAD;
        private final float positiveMAD;

        public ZCalc( final int median, final int negativeMAD, final int positiveMAD ) {
            this.median = median;
            this.negativeMAD = negativeMAD;
            this.positiveMAD = positiveMAD;
        }

        public int getMedian() { return median; }
        public float getNegativeMAD() { return negativeMAD; }
        public float getPositiveMAD() { return positiveMAD; }

        public float getZishScore( final int fragmentSize ) {
            if ( fragmentSize < 0 ) {
                throw new GATKException("negative fragment size");
            }
            final int diff = fragmentSize - median;
            if ( diff == 0 ) return 0.0f;
            if ( diff > 0 ) return 1.0f * diff / positiveMAD;
            return 1.0f * diff / negativeMAD;
        }
    }

    public float getZishScore( final String readGroup, final int fragmentSize ) {
        return getZCalc(readGroup).getZishScore(fragmentSize);
    }

    public int getGroupMedianFragmentSize( final String readGroup ) {
        return getZCalc(readGroup).getMedian();
    }

    @VisibleForTesting ZCalc getZCalc( final String readGroup ) {
        final String libraryName = getLibraryName(readGroup);
        return libraryToZCalc.computeIfAbsent(libraryName,
                libName -> {
                    final IntHistogram.CDF cdf = getLibraryStatistics(libName);
                    final int median = cdf.median();
                    return new ZCalc(median, cdf.leftMedianDeviation(median), cdf.rightMedianDeviation(median));
                });
    }

    public long getNReads() { return nReads; }
    public int getNPartitions() { return partitionBounds.length; }
    public PartitionBounds getPartitionBounds( final int partitionIdx ) { return partitionBounds[partitionIdx]; }
    @VisibleForTesting PartitionBounds[] getAllPartitionBounds() { return partitionBounds; }

    public long getMaxReadsInPartition() { return maxReadsInPartition; }

    public int getCoverage() {
        return coverage;
    }

    public Map<String, IntHistogram.CDF> getAllLibraryStatistics() { return libraryToFragmentStatistics; }

    public IntHistogram.CDF getLibraryStatistics( final String libraryName ) {
        final IntHistogram.CDF stats = libraryToFragmentStatistics.get(libraryName);
        if ( stats == null ) {
            throw new GATKException("No such library: " + libraryName);
        }
        return stats;
    }

    public int getMaxMedianFragmentSize() {
        return libraryToFragmentStatistics.entrySet().stream()
                .mapToInt(entry -> entry.getValue().median())
                .max()
                .orElse(0);
    }

    private static Map<String, IntHistogram> combineMaps( final Map<String, IntHistogram> accumulator,
                                                    final Map<String, IntHistogram> element ) {
        for ( final Map.Entry<String, IntHistogram> entry : element.entrySet() ) {
            final String libraryName = entry.getKey();
            final IntHistogram accumCounts = accumulator.get(libraryName);
            if ( accumCounts == null ) {
                accumulator.put(libraryName, entry.getValue());
            } else {
                accumCounts.addObservations(entry.getValue());
            }
        }
        return accumulator;
    }

    public static Map<String, Integer> buildContigNameToIDMap( final SAMFileHeader header ) {
        final List<SAMSequenceRecord> contigs = header.getSequenceDictionary().getSequences();
        final Map<String, Integer> contigNameToID = new HashMap<>(SVUtils.hashMapCapacity(contigs.size()));
        final int nContigs = contigs.size();
        for ( int contigID = 0; contigID < nContigs; ++contigID ) {
            contigNameToID.put(contigs.get(contigID).getSequenceName(), contigID);
        }
        return contigNameToID;
    }

    public static Map<String, String> buildGroupToLibMap( final SAMFileHeader header ) {
        final List<SAMReadGroupRecord> readGroups = header.getReadGroups();
        final int mapCapacity = SVUtils.hashMapCapacity(header.getReadGroups().size());
        final Map<String, String> readGroupToLibraryMap = new HashMap<>(mapCapacity);
        for ( final SAMReadGroupRecord groupRecord : readGroups ) {
            readGroupToLibraryMap.put(groupRecord.getId(), groupRecord.getLibrary());
        }
        return readGroupToLibraryMap;
    }

    public static void writeMetadata( final ReadMetadata readMetadata,
                                      final String filename ) {
        try ( final Writer writer =
                      new BufferedWriter(new OutputStreamWriter(BucketUtils.createFile(filename))) ) {
            writer.write("#reads:\t" + readMetadata.getNReads() + "\n");
            writer.write("#partitions:\t" + readMetadata.getNPartitions() + "\n");
            writer.write("max reads/partition:\t" + readMetadata.getMaxReadsInPartition() + "\n");
            writer.write("coverage:\t" + readMetadata.getCoverage() + "\n");
            for ( final Map.Entry<String, IntHistogram.CDF> entry : readMetadata.getAllLibraryStatistics().entrySet() ) {
                final IntHistogram.CDF stats = entry.getValue();
                String name = entry.getKey();
                if ( name == null ) {
                    name = NO_GROUP;
                }
                final int median = stats.median();
                writer.write("library " + name + ":\t" + median +
                        "-" + stats.leftMedianDeviation(median) +
                        "+" + stats.rightMedianDeviation(median) + "\n");
            }
        } catch ( final IOException ioe ) {
            throw new GATKException("Can't write metadata file.", ioe);
        }
    }

    public static final class Serializer extends com.esotericsoftware.kryo.Serializer<ReadMetadata> {
        @Override
        public void write( final Kryo kryo, final Output output, final ReadMetadata readMetadata ) {
            readMetadata.serialize(kryo, output);
        }

        @Override
        public ReadMetadata read( final Kryo kryo, final Input input, final Class<ReadMetadata> klass ) {
            return new ReadMetadata(kryo, input);
        }
    }

    @DefaultSerializer(PartitionStatistics.Serializer.class)
    public static final class PartitionStatistics {
        private final Map<String, IntHistogram> libraryToFragmentSizeHistogram;
        private final long nReads;
        private final long nBases;
        private final String firstContig;
        private final int firstLocation;
        private final String lastContig;
        private final int lastLocation;

        public PartitionStatistics( final Iterator<GATKRead> unfilteredReadItr,
                                    final SVReadFilter filter,
                                    final int maxTrackedFragmentLength,
                                    final Map<String, String> readGroupToLibraryMap ) {
            libraryToFragmentSizeHistogram = new HashMap<>();
            GATKRead mappedRead = null;
            while ( unfilteredReadItr.hasNext() ) {
                final GATKRead read = unfilteredReadItr.next();
                if ( filter.isMapped(read) ) {
                    mappedRead = read;
                    break;
                }
            }
            if ( mappedRead == null ) {
                nReads = nBases = 0;
                firstContig = lastContig = null;
                firstLocation = lastLocation = -1;
                return;
            }

            GATKRead lastMappedRead;
            firstContig = mappedRead.getContig();
            firstLocation = mappedRead.getUnclippedStart();
            long reads = 0L;
            long bases = 0L;
            do {
                reads += 1L;
                bases += mappedRead.getLength();
                if ( filter.isNonDiscordantEvidence(mappedRead) ) {
                    // getReadGroup can return null -- that's OK.  library will be null -- that's OK, too.
                    final String library = readGroupToLibraryMap.get(mappedRead.getReadGroup());
                    libraryToFragmentSizeHistogram
                            .computeIfAbsent(library, key -> new IntHistogram(maxTrackedFragmentLength))
                            .addObservation(Math.abs(mappedRead.getFragmentLength()));
                }
                lastMappedRead = mappedRead;
                mappedRead = null;
                while ( unfilteredReadItr.hasNext() ) {
                    final GATKRead read = unfilteredReadItr.next();
                    if ( filter.isMapped(read) ) {
                        mappedRead = read;
                        break;
                    }
                }
            } while ( mappedRead != null );

            lastContig = lastMappedRead.getContig();
            lastLocation = lastMappedRead.getUnclippedEnd() + 1;
            nReads = reads;
            nBases = bases;
        }

        private PartitionStatistics( final Kryo kryo, final Input input ) {
            int nEntries = input.readInt();
            libraryToFragmentSizeHistogram = new HashMap<>(SVUtils.hashMapCapacity(nEntries));
            final IntHistogram.Serializer histogramSerializer = new IntHistogram.Serializer();
            while ( nEntries-- > 0 ) {
                final String libName = input.readString();
                final IntHistogram fragmentSizeHistogram = histogramSerializer.read(kryo, input, IntHistogram.class);
                libraryToFragmentSizeHistogram.put(libName, fragmentSizeHistogram);
            }
            nReads = input.readLong();
            nBases = input.readLong();
            firstContig = input.readString();
            firstLocation = input.readInt();
            lastContig = input.readString();
            lastLocation = input.readInt();
        }

        public long getNReads() {
            return nReads;
        }

        public long getNBases() {
            return nBases;
        }

        public Map<String, IntHistogram> getLibraryToFragmentSizeHistogram() {
            return libraryToFragmentSizeHistogram;
        }

        public String getFirstContig() { return firstContig; }
        public int getFirstLocation() { return firstLocation; }
        public String getLastContig() { return lastContig; }
        public int getLastLocation() { return lastLocation; }

        private void serialize( final Kryo kryo, final Output output ) {
            final IntHistogram.Serializer histogramSerializer = new IntHistogram.Serializer();
            output.writeInt(libraryToFragmentSizeHistogram.size());
            for ( final Map.Entry<String, IntHistogram> entry : libraryToFragmentSizeHistogram.entrySet() ) {
                output.writeString(entry.getKey());
                histogramSerializer.write(kryo, output, entry.getValue());
            }
            output.writeLong(nReads);
            output.writeLong(nBases);
            output.writeString(firstContig);
            output.writeInt(firstLocation);
            output.writeString(lastContig);
            output.writeInt(lastLocation);
        }

        public static final class Serializer
                extends com.esotericsoftware.kryo.Serializer<PartitionStatistics> {
            @Override
            public void write( final Kryo kryo, final Output output,
                               final PartitionStatistics partitionStatistics ) {
                partitionStatistics.serialize(kryo, output);
            }

            @Override
            public PartitionStatistics read( final Kryo kryo, final Input input,
                                             final Class<PartitionStatistics> klass ) {
                return new PartitionStatistics(kryo, input);
            }
        }
    }

    /** A class to track the genomic location of the start of the first and last mapped reads in a partition. */
    @DefaultSerializer(PartitionBounds.Serializer.class)
    public static final class PartitionBounds {
        private final int firstContigID;
        private final int firstStart;
        private final int lastContigID;
        private final int lastStart;
        public final static int UNMAPPED = -1;

        public PartitionBounds( final int firstContigID, final int firstStart,
                                final int lastContigID, final int lastStart ) {
            this.firstContigID = firstContigID;
            this.firstStart = firstStart;
            this.lastContigID = lastContigID;
            this.lastStart = lastStart;
        }

        private PartitionBounds( final Kryo kryo, final Input input ) {
            this.firstContigID = input.readInt();
            this.firstStart = input.readInt();
            this.lastContigID = input.readInt();
            this.lastStart = input.readInt();
        }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeInt(firstContigID);
            output.writeInt(firstStart);
            output.writeInt(lastContigID);
            output.writeInt(lastStart);
        }

        public int getFirstContigID() { return firstContigID; }
        public int getFirstStart() { return firstStart; }
        public int getLastContigID() { return lastContigID; }
        public int getLastStart() { return lastStart; }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<PartitionBounds> {
            @Override
            public void write( final Kryo kryo, final Output output, final PartitionBounds partitionBounds ) {
                partitionBounds.serialize(kryo, output);
            }

            @Override
            public PartitionBounds read( final Kryo kryo, final Input input, final Class<PartitionBounds> klass ) {
                return new PartitionBounds(kryo, input);
            }
        }
    }
}
