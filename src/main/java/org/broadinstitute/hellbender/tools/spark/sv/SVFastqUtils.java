package org.broadinstitute.hellbender.tools.spark.sv;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMUtils;
import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.SequenceUtil;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.fermi.FermiLiteAssembler;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Memory-economical utilities for producing a FASTQ file.
 */
public class SVFastqUtils {

    public static final class Mapping implements Locatable {

        private static final Pattern MAPPING_DESCRIPTION_PATTERN = Pattern.compile("^mapping=(.+):(\\d+);(\\+|\\-);(.+)$");

        private final String contig;

        private final int start;

        private final boolean forwardStrand;

        private final Cigar cigar;

        public Mapping(final GATKRead read) {
            Utils.nonNull(read);
            if (read.isUnmapped()) {
                contig = null;
                start = -1;
                forwardStrand = true;
                cigar = null;
            } else {
                contig = read.getContig();
                start = read.getStart();
                forwardStrand = !read.isReverseStrand();
                cigar = read.getCigar();
            }
        }

        public Mapping(final String mappingDescription) {
            Utils.nonNull(mappingDescription);
            if (mappingDescription.equals("mapping=unmapped")) {
                contig = null;
                start = -1;
                forwardStrand = true;
                cigar = null;
            } else {
                final Matcher matcher = MAPPING_DESCRIPTION_PATTERN.matcher(mappingDescription);
                if (!matcher.find()) {
                    throw new IllegalArgumentException("invalid mapping description: '" + mappingDescription + "'");
                } else {
                    contig = matcher.group(1);
                    start = Integer.parseInt(matcher.group(2));
                    cigar = TextCigarCodec.decode(matcher.group(4));
                    forwardStrand = matcher.group(3).equals("+");
                }
            }
        }

        public boolean isMapped() {
            return contig != null;
        }

        @Override
        public String getContig() {
            return contig;
        }

        @Override
        public int getStart() {
            return start;
        }

        @Override
        public int getEnd() {
            return start;
        }

        public Cigar getCigar() {
            return cigar;
        }

        public boolean isForwardStrand() {
            return forwardStrand;
        }

        public String toString() {
            if (isMapped()) {
                return "mapping=" + String.join(";", getContig() + ":" + getStart(), forwardStrand ? "+" : "-", cigar.toString());
            } else {
                return "mapping=unmapped";
            }
        }
    }

    @DefaultSerializer(FastqRead.Serializer.class)
    public static final class FastqRead implements FermiLiteAssembler.BasesAndQuals {
        private final String name;
        private final OptionalInt fragmentNumber;
        private final byte[] bases;
        private final byte[] quals;
        private final Optional<Mapping> mapping;

        public FastqRead( final GATKRead read ) {
            this(Utils.nonNull(read).getName(),
                 read.isPaired() ? OptionalInt.of(read.isFirstOfPair() ? 1 : 2) : OptionalInt.empty(),
                 read.isReverseStrand() ? BaseUtils.simpleReverseComplement(read.getBases()) : read.getBases(),
                 reverseIf(read.isReverseStrand(), read.getBaseQualities()), new Mapping(read));

        }

        private static byte[] reverseIf(final boolean reverse, final byte[] bytes) {
            if (reverse) {
                SequenceUtil.reverseQualities(bytes);
            }
            return bytes;
        }

        public FastqRead( final String name, final OptionalInt fragmentNumber, final byte[] bases, final byte[] quals, final Mapping mapping) {
            this.name = Utils.nonNull(name);
            this.fragmentNumber = Utils.nonNull(fragmentNumber);
            this.bases = Utils.nonNull(bases);
            this.quals = Utils.nonNull(quals);
            this.mapping = mapping == null ? Optional.empty() : Optional.of(mapping);
        }

        public FastqRead( final String name, final OptionalInt fragmentNumber, final byte[] bases, final byte[] quals ) {
            this(name, fragmentNumber, bases, quals, null);
        }

        public FastqRead(final String name, final byte[] bases, final byte[] quals) {
            this(name, OptionalInt.empty(), bases, quals);
        }

        private FastqRead( final Kryo kryo, final Input input ) {
            name = input.readString();
            final String fragmentNumberString = input.readString();
            fragmentNumber = fragmentNumberString.isEmpty() ? OptionalInt.empty() : OptionalInt.of(Integer.parseInt(fragmentNumberString));
            final int nBases = input.readInt();
            bases = new byte[nBases];
            input.readBytes(bases);
            quals = new byte[nBases];
            input.readBytes(quals);
            final String mappingString = input.readString();
            if (!mappingString.isEmpty()) {
                mapping = Optional.of(new Mapping(mappingString));
            } else {
                mapping = Optional.empty();
            }
        }

        /**
         * Returns the header line of this Fastq read starting with '@' followed by the read id and description separated by tab characters.
         * @return never {@code null}.
         */
        public String getHeader() {
            final String description = getDescription();
            if (description.isEmpty()) {
                return "@" + getId();
            } else {
                return String.format("@%s\t%s", getId(), description);
            }
        }

        public String getId() { return fragmentNumber.isPresent() ? String.join("/", name, "" + fragmentNumber.getAsInt()) : name; }
        public String getName() { return name; }
        public String getDescription() { return mapping.isPresent() ? mapping.get().toString() : ""; }

        @Override public byte[] getBases() { return bases; }
        @Override public byte[] getQuals() { return quals; }

        private void serialize( final Kryo kryo, final Output output ) {
            output.writeAscii(name);
            output.writeAscii(fragmentNumber.isPresent() ? "" + fragmentNumber.getAsInt() : "");
            output.writeInt(bases.length);
            output.writeBytes(bases);
            output.writeBytes(quals);
            output.writeAscii(mapping.isPresent() ? mapping.get().toString() : "");
        }


        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<FastqRead> {
            @Override
            public void write( final Kryo kryo, final Output output, final FastqRead read ) {
                read.serialize(kryo, output);
            }

            @Override
            public FastqRead read( final Kryo kryo, final Input input, final Class<FastqRead> type ) {
                return new FastqRead(kryo, input);
            }
        }
    }

    public static List<FastqRead> readFastqFile( final String fileName ) {
        final int INITIAL_CAPACITY = 10000; // absolute guess, just something not too crazy small
        final List<FastqRead> reads = new ArrayList<>(INITIAL_CAPACITY);
        try ( final BufferedReader reader = new BufferedReader(new InputStreamReader(BucketUtils.openFile(fileName))) ) {
            String seqIdLine;
            int lineNo = 0;
            while ( (seqIdLine = reader.readLine()) != null ) {
                lineNo += 1;
                if ( seqIdLine.length() < 1 || seqIdLine.charAt(0) != '@' ) {
                    throw new GATKException("In FASTQ file "+fileName+" sequence identifier line does not start with @ on line "+lineNo);
                }
                final String callLine = reader.readLine();
                lineNo += 1;
                if ( callLine == null ) {
                    throw new GATKException("In FASTQ file "+fileName+" file truncated: missing calls.");
                }
                final String sepLine = reader.readLine();
                lineNo += 1;
                if ( sepLine == null ) {
                    throw new GATKException("In FASTQ file "+fileName+" file truncated: missing + line.");
                }
                if ( sepLine.length() < 1 || sepLine.charAt(0) != '+' ) {
                    throw new GATKException("In FASTQ file " + fileName + " separator line does not start with + on line " + lineNo);
                }
                final String qualLine = reader.readLine();
                lineNo += 1;
                if ( qualLine == null ) {
                    throw new GATKException("In FASTQ file "+fileName+" file truncated: missing quals.");
                }
                if ( callLine.length() != qualLine.length() ) {
                    throw new GATKException("In FASTQ file "+fileName+" there are "+qualLine.length()+
                            " quality scores on line "+lineNo+" but there are "+callLine.length()+" base calls.");
                }
                final byte[] quals = qualLine.getBytes();
                SAMUtils.fastqToPhred(quals);
                reads.add(new FastqRead(seqIdLine.substring(1), callLine.getBytes(), quals));
            }
        }
        catch ( final IOException ioe ) {
            throw new GATKException("Can't read "+fileName, ioe);
        }
        return reads;
    }

    /** Convert a read's name into a FASTQ record sequence ID */
    public static String readToFastqSeqId( final GATKRead read, final boolean includeMappingLocation ) {
        final String nameSuffix = read.isPaired() ? (read.isFirstOfPair() ? "/1" : "/2") : "";
        final String mapLoc;
        if ( includeMappingLocation ) {
            final Mapping mapping = new Mapping(read);
            mapLoc = mapping.toString();
        } else {
            mapLoc = "";
        }
        return read.getName() + nameSuffix + mapLoc;
    }

    /** Write a list of FASTQ records into a file. */
    public static void writeFastqFile(
            final String fileName,
            final Iterator<FastqRead> fastqReadItr ) {
        try ( final OutputStream writer =
                      new BufferedOutputStream(BucketUtils.createFile(fileName)) ) {
            writeFastqStream(writer, fastqReadItr);
        } catch ( final IOException ioe ) {
            throw new GATKException("Can't write "+fileName, ioe);
        }
    }

    public static void writeFastqStream( final OutputStream writer, final Iterator<FastqRead> fastqReadItr )
        throws IOException {
        int index = 0;
        while ( fastqReadItr.hasNext() ) {
            final FastqRead read = fastqReadItr.next();
            final String header = read.getHeader();
            if ( header == null ) writer.write(Integer.toString(++index).getBytes());
            else writer.write(header.getBytes());
            writer.write('\n');
            writer.write(read.getBases());
            writer.write('\n');
            writer.write('+');
            writer.write('\n');
            final byte[] quals = read.getQuals();
            final int nQuals = quals.length;
            final byte[] fastqQuals = new byte[nQuals];
            for ( int idx = 0; idx != nQuals; ++idx ) {
                fastqQuals[idx] = (byte)SAMUtils.phredToFastq(quals[idx]);
            }
            writer.write(fastqQuals);
            writer.write('\n');
        }
    }
}
