package org.broadinstitute.hellbender.tools.spark.pipelines;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.util.IntervalList;
import htsjdk.samtools.util.Locatable;
import org.apache.commons.math3.stat.inference.AlternativeHypothesis;
import org.apache.commons.math3.stat.inference.BinomialTest;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.ShuffleJoinReadsWithRefBases;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.exome.HashedListTargetCollection;
import org.broadinstitute.hellbender.tools.exome.TargetCollection;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipList;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.AlignmentUtils;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;

@CommandLineProgramProperties(
        summary = "Gets alt/ref counts at SNP sites from the input BAM file",
        oneLineSummary = "Gets alt/ref counts at SNP sites from a BAM file",
        programGroup = SparkProgramGroup.class)
public final class GetHetPulldownSpark extends GATKSparkTool {
    protected static final String MODE_FULL_NAME = "mode";
    protected static final String MODE_SHORT_NAME = "m";

    protected static final String NORMAL_MODE_ARGUMENT = "normal";
    protected static final String TUMOR_MODE_ARGUMENT = "tumor";

    private static final long serialVersionUID = 1L;

    @Override
    public boolean requiresReads() { return true; }

    @Override
    public boolean requiresReference() { return true; }

    @Argument(
            doc = "Mode for filtering SNP sites (normal mode performs binomial test to check for heterozygosity).",
            fullName = MODE_FULL_NAME,
            shortName = MODE_SHORT_NAME,
            optional = false
    )
    protected String mode;

    @Argument(
            doc = "Interval-list file of common SNPs (normal-mode only).",
            fullName = "snp",
            shortName = "snp",
            optional = false
    )
    protected File snpFile;

    @Argument(
        doc = "Normal-mode heterozygous allele fraction.",
        fullName = "hetAlleleFraction",
        shortName = "hetAF",
        optional = false
    )
    protected static double hetAlleleFraction = 0.5;

    @Argument(
            doc = "Normal-mode p-value threshold for binomial test for heterozygous SNPs.",
            fullName = "pValue",
            shortName = "p",
            optional = false
    )
    protected static double pvalThreshold = 0.05;

    @Argument(doc = "uri for the output file: a local file path",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = true)
    protected String out;

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        if (!(mode.equals(NORMAL_MODE_ARGUMENT) || mode.equals(TUMOR_MODE_ARGUMENT))) {
            throw new UserException.BadArgumentValue(MODE_FULL_NAME, mode, "Mode must be normal or tumor.");
        }
        final TargetCollection<Locatable> snps = getSNPs();

//        final List<Locatable> isl = snps.targets();
        final Broadcast<IntervalsSkipList<Locatable>> islBroad = ctx.broadcast(new IntervalsSkipList<>(snps.targets()));

        final JavaRDD<GATKRead> rawReads = getReads();
        final JavaRDD<GATKRead> reads = rawReads.filter(read -> !read.isUnmapped() && read.getStart() <= read.getEnd() && !read.isDuplicate());
        final JavaPairRDD<GATKRead, ReferenceBases> readsWithRefBases = ShuffleJoinReadsWithRefBases.addBases(getReference(), reads);

        Function<Tuple2<Locatable, Tuple2<Integer, Integer>>, Boolean> filter;
        if (mode.equals(NORMAL_MODE_ARGUMENT)) {
            filter = keyValue -> isRefAltCountHetCompatible(keyValue._2());
        } else {
            filter = keyValue -> isOverCountThresholds(keyValue._2());
        }

        final Map<Locatable, Tuple2<Integer, Integer>> byKey = readsWithRefBases.flatMapToPair(readWithRefBases ->
                islBroad.getValue().getOverlapping(new SimpleInterval(readWithRefBases._1())).stream()
                        .map(over -> new Tuple2<>(over, getRefAltTuple2(readWithRefBases, over)))
                        .collect(Collectors.toList()))
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1() + t2._1(), t1._2() + t2._2()))
                .filter(filter)
                .collectAsMap();

        final SortedMap<Locatable, Tuple2<Integer, Integer>> byKeySorted = new TreeMap<>(IntervalUtils.LEXICOGRAPHICAL_ORDER_COMPARATOR);
        byKeySorted.putAll(byKey);

        print(byKeySorted, System.out);

        if (out != null){
            final File file = new File(out);
            try (final OutputStream outputStream = BucketUtils.createFile(file.getPath(), (PipelineOptions) null);
                final PrintStream ps = new PrintStream(outputStream)) {
                print(byKeySorted, ps);
            } catch (final IOException e){
                throw new UserException.CouldNotCreateOutputFile(file, e);
            }
        }
    }

    private Tuple2<Integer, Integer> getRefAltTuple2(final Tuple2<GATKRead, ReferenceBases> readWithRefBases, final Locatable snpSite) {
        final GATKRead read = readWithRefBases._1();
        final Locatable readInterval = new SimpleInterval(read.getContig(), read.getStart(), read.getEnd());
//        final ReadPileup pileup = new ReadPileup(readInterval, Collections.singletonList(read), 0);
//        final byte readBase = pileup.getBases()[0];
        final ReferenceBases referenceBases = readWithRefBases._2();
//        final byte refBase = referenceBases.getSubset(new SimpleInterval(snpSite)).getBases()[0];
        final byte refBase = referenceBases.getBases()[snpSite.getStart() - referenceBases.getInterval().getStart()];
//        System.out.println("SNP  interval: " + snpSite.toString());
//        System.out.println("Read interval: " + readInterval);
//        System.out.println("Read length  : " + read.getBases().length);
//        System.out.println("Ref  length  : " + referenceBases.getBases().length);
//        System.out.println("Read CIGAR   : " + read.getCigar().toString());
//        System.out.println("Read CIGAR c : " + CigarUtils.trimReadToUnclippedBases(read.getCigar()).toString());
        final byte[] readUnclippedBases = Arrays.copyOfRange(read.getBases(), read.getStart() - read.getUnclippedStart(), read.getLength() - (read.getUnclippedEnd() - read.getEnd()));
//        System.out.println("Read (full)  : " + read.getBasesString());
//        System.out.println("Read         : " + StringUtil.bytesToString(readUnclippedBases));
//        System.out.println("Ref          : " + StringUtil.bytesToString(referenceBases.getBases()));
        final int refStart = snpSite.getStart() - read.getStart();
        try {
            final byte readBase = AlignmentUtils.getBasesCoveringRefInterval(refStart, refStart, readUnclippedBases, 0, CigarUtils.trimReadToUnclippedBases(read.getCigar()))[0];
//        System.out.println("Read base    : " + StringUtil.byteToChar(readBase));
//        System.out.println("Ref  base    : " + StringUtil.byteToChar(refBase));
//        System.out.println();
            if (readBase == refBase) {
                return new Tuple2<>(1, 0);
            }
            return new Tuple2<>(0, 1);
        } catch (final NullPointerException e) {
            //deletions
//            System.out.println("SNP  interval: " + snpSite.toString());
//            System.out.println("Read interval: " + readInterval);
//            System.out.println("Read length  : " + read.getBases().length);
//            System.out.println("Ref  length  : " + referenceBases.getBases().length);
//            System.out.println("Read CIGAR   : " + read.getCigar().toString());
//            System.out.println("Read CIGAR c : " + CigarUtils.trimReadToUnclippedBases(read.getCigar()).toString());
//            System.out.println("Read (full)  : " + read.getBasesString());
//            System.out.println("Read         : " + StringUtil.bytesToString(readUnclippedBases));
//            System.out.println("Ref          : " + StringUtil.bytesToString(referenceBases.getBases()));
//            System.out.println("Ref  base    : " + StringUtil.byteToChar(refBase));
//            System.out.println();
            return new Tuple2<>(0, 0);
        }
    }

    private void print(final SortedMap<Locatable, Tuple2<Integer, Integer>> byKeySorted, final PrintStream ps) {
        ps.println("CONTIG" + "\t" + "POS" + "\t" + "REF" + "\t" + "ALT");
        for (final Locatable loc : byKeySorted.keySet()){
            ps.println(loc.getContig() + "\t" + loc.getStart() + "\t" + byKeySorted.get(loc)._1() + "\t" + byKeySorted.get(loc)._2());
        }
    }

    private TargetCollection<Locatable> getSNPs() {
        final IntervalList snps = IntervalList.fromFile(snpFile);
        //TODO need to split up adjacent SNPs
        //NOTE: java does not allow conversion of List<Interval> to List<Locatable>.
        //So we do this trick
        List<Locatable> splitSNPs = new ArrayList<>();
        snps.getIntervals().stream().map(GetHetPulldownSpark::splitSites).forEach(splitSNPs::addAll);
        return new HashedListTargetCollection<>(splitSNPs);
    }

    private static List<Locatable> splitSites(final Locatable loc) {
        final List<Locatable> sites = new ArrayList<>();
        for (int i = loc.getStart(); i <= loc.getEnd(); i++) {
            sites.add(new SimpleInterval(loc.getContig(), i, i));
        }
        return sites;
    }

    private static boolean isRefAltCountHetCompatible(final Tuple2<Integer, Integer> refAltCounts) {
        final int refCount = refAltCounts._1();
        final int altCount = refAltCounts._2();
        final int totalCount = refCount + altCount;
        final int majorReadCount = Math.max(refCount, altCount);
        final int minorReadCount = Math.min(refCount, altCount);

        if (majorReadCount == 0 || minorReadCount == 0 || totalCount <= 10) {
            return false;
        }

        final double pval = new BinomialTest().binomialTest(totalCount, majorReadCount, hetAlleleFraction,
                AlternativeHypothesis.TWO_SIDED);

        return pval >= pvalThreshold;
    }

    private static boolean isOverCountThresholds(final Tuple2<Integer, Integer> refAltCounts) {
        final int refCount = refAltCounts._1();
        final int altCount = refAltCounts._2();
        final int totalCount = refCount + altCount;

        return refCount > 0 && altCount > 0 && totalCount > 10;
    }
}

