package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * todo lots of work is actually artifacts from development, in-memory process where multiple stages of operations are concatenated together would eliminate much of code here
 */
class GenotyperInputParser implements Serializable{
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LogManager.getLogger(SingleDiploidSampleBiallelicSVGenotyperSpark.class);


    static JavaPairRDD<SVJunction, List<GATKRead>> gatherVariantsAndEvidence(final JavaSparkContext ctx,
                                                                             final String inputVCF,
                                                                             final String pathToAssembledFASTAFiles,
                                                                             final String pathToContigAlignments,
                                                                             final String pathToFASTQFiles,
                                                                             final JavaRDD<GATKRead> rawBamReads,
                                                                             final ReferenceMultiSource reference) {
        // parse discovery VC calls
        final List<SVJunction> junctions = parseVCFInput(ctx, inputVCF, pathToAssembledFASTAFiles, pathToContigAlignments, reference);
        filterJunctions(junctions, makeJunctionFilter());

        // gather and filter reads that will be used in genotyping
        final JavaPairRDD<SVJunction, List<GATKRead>> genotypingReads = loadGenotypingReads(ctx, junctions, pathToFASTQFiles, rawBamReads).cache();
        final Set<SVJunction> sparkJunctions = genotypingReads.keys().collect().stream().collect(Collectors.toSet());
        if(Sets.symmetricDifference(sparkJunctions, junctions.stream().collect(Collectors.toSet())).size()!=0){
            throw new IllegalStateException("Junctions are different before and after read gathering!");
        }
        logger.info(".........DONE MERGING READS.........");
        return genotypingReads;
    }

    /**
     * Parse input and return a list of convenience struct for use by specific genotyping module.
     *
     * Depends on {@link SVJunction#convertToSVJunction(VariantContext, Broadcast, Broadcast)} for the actual mapping.
     *
     * Not tested because most all methods this depends on are tested elsewhere already.
     */
    static List<SVJunction> parseVCFInput(final JavaSparkContext ctx,
                                          final String vcfInput,
                                          final String localAssemblyResultPath,
                                          final String alignmentResultPath,
                                          final ReferenceMultiSource reference){

        final List<VariantContext> listOfVCs = new VariantsSparkSource(ctx).getParallelVariantContexts(vcfInput, null).collect();

        final JavaPairRDD<Long, ContigsCollection> asmId2Contigs = ContigsCollection.loadContigsCollectionKeyedByAssemblyId(ctx, localAssemblyResultPath)
                .mapToPair( p -> new Tuple2<>(Long.parseLong(p._1().replace(SVConstants.FASTQ_OUT_PREFIX, "")), p._2()));

        final JavaPairRDD<Long, Iterable<AlignmentRegion>> asmId2Alignments = ctx.textFile(alignmentResultPath)
                .map(ContigsCollection::parseAlignedAssembledContigLine)
                .mapToPair(ar -> new Tuple2<>(Long.parseLong(ar.assemblyId.replace(SVConstants.FASTQ_OUT_PREFIX, "")), ar))
                .groupByKey();

        final Map<Long, List<LocalAssemblyContig>> assemblyID2assembleContents = mapAssemblyID2ItsAlignments(asmId2Contigs, asmId2Alignments, gatherAsmIDAnnotations(listOfVCs));
        final Broadcast<Map<Long, List<LocalAssemblyContig>>> assemblyID2assembleContentsBroadcast = ctx.broadcast(assemblyID2assembleContents);

        final Broadcast<ReferenceMultiSource> referenceMultiSourceBroadcast = ctx.broadcast(reference);

        if(SingleDiploidSampleBiallelicSVGenotyperSpark.in_debug_state) logger.info(".........BEGIN CONSTRUCTING JUNCTIONS FROM VC AND ALIGNMENTS.........");
        final List<SVJunction> junctions = listOfVCs.stream().map(vc -> SVJunction.convertToSVJunction(vc, assemblyID2assembleContentsBroadcast, referenceMultiSourceBroadcast)).collect(Collectors.toList());
        if(SingleDiploidSampleBiallelicSVGenotyperSpark.in_debug_state) logger.info(String.format(".........CONSTRUCTED %d JUNCTIONS FROM VC AND ALIGNMENTS.........", junctions.size()));
        assemblyID2assembleContentsBroadcast.destroy();referenceMultiSourceBroadcast.destroy();

        return junctions;
    }

    /**
     * @return a mapping from assembly id to the result of locally assembled contigs aligned back to ref,
     *         given the path to re-alignment result.
     */
    @VisibleForTesting
    static Map<Long, List<LocalAssemblyContig>> mapAssemblyID2ItsAlignments(final JavaPairRDD<Long, ContigsCollection> asmId2Contigs,
                                                                            final JavaPairRDD<Long, Iterable<AlignmentRegion>> asmId2Alignments,
                                                                            final Set<Long> assembliesToKeep){
        return
                asmId2Alignments.join(asmId2Contigs)
                        .mapValues( pair -> {
                            final Map<String, ArrayList<AlignmentRegion>> contigId2Alignments = Utils.stream(pair._1).collect(Collectors.groupingBy(ar -> ar.contigId))
                                    .entrySet().stream().collect(Collectors.toMap( Map.Entry::getKey, p -> new ArrayList<>(p.getValue() )));
                            final List<LocalAssemblyContig> collection = pair._2.getContents() ;
                            collection.forEach(contig -> contig.setAlignmentRegions(contigId2Alignments.get(contig.contigID)));
                            return collection;
                        })
                        .filter(p -> assembliesToKeep.contains(p._1)).collectAsMap();
    }

    @VisibleForTesting
    static Set<Long> gatherAsmIDAnnotations(final List<VariantContext> vcs){
        return vcs.stream()
                .flatMap(vc -> Arrays.stream(vc.getAttributeAsString(GATKSVVCFHeaderLines.ASSEMBLY_IDS, "").replace("[", "").replace("]", "").split(", ")).map(Long::valueOf)  )
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static void filterJunctions(final List<SVJunction> junctions, final Predicate<SVJunction> predicate){
        final List<SVJunction> junctionsToBeFiltered = junctions.stream().filter(predicate).collect(Collectors.toList());
        if(junctionsToBeFiltered.size()!=0){
            logger.warn(".........FOLLOWING JUNCTIONS TO BE EXCLUDED IN GENOTYPING BECAUSE OF HIGH COVERAGE.........");
            junctionsToBeFiltered.stream().map(j -> j.getOriginalVC().getID()).forEach(logger::warn);
            junctions.removeAll(junctionsToBeFiltered);
            logger.warn(String.format(".........%d JUNCTIONS LEFT TO BE GENOTYPED.........", junctions.size()));
        }
    }

    static Predicate<SVJunction> makeJunctionFilter(){
        return new JunctionFilterBasedOnInterval();
    }
    @VisibleForTesting
    final static class JunctionFilterBasedOnInterval implements Predicate<SVJunction>{
        private final Set<SimpleInterval> defaultRegionsToExclude =  Stream.of(new SimpleInterval("2", 33141200, 33141700),
                new SimpleInterval("MT", 12867, 14977),
                new SimpleInterval("17", 41379304, 41401057),
                new SimpleInterval("17", 41381590, 41463575),
                new SimpleInterval("17", 41401193, 41466836)).collect(Collectors.toCollection(HashSet::new));

        private final Set<SimpleInterval> regionsToExclude;

        JunctionFilterBasedOnInterval(){ regionsToExclude = defaultRegionsToExclude; }

        JunctionFilterBasedOnInterval(final Set<SimpleInterval> regionsToExclude) { this.regionsToExclude = regionsToExclude;}

        public boolean test(final SVJunction j){
            final Tuple2<SimpleInterval, SimpleInterval> windows = j.getReferenceWindows();
            final SimpleInterval left = windows._1();
            final SimpleInterval right = windows._2();
            return regionsToExclude.stream().anyMatch(region -> region.overlaps(left) || region.overlaps(right));
        }
    }

    /**
     * Prepare reads that will be used for likelihood calculations.
     *
     * Reads are gathered from two resources:
     * 1) original FASTQ records pulled out by {@link FindBreakpointEvidenceSpark} that are associated with
     *      putative breakpoints, which in turn the appropriate {@link SVJunction} are associated with.
     * 2) original bam file.
     *
     * Exactly how reads are pulled in from the FASTQ and bam file is determined by
     * {@link SVJunction#readSuitableForGenotyping(GATKRead)}, whose exact behavior is controlled by concrete types of SV.
     *
     * For a junction, reads that are gathered from both sources (bam, and reconstructed from FASTQ), will be de-duplicated
     * such that the bam version will be kept.
     *
     * @param svJunctions           a list of sv junctions to be genotyped
     * @param pathToFASTQFiles      a string to the directory where the FASTQ files generated by {@link FindBreakpointEvidenceSpark} are stored
     */
    static JavaPairRDD<SVJunction, List<GATKRead>> loadGenotypingReads(final JavaSparkContext ctx,
                                                                       final List<SVJunction> svJunctions,
                                                                       final String pathToFASTQFiles,
                                                                       final JavaRDD<GATKRead> rawBamReads){

        // gather reads from FASTQ files collected in previous steps in pipeline
        final JavaPairRDD<Long, String> fastq = SVFastqUtils.loadFASTQFiles(ctx, pathToFASTQFiles)
                .mapToPair(pair -> new Tuple2<>(Long.parseLong(FilenameUtils.getBaseName(pair._1()).replace(SVConstants.FASTQ_OUT_PREFIX, "")), pair._2())); // essentially extract asm id from FASTQ file name

        final JavaPairRDD<Long, List<SVJunction>> asmid2SVJunctions = ctx.parallelizePairs(getAssemblyToJunctionsMapping(svJunctions).entrySet().stream().map(e -> new Tuple2<>(e.getKey(), e.getValue())).collect(Collectors.toList()));

        final JavaPairRDD<SVJunction, List<GATKRead>> fastqReads = associateJunctionWithFastqReads(fastq, asmid2SVJunctions);

        // gather reads from bam file (will overlap with FASTQ reads)
        if(SingleDiploidSampleBiallelicSVGenotyperSpark.in_debug_state) logger.info(".........BEGIN EXTRACTING BAM READS.........");
        final JavaPairRDD<SVJunction, List<GATKRead>> bamReads = associateJunctionWithBamReads(rawBamReads, svJunctions).cache();
        if(SingleDiploidSampleBiallelicSVGenotyperSpark.in_debug_state){
            bamReads.count(); logger.info(".........DONE EXTRACTING BAM READS.........");
        }

        // merge together the fastqReads and bamReads (deduplication to follow)
        if(SingleDiploidSampleBiallelicSVGenotyperSpark.in_debug_state) logger.info(".........BEGIN MERGING READS.........");
        final JavaPairRDD<SVJunction, Tuple2<List<GATKRead>, List<GATKRead>>> redundantReads = fastqReads.join(bamReads).cache();
        bamReads.unpersist();

        return redundantReads.mapValues(pair -> mergeReadsByName(pair._1, pair._2));
    }

    /**
     * Get FASTQ reads that were pulled out by {@link FindBreakpointEvidenceSpark}.
     * Reads are filtered by custom filters defined by the appropriate junctions.
     * @return Reads associated with a particular breakpoint.
     */
    @VisibleForTesting
    static JavaPairRDD<SVJunction, List<GATKRead>> associateJunctionWithFastqReads(final JavaPairRDD<Long, String> fastqContents,
                                                                                   final JavaPairRDD<Long, List<SVJunction>> asmid2SVJunctions){

        final Set<Long> interestingAsmIDs = new HashSet<>( asmid2SVJunctions.keys().collect() );

        // map assembly id (only those giving signal) to FASTQ contents then reconstruct read
        final JavaPairRDD<Long, List<GATKRead>> preFilteredReads = fastqContents.filter(pair -> interestingAsmIDs.contains(pair._1())) // filter out unused assemblies
                .mapValues(contents -> Utils.stream(SVFastqUtils.extractFASTQContents(contents)).map(SVFastqUtils::convertToRead).collect(Collectors.toList())); // FASTQ-> GATKRead

        return preFilteredReads.join(asmid2SVJunctions)
                .mapValues(pair -> {
                    final List<GATKRead> reads = pair._1();
                    return pair._2.stream().map(j -> new Tuple2<>(j, reads.stream().filter(j::readSuitableForGenotyping).collect(Collectors.toList())))
                            .collect(Collectors.toList());
                }) // (long, {(junction, {reads})}
                .values()
                .flatMapToPair(List::iterator) // (junction, {reads}) with duplicated keys
                .groupByKey()
                .mapValues(it -> Utils.stream(it).flatMap(List::stream).distinct().collect(Collectors.toList()) );
    }

    /**
     * Reverse the mapping of (SVJunction, its associated VC's asmId) to (asmId, associated SVJunction from VC detected by this assembly).
     * @param svJunctions
     * @return
     */
    @VisibleForTesting
    static Map<Long, List<SVJunction>> getAssemblyToJunctionsMapping(final List<SVJunction> svJunctions){
        // get mapping from assembly id to SVJunction's; see discussion on StackOverflow 38471056
        return svJunctions.stream()
                .flatMap(svJunction -> svJunction.getAssemblyIDs().stream().map(asid -> new AbstractMap.SimpleEntry<>(asid, svJunction)))
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
    }

    /**
     * Go back to the original bam and gather reads around region identified by {@link SVJunction}.
     *
     * Reads for each junction are filtered defined by the appropriate junctions.
     *
     * Note that read names are changed by appending "/1" or "/2" to their original names so later when bam reads are
     * merged with FASTQ reads extracted by {@link #associateJunctionWithFastqReads(JavaPairRDD, JavaPairRDD)},
     * duplicate reads can be removed by read names.
     *
     * Note also that the interface seems redundant because we want to test this method (so calling getReads() internally would make that impossible).
     */
    @VisibleForTesting
    static JavaPairRDD<SVJunction, List<GATKRead>> associateJunctionWithBamReads(final JavaRDD<GATKRead> reads,
                                                                                 final List<SVJunction> svJunctions){
        return reads
                .filter(read -> svJunctions.stream().anyMatch(junction -> junction.readSuitableForGenotyping(read))) // pre-filer: if suitable for any junction
                .flatMapToPair( read -> svJunctions.stream()
                        .filter(junction -> junction.readSuitableForGenotyping(read))
                        .map(junc -> new Tuple2<>(junc, read))
                        .collect(Collectors.toList()).iterator() )                                           // read -> { (junction, read) }
                .groupByKey()
                .mapValues( it -> {// bwa strips out the "/1" "/2" part, but we need these for merging with FASTQ reads
                    it.forEach(read -> read.setName(read.getName() + (read.isFirstOfPair() ? "/1" : "/2")));
                    return Utils.stream(it).collect(Collectors.toList());}); // turn iterable to list
    }

    @VisibleForTesting
    static List<GATKRead> mergeReadsByName(final List<GATKRead> fastqReads,
                                           final List<GATKRead> bamReads){
        // for reads appear in both, keep the one from bam
        final Set<String> common = Sets.intersection(fastqReads.stream().map(GATKRead::getName).collect(Collectors.toSet()),
                bamReads.stream().map(GATKRead::getName).collect(Collectors.toSet()));
        final List<GATKRead> toReturn = new ArrayList<>(bamReads);
        toReturn.addAll( fastqReads.stream().filter(r -> !common.contains(r.getName())).collect(Collectors.toList()) );
        return toReturn;
    }
}
