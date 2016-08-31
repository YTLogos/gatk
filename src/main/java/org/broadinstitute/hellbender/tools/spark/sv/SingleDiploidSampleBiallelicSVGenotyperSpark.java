package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;

/**
 * TODO: when we move to genotyping multiple types of sv, this should be moved to an engine class instead of base tool class.
 * TODO: right now we are dealing with single sample only, so we don't need a {@link org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypingData} alike,
 *       for the purpose of storing a {@link org.broadinstitute.hellbender.tools.walkers.genotyper.PloidyModel}
 *       and getting the appropriate {@link org.broadinstitute.hellbender.utils.genotyper.LikelihoodMatrix} for the sample.
 *       it may make sense to refactor {@link ReadLikelihoods} for a more explicit sample-stratified structure
 *
 * Most of this class is spent on loading variants and reads. The exact filtering is handled by the appropriate
 * {@link SVJunction} class for concrete SV types.
 */
@CommandLineProgramProperties(
        summary = "Genotype called structural variants from WGS data.",
        oneLineSummary = "Genotype called structural variants from WGS data",
        programGroup = StructuralVariationSparkProgramGroup.class
)
public final class SingleDiploidSampleBiallelicSVGenotyperSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc       = "An URI to the directory where all called inversion breakends are stored as text files. " +
            "The stored file format should conform to the one specified in {@link ContigAligner.AssembledBreakpoint#toString()}",
            shortName = StandardArgumentDefinitions.VARIANT_SHORT_NAME,
            fullName  = StandardArgumentDefinitions.VARIANT_LONG_NAME,
            optional  = false)
    private String VCFFile;

    @Argument(doc       = "An URI to the directory where all called inversion breakends are stored as text files. " +
            "The stored file format should conform to the one specified in {@link ContigAligner.AssembledBreakpoint#toString()}",
            shortName = "fastq",
            fullName  = "FASTQDir",
            optional  = false)
    private String pathToFASTQFiles;

    @Argument(doc       = "An URI to the directory where all called inversion breakends are stored as text files. " +
            "The stored file format should conform to the one specified in {@link ContigAligner.AssembledBreakpoint#toString()}",
            shortName = "assembly",
            fullName  = "assembledFASTADir",
            optional  = false)
    private String pathToAssembledFASTAFiles;

    @Argument(doc       = "An URI to the directory where all called inversion breakends are stored as text files. " +
            "The stored file format should conform to the one specified in {@link ContigAligner.AssembledBreakpoint#toString()}",
            shortName = "aln",
            fullName  = "contigAlignments",
            optional  = false)
    private String pathToContigAlignments;

    @Argument(doc       = "An URI to the directory where all called inversion breakends are stored as text files. " +
            "The stored file format should conform to the one specified in {@link ContigAligner.AssembledBreakpoint#toString()}",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = false)
    private String outFileName;


    @Argument(doc = "FASTA formatted reference", shortName = "fastaReference",
            fullName = "fastaReference", optional = false)
    private String fastaReference;

    @Override
    public final boolean requiresReference() {
        return true;
    }

    @Override
    public final boolean requiresReads() {
        return true;
    }

    // for developer performance debugging use
    static final boolean in_debug_state = true;
    private static final Logger logger = LogManager.getLogger(SingleDiploidSampleBiallelicSVGenotyperSpark.class);

    static final String testSampleName = "NA12878_PCR-_30X";
    static final int ploidy = 2;
    static final int alleleCount = 2;
    static final int readLength = 151;
    static final double expectedBaseErrorRate = 1.0/readLength; // blunt guess, will change
    // LikelihoodEngineArgumentCollection.phredScaledGlobalReadMismappingRate default value, used in HC
    static final double maximumLikelihoodDifferenceCap = 45;

    @Override
    public void runTool(final JavaSparkContext ctx){

        final JavaPairRDD<SVJunction, List<GATKRead>> junctionsAndReads = GenotyperInputParser.gatherVariantsAndEvidence(ctx, VCFFile, pathToAssembledFASTAFiles, pathToContigAlignments, pathToFASTQFiles, getReads(), getReference());

        final JavaPairRDD<SVJunction, GenotypeLikelihoods> genotypedJunctions = junctionsAndReads.mapToPair(GenotyperModule::genotype);
        junctionsAndReads.unpersist();

        GenotyperOutputWriter.writeGenotypedVariants(genotypedJunctions, outFileName, fastaReference, getAuthenticatedGCSOptions());
    }
}
