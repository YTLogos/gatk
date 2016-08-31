package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.genomics.dataflow.utils.GCSOptions;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * todo: to be merged with SVVCFWriter later
 */
class GenotyperOutputWriter implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String debugStringSavingOutputPath = "";


    private static final Logger logger = LogManager.getLogger(GenotyperOutputWriter.class);

    static void writeGenotypedVariants(final JavaPairRDD<SVJunction, GenotypeLikelihoods> genotypedJunctions,
                                       final String outputFileName, final String fastaReference,
                                       final PipelineOptions authenticationOptions) {
        // turn back to VC and save
        final SAMSequenceDictionary referenceSequenceDictionary = new ReferenceMultiSource(authenticationOptions, fastaReference, ReferenceWindowFunctions.IDENTITY_FUNCTION).getReferenceSequenceDictionary(null);
        final List<VariantContext> genotypedVCs = collectAndSortGenotypedVC(genotypedJunctions, referenceSequenceDictionary);

        try (final OutputStream outputStream = new BufferedOutputStream(BucketUtils.createFile(outputFileName, authenticationOptions))){
            writeGenotypedVariantsToVCF(genotypedVCs, referenceSequenceDictionary, outputStream);
            GenotyperModule.writeDebugReadLikelihoods(genotypedJunctions.coalesce(1).keys(), debugStringSavingOutputPath);
        } catch (final IOException ioex){
            throw new GATKException("Could not create output file", ioex);
        }
    }

    @VisibleForTesting
    static List<VariantContext> collectAndSortGenotypedVC(final JavaPairRDD<SVJunction, GenotypeLikelihoods> genotypedJunctions,
                                                          final SAMSequenceDictionary referenceSequenceDictionary){
        return Utils.stream(genotypedJunctions.map(pair -> pair._1().setPL( pair._2().getAsPLs()) ).collect())// set PL
                .map(SVJunction::getGenotypedVC) // add annotation and update VC
                .sorted((VariantContext v1, VariantContext v2) -> IntervalUtils.compareLocatables(v1, v2, referenceSequenceDictionary))
                .collect(Collectors.toList());
    }

    /**
     * TODO: should we emit reference confidence, we need more
     * Writes out new VCF FORMAT fields computed by this tool.
     */
    @VisibleForTesting
    static void writeGenotypedVariantsToVCF(final List<VariantContext> vcList,
                                            final SAMSequenceDictionary referenceSequenceDictionary,
                                            final OutputStream outputStream){

        try(final VariantContextWriter vcfWriter = new VariantContextWriterBuilder().clearOptions()
                .setOutputStream(outputStream)
                .setReferenceDictionary(referenceSequenceDictionary)
                .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
                .setOptions(Arrays.stream(new Options[]{}).collect(Collectors.toCollection(()-> EnumSet.noneOf(Options.class))))
                .setOption(Options.WRITE_FULL_FORMAT_FIELD)
                .build()){
            final VCFHeader header = new VCFHeader(GATKSVVCFHeaderLines.vcfHeaderLines.values().stream().collect(Collectors.toSet()), Collections.singletonList(SingleDiploidSampleBiallelicSVGenotyperSpark.testSampleName));
            header.setSequenceDictionary(referenceSequenceDictionary);
            header.addMetaDataLine( VCFStandardHeaderLines.getInfoLine(VCFConstants.END_KEY) );

            header.addMetaDataLine( VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_KEY) );
            header.addMetaDataLine( VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_QUALITY_KEY));
            header.addMetaDataLine( VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_PL_KEY) );

            vcfWriter.writeHeader(header);
            vcList.forEach(vc -> logger.debug(String.format(".........%s:%d-%d\t%s.........", vc.getContig(), vc.getStart(), vc.getEnd(), vc.getGenotype(0).getLikelihoodsString())));
            vcList.forEach(vcfWriter::add);
        }
    }
}
