package org.broadinstitute.hellbender.tools.walkers.variantutils;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import htsjdk.variant.variantcontext.VariantContextUtils;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.*;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.VariantProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadsContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.engine.VariantWalker;
import org.broadinstitute.hellbender.exceptions.UserException;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by haasb on 9/25/15.
 */
@CommandLineProgramProperties(
        summary = "Prunes a VCF file from unwanted annotations.",
        oneLineSummary = "Prunes VCF annotations",
        programGroup = VariantProgramGroup.class
)
public final class PruneVCF extends VariantWalker {
    @Argument(fullName = "annotationsToKeep", shortName = "an2k", doc = "which annotations to keep", optional = true)
    List<String> annots = new ArrayList<>();

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, doc="Write output to this file", optional=false)
    public File OUTPUT;

    private VariantContextWriter writer;

    private void initializeVcfWriter() {
        writer = new VariantContextWriterBuilder()
                .setOutputFile(OUTPUT)
                .setOutputFileType(VariantContextWriterBuilder.OutputType.VCF)
                .unsetOption(Options.INDEX_ON_THE_FLY)
                .build();

        final Set<VCFHeaderLine> hInfo = new HashSet<>();
        hInfo.addAll(getHeaderForVariants().getMetaDataInInputOrder());
        writer.writeHeader(new VCFHeader(hInfo, getHeaderForVariants().getGenotypeSamples()));
    }

    @Override
    public void onTraversalStart() {
        initializeVcfWriter();
    }

    @Override
    public void apply(final VariantContext vc, final ReadsContext readsContext, final ReferenceContext ref, final FeatureContext featureContext) {
        List<String> keyList = new ArrayList<>();
        keyList.addAll(vc.getCommonInfo().getAttributes().keySet());
        VariantContextBuilder vcBuilder = new VariantContextBuilder(vc).rmAttributes(keyList);
        for (String annot : annots) {
            if (vc.hasAttribute(annot)) {
                vcBuilder.attribute(annot, vc.getAttribute(annot));
            }
        }
        writer.add(vcBuilder.make());
    }

    @Override
    public Object onTraversalDone() {
        writer.close();
        return null;
    }

}
