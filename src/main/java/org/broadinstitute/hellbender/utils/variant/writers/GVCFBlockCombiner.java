package org.broadinstitute.hellbender.utils.variant.writers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.iterators.PushPullTransformer;
import org.broadinstitute.hellbender.utils.variant.GATKVCFConstants;
import org.broadinstitute.hellbender.utils.variant.GATKVCFHeaderLines;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public final class GVCFBlockCombiner implements PushPullTransformer<VariantContext> {
    private final RangeMap<Integer, Range<Integer>> gqPartitions;
    private final int defaultPloidy;
    private final Queue<VariantContext> toOutput = new ArrayDeque<>();

    /**
     * fields updated on the fly during GVCFWriter operation
     */
    private int nextAvailableStart = -1;
    private String contigOfNextAvailableStart = null;
    private String sampleName = null;

    private HomRefBlock currentBlock = null;

    public GVCFBlockCombiner(List<Integer> gqPartitions, int defaultPloidy) {
        this.gqPartitions = parsePartitions(gqPartitions);
        this.defaultPloidy = defaultPloidy;
    }

    /**
     * Create {@link HomRefBlock}s which will collectively accept variants of any genotype quality
     * <p>
     * Each individual block covers a band of genotype qualities with the splits between bands occurring at values in {@code gqPartitions}.
     * There will be {@code gqPartitions.size() +1} bands produced covering the entire possible range of genotype qualities from 0 to {@link Integer#MAX_VALUE}.
     *
     * @param gqPartitions proposed GQ partitions
     * @return a list of HomRefBlocks accepting bands of genotypes qualities split at the points specified in gqPartitions
     */
    @VisibleForTesting
    static RangeMap<Integer, Range<Integer>> parsePartitions(final List<Integer> gqPartitions) {
        Utils.nonEmpty(gqPartitions);
        Utils.containsNoNull(gqPartitions, "gqPartitions contains a null Integer");
        final RangeMap<Integer, Range<Integer>> result = TreeRangeMap.create();
        int lastThreshold = 0;
        for (final Integer value : gqPartitions) {
            if (value < lastThreshold) {
                throw new IllegalArgumentException("gqPartitions is out of order.  Last is " + lastThreshold + " but next is " + value);
            } else if (value == lastThreshold) {
                throw new IllegalArgumentException("gqPartitions is equal elements: Last is " + lastThreshold + " but next is " + value);
            }
            result.put(Range.closedOpen(lastThreshold, value), Range.closedOpen(lastThreshold, value));
            lastThreshold = value;
        }
        result.put(Range.closedOpen(lastThreshold, Integer.MAX_VALUE), Range.closedOpen(lastThreshold, Integer.MAX_VALUE));

        return result;
    }

    void addRangesToHeader(VCFHeader header) {
        Utils.nonNull(header, "header cannot be null");

        header.addMetaDataLine(VCFStandardHeaderLines.getInfoLine(VCFConstants.END_KEY));
        header.addMetaDataLine(GATKVCFHeaderLines.getFormatLine(GATKVCFConstants.MIN_DP_FORMAT_KEY));

        for (final Range<Integer> partition : gqPartitions.asMapOfRanges().keySet()) {
            header.addMetaDataLine(rangeToVCFHeaderLine(partition));
        }
    }

    static VCFHeaderLine rangeToVCFHeaderLine(Range<Integer> genotypeQualityBand) {
        // Need to uniquify the key for the header line using the min/max GQ, since
        // VCFHeader does not allow lines with duplicate keys.
        final String key = String.format("GVCFBlock%d-%d", genotypeQualityBand.lowerEndpoint(), genotypeQualityBand.upperEndpoint());
        return new VCFHeaderLine(key, "minGQ=" + genotypeQualityBand.lowerEndpoint() + "(inclusive),maxGQ=" + genotypeQualityBand.upperEndpoint() + "(exclusive)");
    }

    /**
     * Add hom-ref site from vc to this gVCF hom-ref state tracking, emitting any pending states if appropriate
     *
     * @param vc a non-null VariantContext
     * @param g  a non-null genotype from VariantContext
     * @return a VariantContext to be emitted, or null if non is appropriate
     */
    private VariantContext addHomRefSite(final VariantContext vc, final Genotype g) {

        if (nextAvailableStart != -1) {
            // don't create blocks while the hom-ref site falls before nextAvailableStart (for deletions)
            if (vc.getStart() <= nextAvailableStart && vc.getContig().equals(contigOfNextAvailableStart)) {
                return null;
            }
            // otherwise, reset to non-relevant
            nextAvailableStart = -1;
            contigOfNextAvailableStart = null;
        }

        final VariantContext result;
        if (currentBlock != null && currentBlock.canMergeWith(g)) {
            currentBlock.add(vc.getStart(), g);
            result = null;
        } else {
            result = currentBlock != null ? currentBlock.toVariantContext(sampleName) : null;
            currentBlock = createNewBlock(vc, g);
        }
        return result;
    }

    /**
     * Helper function to create a new HomRefBlock from a variant context and current genotype
     *
     * @param vc the VariantContext at the site where want to start the band
     * @param g  the genotype of the sample from vc that should be used to initialize the block
     * @return a newly allocated and initialized block containing g already
     */
    private HomRefBlock createNewBlock(final VariantContext vc, final Genotype g) {
        // figure out the GQ limits to use based on the GQ of g
        final int gq = g.getGQ();
        final Range<Integer> partition = gqPartitions.get(gq);

        if( partition == null) {
            throw new GATKException("GQ " + g + " from " + vc + " didn't fit into any partition");
        }

        // create the block, add g to it, and return it for use
        final HomRefBlock block = new HomRefBlock(vc, partition.lowerEndpoint(), partition.upperEndpoint(), defaultPloidy);
        block.add(vc.getStart(), g);
        return block;
    }

    /**
     * Flush the current hom-ref block, if necessary, to the underlying writer, and reset the currentBlock to null
     */
    private void emitCurrentBlock() {
        if (currentBlock != null) {
            toOutput.add(currentBlock.toVariantContext(sampleName));
            this.currentBlock = null;
        }
    }

    @Override
    public void submit(VariantContext item) {
        Utils.nonNull(item);
        Utils.validateArg(item.hasGenotypes(), "GVCF assumes that the VariantContext has genotypes");
        Utils.validateArg(item.getGenotypes().size() == 1, () -> "GVCF assumes that the VariantContext has exactly one genotype but saw " + item.getGenotypes().size());

        if (sampleName == null) {
            sampleName = item.getGenotype(0).getSampleName();
        }

        if (currentBlock != null && !currentBlock.isContiguous(item)) {
            // we've made a non-contiguous step (across interval, onto another chr), so finalize
            emitCurrentBlock();
        }

        final Genotype g = item.getGenotype(0);
        if (g.isHomRef() && item.hasAlternateAllele(GATKVCFConstants.NON_REF_SYMBOLIC_ALLELE) && item.isBiallelic()) {
            // create bands
            final VariantContext maybeCompletedBand = addHomRefSite(item, g);
            if (maybeCompletedBand != null) {
                toOutput.add(maybeCompletedBand);
            }
        } else {
            // g is variant, so flush the bands and emit vc
            emitCurrentBlock();
            nextAvailableStart = item.getEnd();
            contigOfNextAvailableStart = item.getContig();
            toOutput.add(item);
        }
    }

    @Override
    public boolean hasFinalizedItems() {
        return !toOutput.isEmpty();
    }

    @Override
    public List<VariantContext> consumeFinalizedItems() {
        List<VariantContext> result = new ArrayList<>(toOutput);
        toOutput.clear();
        return result;
    }

    @Override
    public void signalEndOfInput() {
        emitCurrentBlock();
    }
}