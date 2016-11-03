package org.broadinstitute.hellbender.utils.variant.writers;

import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.utils.iterators.PushToPullIterator;

import java.util.*;

public class HomRefBlockMergingIterator extends PushToPullIterator<VariantContext> {

    public HomRefBlockMergingIterator(Iterator<VariantContext> variants, final List<Integer> gqPartitions, final int defaultPloidy){
       super(variants, new GVCFBlockCombiner(gqPartitions, defaultPloidy));
    }

}
