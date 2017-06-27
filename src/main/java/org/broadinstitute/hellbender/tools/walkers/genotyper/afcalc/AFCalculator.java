package org.broadinstitute.hellbender.tools.walkers.genotyper.afcalc;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Generic interface for calculating the probability of alleles segregating given priors and genotype likelihoods
 */
public interface AFCalculator {

    /**
     * Compute the probability of the alleles segregating given the genotype likelihoods of the samples in vc
     *
     * @param vc the VariantContext holding the alleles and sample information.  The VariantContext
     *           must have at least 1 alternative allele
     * @return result (for programming convenience)
     */
    AFCalculationResult getLog10PNonRef(final VariantContext vc, final int defaultPloidy, final int maximumAlternativeAlleles);
}
