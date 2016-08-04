package org.broadinstitute.hellbender.tools.walkers.genotyper;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.AlleleList;
import org.broadinstitute.hellbender.utils.genotyper.AlleleListPermutation;
import org.broadinstitute.hellbender.utils.genotyper.LikelihoodMatrix;

import java.util.ArrayList;
import java.util.List;

/**
 * This class delegates genotyping to allele count- and ploidy-dependent {@link GenotypeLikelihoodCalculator}s
 * under the assumption that sample genotypes are independent conditional on their population frequencies.
 */
public final class IndependentSampleGenotypesModel {
    private static final int DEFAULT_CACHE_PLOIDY_CAPACITY = 10;
    private static final int DEFAULT_CACHE_ALLELE_CAPACITY = 50;

    private final int cacheAlleleCountCapacity;
    private final int cachePloidyCapacity;

    private final GenotypeLikelihoodCalculators calculatorManager;
    /**
     * A 2-D array of {@link GenotypeLikelihoodCalculator}'s, whose creation is managed by {@link #calculatorManager}.
     * Entries are lazily constructed, i.e. the 2-D array takes its shape up on this class' construction, but
     * entries are not constructed until specifically requested.
     */
    private GenotypeLikelihoodCalculator[][] likelihoodCalculators;

    public IndependentSampleGenotypesModel() { this(DEFAULT_CACHE_PLOIDY_CAPACITY, DEFAULT_CACHE_ALLELE_CAPACITY); }

    /**
     *  Initialize model with given maximum allele count and ploidy for caching
     */
    public IndependentSampleGenotypesModel(final int calculatorCachePloidyCapacity, final int calculatorCacheAlleleCapacity) {
        Utils.validateArg(calculatorCachePloidyCapacity >= 0, () -> "the ploidy provided cannot be negative: " + calculatorCachePloidyCapacity);
        Utils.validateArg(calculatorCacheAlleleCapacity >= 0, () -> "the maximum allele index provided cannot be negative: " + calculatorCacheAlleleCapacity);

        cacheAlleleCountCapacity = calculatorCacheAlleleCapacity;
        cachePloidyCapacity = calculatorCachePloidyCapacity;
        calculatorManager = new GenotypeLikelihoodCalculators();
        likelihoodCalculators = new GenotypeLikelihoodCalculator[calculatorCachePloidyCapacity][calculatorCacheAlleleCapacity];
    }

    /**
     * Given a list of alleles, ploidy model, and read likelihoods, compute allele likelihoods.
     *
     * @throws IllegalArgumentException when any of {@code genotypingAlleles} and {@code data} is {@code null},
     *                                  or {@code data.numberofSamples()} returns negative.
     */
    public <A extends Allele> GenotypingLikelihoods<A> calculateLikelihoods(final AlleleList<A> genotypingAlleles,
                                                                            final GenotypingData<A> data) {

        Utils.nonNull(genotypingAlleles, "the allele cannot be null");
        Utils.nonNull(data, "the genotyping data cannot be null");

        // prepare data, get information necessary
        final int                               alleleCount = genotypingAlleles.numberOfAlleles();
        final int                               sampleCount = data.numberOfSamples();
        final PloidyModel                       ploidyModel = data.ploidyModel();
        final AlleleListPermutation<A>          permutation = data.permutation(genotypingAlleles);
        final AlleleLikelihoodMatrixMapper<A>   alleleLikelihoodMatrixMapper = AlleleLikelihoodMatrixMapper.newInstance(permutation);

        Utils.validateArg(sampleCount >= 0, () -> "Sample count cannot be negative!");
        if(sampleCount==0) { return new GenotypingLikelihoods<>(genotypingAlleles, ploidyModel, new ArrayList<>()); }

        GenotypeLikelihoodCalculator likelihoodsCalculator = getLikelihoodsCalculator(ploidyModel.samplePloidy(0), alleleCount);

        // result container
        final List<GenotypeLikelihoods> genotypeLikelihoods = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {// walk over all samples, compute GL

            // get a new likelihoodsCalculator if this sample's ploidy differs from the previous sample's
            final int samplePloidy = ploidyModel.samplePloidy(i);
            if (samplePloidy != likelihoodsCalculator.ploidy()) {
                likelihoodsCalculator = getLikelihoodsCalculator(samplePloidy, alleleCount);
            }

            final LikelihoodMatrix<A> sampleLikelihoods = alleleLikelihoodMatrixMapper.apply(data.readLikelihoods().sampleMatrix(i));
            genotypeLikelihoods.add(likelihoodsCalculator.genotypeLikelihoods(sampleLikelihoods));
        }

        return new GenotypingLikelihoods<>(genotypingAlleles, ploidyModel, genotypeLikelihoods);
    }

    /**
     * If the requested sample ploidy or allele count exceeds capacity of cached calculatorManager, ask for a new, appropriate
     * one from the manager.
     * Otherwise 1) if the cached one has been constructed (not just place holder created), return the fully constructed one
     *           2) ask the manager to fully construct the place holder calculator, save it, then return it.
     * @param samplePloidy
     * @param alleleCount
     * @return
     */
    private GenotypeLikelihoodCalculator getLikelihoodsCalculator(final int samplePloidy, final int alleleCount) {
        if (samplePloidy >= cachePloidyCapacity || alleleCount >= cacheAlleleCountCapacity) {
            return calculatorManager.getInstance(samplePloidy, alleleCount);
        }
        final GenotypeLikelihoodCalculator result = likelihoodCalculators[samplePloidy][alleleCount];
        if (result != null) {
            return result;
        } else {
            final GenotypeLikelihoodCalculator newOne = calculatorManager.getInstance(samplePloidy, alleleCount);
            likelihoodCalculators[samplePloidy][alleleCount] = newOne;
            return newOne;
        }
    }
}