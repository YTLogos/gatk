package org.broadinstitute.hellbender.utils.genotyper;


import com.google.common.annotations.VisibleForTesting;
import htsjdk.variant.variantcontext.Allele;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.downsampling.AlleleBiasedDownsamplingUtils;
import org.broadinstitute.hellbender.utils.pileup.PileupElement;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *   Wrapper class that holds a set of maps of the form (Read -> Map(Allele->Double))
 *   For each read, this holds underlying alleles represented by an aligned read, and corresponding relative likelihood.
 */
public final class PerReadAlleleLikelihoodMap {

    // -----------------------------------------------------------------------------------------------
    // Core structs
    // -----------------------------------------------------------------------------------------------

    /** A set of all of the allele, so we can efficiently determine if an allele is already present */
    private final Set<Allele> allelesSet;

    /** A list of the unique allele, as an ArrayList so we can call get(i) efficiently */
    private final List<Allele> alleles;

    private final Map<GATKRead,
                      Map<Allele, Double>> likelihoodReadMap;

    // -----------------------------------------------------------------------------------------------
    // Initializers and destructor-alike
    // -----------------------------------------------------------------------------------------------

    /**
     * Empty initializes core fields.
     */
    public PerReadAlleleLikelihoodMap() {
        allelesSet = new LinkedHashSet<>();
        alleles = new ArrayList<>();
        likelihoodReadMap = new LinkedHashMap<>();
    }

    /**
     * Removes all elements from the map.
     */
    public void clear() {
        allelesSet.clear();
        alleles.clear();
        likelihoodReadMap.clear();
    }

    // -----------------------------------------------------------------------------------------------
    // Modifiers (modifies fields) : add
    // -----------------------------------------------------------------------------------------------

    /**
     * Helper function to add the read underneath a pileup element to the map
     * @param p             Pileup element
     * @param a             Corresponding allele
     * @param likelihood    Allele likelihood
     *
     * @throws IllegalArgumentException if any of the inputs is {@code null}
     */
    public void add(final PileupElement p,
                    final Allele a,
                    final double likelihood) {
        Utils.nonNull(p, "Pileup element cannot be null");
        Utils.nonNull(p.getRead(), "Read underlying pileup element cannot be null");
        Utils.nonNull(a, "Allele for add() cannot be null");
        add(p.getRead(), a, likelihood);
    }

    /**
     * Add a new entry into the Read -> ( Allele -> Likelihood ) map of maps.
     * @param read          - the GATKRead that was evaluated
     * @param a             - the Allele against which the GATKRead was evaluated
     * @param likelihood    - the likelihood score resulting from the evaluation of {@code read} against {@code a}
     *
     * @throws IllegalArgumentException if any of the inputs is {@code null} or the log likelihood {@code likelihood} is positive
     */
    public void add(final GATKRead read,
                    final Allele a,
                    final Double likelihood) {
        Utils.nonNull( read , "Cannot add a null read to the allele likelihood map");
        Utils.nonNull( a, "Cannot add a null allele to the allele likelihood map");
        Utils.nonNull( likelihood, "Likelihood cannot be null");
        if ( likelihood > 0.0 ) {
            throw new IllegalArgumentException("Likelihood must be negative (L = log(p))");
        }

        if (!allelesSet.contains(a)) {
            allelesSet.add(a);
            alleles.add(a);
        }

        likelihoodReadMap.computeIfAbsent(read, r -> new LinkedHashMap<>()).put(a,likelihood);
    }

    // -----------------------------------------------------------------------------------------------
    // Modifiers (modifies fields) : remove
    // -----------------------------------------------------------------------------------------------

    /**
     * Remove reads from this map that are poorly modelled w.r.t. their per allele likelihoods
     *
     * Goes through each read in this map, and if it is poorly modelled removes it from the map.
     *
     * @see #readIsPoorlyModelled
     * for more information about the poorly modelled test.
     *
     * @param maxErrorRatePerBase see equivalent parameter in {@link #readIsPoorlyModelled(GATKRead, Collection, double)}
     *
     * @return the list of reads removed from this map because they are poorly modelled
     */
    public List<GATKRead> filterPoorlyModelledReads(final double maxErrorRatePerBase) {
        final List<GATKRead> removedReads = new LinkedList<>();
        final Iterator<Map.Entry<GATKRead, Map<Allele, Double>>> it = likelihoodReadMap.entrySet().iterator();
        while ( it.hasNext() ) {
            final Map.Entry<GATKRead, Map<Allele, Double>> record = it.next();
            if ( readIsPoorlyModelled(record.getKey(), record.getValue().values(), maxErrorRatePerBase) ) {
                removedReads.add(record.getKey());
                it.remove();
            }
        }

        return removedReads;
    }

    /**
     * A read is poorly modeled when it's likelihood is below what would be expected for a read
     * originating from one of the alleles given the {@code maxErrorRatePerBase} of the reads in general.
     *
     * This function makes a number of key assumptions:
     * First, that the likelihoods reflect the total likelihood of the read.
     * In other words, that the read would be fully explained by one of the alleles.
     * This indicates that the allele should be something like the full haplotype from which the read might originate.
     *
     * It further assumes that each error in the read occurs with likelihood of -3 (Q30 confidence per base).
     * So a read with a 10% error rate with Q30 bases that's 100 bp long we'd expect to see 10 real Q30 errors
     * even against the true haplotype.
     * So for this read to be well modelled by at least one allele we'd expect a likelihood to be >= 10 * -3.
     *
     * @param read                  the read we want to evaluate
     * @param log10Likelihoods      a list of the log10 likelihoods of the read against a set of haplotypes.
     * @param maxErrorRatePerBase   the maximum error rate we'd expect for this read per base, in real space.
     *                              So 0.01 means a 1% error rate
     *
     * @return true if none of the log10 likelihoods imply that the read truly originated from one of the haplotypes
     */
    @VisibleForTesting
    static boolean readIsPoorlyModelled(final GATKRead read,
                                        final Collection<Double> log10Likelihoods,
                                        final double maxErrorRatePerBase) {

        final double maxErrorsForRead = Math.min(2.0, Math.ceil(read.getLength() * maxErrorRatePerBase));
        final double log10QualPerBase = -4.0;
        final double log10MaxLikelihoodForTrueAllele = maxErrorsForRead * log10QualPerBase;

        return log10Likelihoods.stream().allMatch(lik -> lik < log10MaxLikelihoodForTrueAllele);
    }

    /**
     * For each allele "a" , identify those reads whose most likely allele is "a", and remove a "downsamplingFraction" proportion
     * of those reads from the "likelihoodReadMap".
     * This is used for e.g. sample contamination.
     *
     * @param downsamplingFraction - the fraction of supporting reads to remove from each allele.
     *                             If <=0 all reads kept, if >=1 all reads tossed.
     */
    public void performPerAlleleDownsampling(final double downsamplingFraction) {
        if ( downsamplingFraction <= 0.0 ) { //remove no reads
            return;
        }
        if ( downsamplingFraction >= 1.0 ) { //remove all reads
            likelihoodReadMap.clear();
            return;
        }

        // stratifying the reads by the alleles they represent at this position
        final Map<Allele, List<GATKRead>> alleleReadMap = getAlleleStratifiedReadMap();

        // compute the reads to remove and actually remove them
        final int totalReads= AlleleBiasedDownsamplingUtils.totalReads(alleleReadMap);
        final List<GATKRead> readsToRemove = AlleleBiasedDownsamplingUtils.selectAlleleBiasedReads(alleleReadMap, totalReads, downsamplingFraction);
        for ( final GATKRead read : readsToRemove ) {
            likelihoodReadMap.remove(read);
        }
    }

    // -----------------------------------------------------------------------------------------------
    // Accessors: non-trivial
    // -----------------------------------------------------------------------------------------------

    /**
     * Convert the {@link #likelihoodReadMap} to a map of alleles to reads, where each read is mapped uniquely to the allele
     * for which it has the greatest associated likelihood
     *
     * @return  a map from each allele to a list of reads that 'support' the allele,
     *          i.e. the allele is an informative {@link MostLikelyAllele} for those reads
     */
    @VisibleForTesting
    Map<Allele,List<GATKRead>> getAlleleStratifiedReadMap() {
        final Map<Allele, List<GATKRead>> alleleReadMap = alleles.stream().collect(
                Collectors.toMap(Function.identity(),
                        allele -> new ArrayList<>()));

        for ( final Map.Entry<GATKRead, Map<Allele, Double>> entry : likelihoodReadMap.entrySet() ) {
            final MostLikelyAllele bestAllele = getMostLikelyAllele(entry.getValue());
            if ( bestAllele.isInformative() ) {
                alleleReadMap.get(bestAllele.getMostLikelyAllele()).add(entry.getKey());
            }
        }

        return alleleReadMap;
    }

    /**
     * Get the most likely alleles estimated across all reads in this object
     *
     * Takes the most likely two alleles according to their diploid genotype likelihoods.
     * That is, for each allele i and j we compute p(D | i,j) where D is the read likelihoods.
     * We track the maximum i,j likelihood and return an object that contains the alleles i and j as well as the max likelihood.
     *
     * Note that the second most likely diploid genotype is not tracked so the resulting MostLikelyAllele
     * doesn't have a meaningful get best likelihood.
     *
     * @return a MostLikelyAllele object, or null if this map is empty
     */
    public MostLikelyAllele getMostLikelyDiploidAlleles() {
        if ( isEmpty() ) {
            return null;
        }

        Allele bestAllele1= null;
        Allele bestAllele2= null;
        double maxElement = Double.NEGATIVE_INFINITY;
        for( int i = 0; i < alleles.size(); i++ ) {
            final Allele allele_i = alleles.get(i);
            for( int j = 0; j <= i; j++ ) {
                final Allele allele_j = alleles.get(j);

                double haplotypeLikelihood = 0.0;
                for( final Map.Entry<GATKRead, Map<Allele,Double>> entry : likelihoodReadMap.entrySet() ) {
                    // Compute log10(10^x1/2 + 10^x2/2) = log10(10^x1+10^x2)-log10(2)
                    final double likelihood_i = entry.getValue().get(allele_i);
                    final double likelihood_j = entry.getValue().get(allele_j);
                    haplotypeLikelihood += MathUtils.approximateLog10SumLog10(likelihood_i, likelihood_j) + MathUtils.LOG10_ONE_HALF;

                    // fast exit.  If this diploid pair is already worse than the max, just stop and look at the next pair
                    if ( haplotypeLikelihood < maxElement ) {
                        break;
                    }
                }

                // keep track of the max element and associated alleles
                if ( haplotypeLikelihood > maxElement ) {
                    bestAllele1 = allele_i;
                    bestAllele2 = allele_j;
                    maxElement = haplotypeLikelihood;
                }
            }
        }

        if ( maxElement == Double.NEGATIVE_INFINITY ) {
            throw new IllegalStateException("max likelihood is " + maxElement + " indicating something has gone wrong");
        }
        if (bestAllele1 == null || bestAllele2 == null){
            throw new IllegalStateException("best alleles are null, indicating something has gone wrong");
        }

        return new MostLikelyAllele(bestAllele1, bestAllele2, maxElement, maxElement);
    }

    /**
     * Given a map from alleles to likelihoods, find and return the allele with the largest likelihood.
     *
     * @throws IllegalArgumentException when the input map is {@code null}
     */
    public static MostLikelyAllele getMostLikelyAllele( final Map<Allele,Double> alleleMap ) {
        return getMostLikelyAllele(alleleMap, null);
    }

    /**
     * Given a map from alleles to likelihoods, and a set of alleles to look into
     * find and return the allele with the largest likelihood.
     *
     * @param alleleMap - a map from alleles to likelihoods
     * @param onlyConsiderTheseAlleles if not null, we will only consider alleles in this set for being one of the best.
     *                                 this is useful for the case where you've selected a subset of the alleles that
     *                                 the reads have been computed for further analysis.  If null totally ignored
     *
     * @return - a MostLikelyAllele object
     *
     * @throws IllegalArgumentException when the map input is {@code null}
     */
    public static MostLikelyAllele getMostLikelyAllele( final Map<Allele,Double> alleleMap,
                                                        final Set<Allele> onlyConsiderTheseAlleles ) {
        Utils.nonNull(alleleMap, "The allele to likelihood map cannot be null");
        final List<Map.Entry<Allele, Double>> revSortedEntries = alleleMap.entrySet().stream()
                .filter(e1 -> (onlyConsiderTheseAlleles == null) || onlyConsiderTheseAlleles.contains(e1.getKey()))
                .sorted((e1, e2) -> -Double.compare(e1.getValue(), e2.getValue()))   //Note: reverse sort
                .collect(Collectors.toList());

        if (revSortedEntries.isEmpty()){
            return MostLikelyAllele.NO_CALL;
        }
        if (revSortedEntries.size() == 1){
            return new MostLikelyAllele(revSortedEntries.get(0).getKey(),
                                        revSortedEntries.get(0).getValue());
        }
        return new MostLikelyAllele(revSortedEntries.get(0).getKey(),
                                    revSortedEntries.get(1).getKey(),
                                    revSortedEntries.get(0).getValue(),
                                    revSortedEntries.get(1).getValue());
    }

    // -----------------------------------------------------------------------------------------------
    // Trivial accessors
    // -----------------------------------------------------------------------------------------------

    /**
     * Debug method to dump contents of object into string for display
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        sb.append("Alelles in map:");
        for (final Allele a:alleles) {
            sb.append(a.getDisplayString()).append(",");
        }
        sb.append("\n");
        for (final Map.Entry <GATKRead, Map<Allele, Double>> el : getLikelihoodReadMap().entrySet() ) {
            for (final Map.Entry<Allele,Double> eli : el.getValue().entrySet()) {
                sb.append("Read "+el.getKey().getName()+". Allele:"+eli.getKey().getDisplayString()+" has likelihood="+ Double.toString(eli.getValue())+"\n");
            }

        }
        return sb.toString();
    }

    public int size() {
        return likelihoodReadMap.size();
    }

    /**
     * Returns whether the map is empty.
     */
    public boolean isEmpty() {
        return likelihoodReadMap.isEmpty();
    }

    /**
     * Returns the underlying map.
     * NOTE: returns the actual representation. Callers must be aware of this.
     */
    public Map<GATKRead,Map<Allele,Double>> getLikelihoodReadMap() {
        return likelihoodReadMap;
    }

    /**
     * Get an unmodifiable set of the unique alleles in this PerReadAlleleLikelihoodMap
     * @return a non-null unmodifiable set
     */
    public Set<Allele> getAllelesSet() {
        return Collections.unmodifiableSet(allelesSet);
    }

    /**
     * Returns the set of reads stores in the map.
     */
    public Set<GATKRead> getReads() {
        return likelihoodReadMap.keySet();
    }

    /**
     * Does the current map contain the key associated with a particular SAM record in pileup?
     * @param p                 Pileup element
     * @return true if the map contains pileup element, else false
     */
    public boolean containsPileupElement(final PileupElement p) {
        Utils.nonNull(p);
        return likelihoodReadMap.containsKey(p.getRead());
    }

    /**
     * @return  A map from allele to their likelihoods is the read contains {@link PileupElement} {@code p},
     *          otherwise {@code null}.
     */
    public Map<Allele,Double> getLikelihoodsAssociatedWithPileupElement(final PileupElement p) {
        if (!likelihoodReadMap.containsKey(p.getRead())) {
            return null;
        }

        return likelihoodReadMap.get(p.getRead());
    }

    /**
     * Returns the allele->likelihood map the corresponds to the element's read or {@code null} if no such map exists.
     * @throws IllegalArgumentException if {@code p} is {@code null}.
     */
    public Map<Allele, Double> getLikelihoods(final PileupElement p) {
        Utils.nonNull(p);
        return likelihoodReadMap.get(p.getRead()); //this will return null if the read is not in the map already.
    }

    /**
     * Returns the number of alleles in this map.
     */
    @VisibleForTesting
    int alleleCount() {
        return alleles.size();
    }

    /**
     * Returns an allele at given index.
     */
    @VisibleForTesting
    Allele getAllele(final int i) {
        return alleles.get(i);
    }

    /**
     * Returns an unmodifiable collection of maps from allele to likelihoods.
     * There is one map per read.
     */
    @VisibleForTesting
    Collection<Map<Allele, Double>> getAlleleToLikelihoodMaps() {
        return Collections.unmodifiableCollection(likelihoodReadMap.values());
    }
}
