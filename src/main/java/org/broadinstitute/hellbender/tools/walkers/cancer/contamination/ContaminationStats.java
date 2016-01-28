package org.broadinstitute.hellbender.tools.walkers.cancer.contamination;

import org.broadinstitute.hellbender.utils.GenomeLoc;

/**
 * a class that tracks our contamination stats; both the estimate of contamination, as well as the number of sites and other
 * run-specific data
 */
public final class ContaminationStats {
    static final int ALLELE_COUNT = 4;
    private GenomeLoc site;
    private int numberOfSites = 0;
    private double sumOfAlleleFrequency = 0.0;
    private long basesFor = 0l;
    private long basesAgainst = 0l;
    private long basesOther = 0l;
    private ContaminationEstimate contaminationEstimate;
    private final int[] alleleBreakdown;

    public ContaminationStats(GenomeLoc site, int numberOfSites, double sumOfAlleleFrequency, long basesFor, long basesAgainst, long basesOther, int alleleBreakdown[], ContaminationEstimate estimate) {
        this.site = site;
        this.numberOfSites = numberOfSites;
        this.sumOfAlleleFrequency = sumOfAlleleFrequency;
        this.basesFor = basesFor;
        this.basesAgainst = basesAgainst;
        this.contaminationEstimate = estimate;
        if (alleleBreakdown.length != ALLELE_COUNT) throw new IllegalArgumentException("Allele breakdown should have length " + ALLELE_COUNT);
        this.alleleBreakdown = alleleBreakdown;
    }

    public int getNumberOfSites() {
        return numberOfSites;
    }

    public double getMinorAlleleFrequency() {
        return sumOfAlleleFrequency /(double)numberOfSites;
    }

    public long getBasesMatching() {
        return basesFor;
    }

    public long getBasesOther() {
        return basesOther;
    }

    public long getBasesMismatching() {
        return basesAgainst;
    }

    public ContaminationEstimate getContamination() {
        return this.contaminationEstimate;
    }

    public GenomeLoc getSite() {
        return site;
    }

    public void add(ContaminationStats other) {
        if (other == null) return; 
        this.numberOfSites          += other.numberOfSites;
        this.sumOfAlleleFrequency   += other.sumOfAlleleFrequency;
        this.basesOther             += other.basesOther;
        this.basesFor               += other.basesFor;
        this.basesAgainst           += other.basesAgainst;
        for (int x = 0; x < ALLELE_COUNT; x++) this.alleleBreakdown[x] += other.alleleBreakdown[x];
        for (int i = 0; i < this.contaminationEstimate.getBins().length; i++) {
            this.contaminationEstimate.getBins()[i] += other.contaminationEstimate.getBins()[i];
        }
        this.contaminationEstimate.setPopulationFit(this.contaminationEstimate.getPopulationFit() +other.contaminationEstimate.getPopulationFit()); 
    }
}