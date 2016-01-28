package org.broadinstitute.hellbender.tools.walkers.cancer.contamination;

import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.util.StringUtil;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.Hidden;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.pileup.ReadPileup;

import java.io.PrintStream;
import java.util.*;

/**
 * Determine the percent contamination of an input bam by sample, by lane, or in aggregate across all the input reads
 *
 * <h3>Usage examples</h3>
 * <p>These are example commands that show how to run the ContaminationWalker (a.k.a. ContEst) for typical use cases. Square brackets ("[ ]")
 * indicate optional arguments. Note that parameter values and/or resources shown here may not be the latest recommended; see the
 * online documentation for detailed recommendations. </p>
 *
 * <br />
 * <h4>Contamination estimation using the normal BAM for genotyping on-the-fly</h4>
 * <pre>
 *   java
 *     -jar GenomeAnalysisTK.jar \
 *     -T ContaminationWalker \
 *     -R reference.fasta \
 *     -I:eval tumor.bam \
 *     -I:genotype normal.bam \
 *     --popFile populationAlleleFrequencies.vcf \
 *     -L populationSites.interval_list
 *     [-L targets.interval_list] \
 *     -isr INTERSECTION \
 *     -o output.txt
 * </pre>
 *
 *<h3>Output</h3>
 * The product of the contamination walker is a text file containing estimated percent contamination, as well as error bars on this estimate.
 *
 * <h3>Notes</h3>
 * Multiple modes are supported simultaneously, e.g. contamination by sample and readgroup can be computed in the same run.
 */
public final class ContaminationWalker extends VariantWalker {

    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    // Some constants we use
    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    /** what type of run stats would we like: */
    public enum ContaminationRunType {
        SAMPLE,    // calculate contamination for each sample
        READGROUP, // for each read group
        META        // for all inputs as a single source
    }
    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    // inputs
    // ------------------------------------------------------------------------------------------------------------------------------------------------------

    // the population information; the allele frequencies for each position in known populations
    @Argument(fullName="popfile", shortName = "pf", doc="the variant file containing information about the population allele frequencies", optional=true)
    public FeatureInput<VariantContext> pop;

    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    // outputs and args
    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    @Argument
    PrintStream out; // the general output of the tool

    @Argument(fullName = "min_qscore", optional = true, doc = "threshold for minimum base quality score")
    public int MIN_QSCORE = 20;

    @Argument(fullName = "min_mapq", optional = true, doc = "threshold for minimum mapping quality score")
    public int MIN_MAPQ = 20;

    @Argument(fullName = "trim_fraction", doc = "at most, what fraction of sites should be trimmed based on BETA_THRESHOLD", optional = true)
    public double TRIM_FRACTION = 0.01;

    @Argument(fullName = "beta_threshold", doc = "threshold for p(f>=0.5) to trim", optional = true)
    public double BETA_THRESHOLD = 0.95;

    @Argument(shortName = "llc", fullName = "lane_level_contamination", doc = "set to META (default), SAMPLE or READGROUP to produce per-bam, per-sample or per-lane estimates", optional = true)
    private Set<ContaminationRunType> laneStats = null;

    @Argument(shortName = "sn", fullName = "sample_name", doc = "The sample name; used to extract the correct genotypes from mutli-sample truth vcfs", optional = true)
    private String sampleName = "unknown";

    @Argument(shortName = "pc", fullName = "precision", doc = "the degree of precision to which the contamination tool should estimate (e.g. the bin size)", optional = true)
    private double precision = 0.1;

    @Argument(shortName = "br", fullName = "base_report", doc = "Where to write a full report about the loci we processed", optional = true)
    public PrintStream baseReport = null;

    @Argument(shortName = "lf", fullName = "likelihood_file", doc = "write the likelihood values to the specified location", optional = true)
    public PrintStream likelihoodFile = null;

    @Argument(shortName = "mbc", fullName = "minimum_base_count", doc = "what minimum number of bases do we need to see to call contamination in a lane / sample?", optional = true)
    public Integer minBaseCount = 500;

    @Argument(shortName = "population", fullName = "population", doc = "evaluate contamination for just a single contamination population", optional = true)
    public String population = "CEU";

    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    // hidden arguments
    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    @Hidden
    @Argument(fullName = "trim_interval", doc = "progressively trim from 0 to TRIM_FRACTION by this interval", optional = true)
    public double TRIM_INTERVAL = 0;

    @Hidden
    @Argument(fullName = "min_site_depth", optional = true, doc = "minimum depth at a site to consider in calculation")
    public int MIN_SITE_DEPTH = 0;

    @Hidden
    @Argument(fullName = "fixed_epsilon_qscore", optional = true, doc = "use a constant epsilon (phred scale) for calculation")
    public Byte FIXED_EPSILON = null;

    @Hidden
    @Argument(fullName = "min_genotype_depth", optional = true, doc = "what minimum depth is required to call a site in seq genotype mode")
    public int MIN_GENOTYPE_DEPTH_FOR_SEQ = 50;

    @Hidden
    @Argument(fullName = "min_genotype_ratio", optional = true, doc = "the ratio of alt to other bases to call a site a hom non-ref variant")
    public double MIN_GENOTYPE_RATIO = 0.80;

    @Hidden
    @Argument(fullName = "min_genotype_llh", optional = true, doc = "the min log likelihood for UG to call a genotype")
    public double MIN_UG_LOG_LIKELIHOOD = 5;
    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    // global variables to the walker
    // ------------------------------------------------------------------------------------------------------------------------------------------------------
    private static final Map<Integer,Allele> alleles = new HashMap<>();                   // the set of alleles we work with
    private final Map<String, ContaminationRunType> contaminationNames = new LinkedHashMap<>();       // a list, containing the contamination names, be it read groups or bam file names
    private static String[] ALL_POPULATIONS = new String[]{"ALL", "CHD", "LWK", "CHB", "CEU", "MXL", "GIH", "MKK", "TSI", "CLM", "GBR", "ASW", "YRI", "IBS", "FIN", "PUR", "JPT", "CHS"};
    private String[] populationsToEvaluate;

    // variables involved in the array-free mode
    public static final String EVAL_BAM_TAG = "eval";
    public static final String GENOTYPE_BAM_TAG = "genotype";
    String evalSample = null;
    String genotypeSample = null;


    // counts for each of the possible combinations
    int totalSites = 0;
    int countPopulationSites = 0;
    int countGenotypeNonHomVar = 0;
    int countGenotypeHomVar = 0;
    int countPassCoverage = 0;
    int countResults = 0;

    // create our list of allele characters for conversion
    static {
        alleles.put(0, Allele.create((byte) 'A'));
        alleles.put(1, Allele.create((byte) 'C'));
        alleles.put(2, Allele.create((byte) 'G'));
        alleles.put(3, Allele.create((byte) 'T'));
    }

    // a bunch of setup to initialize the walker
    @Override
    public void onTraversalStart() {
        // now sort out what tags go with what bam
        if (GENOTYPE_BAM_TAG.equalsIgnoreCase(tag)) {
            try {
                if (getToolkit().getReadsDataSource().getHeader(id).getReadGroups().isEmpty())
                    throw new RuntimeException("No Read Groups found for Genotyping BAM -- Read Groups are Required in sequencing genotype mode!");
                genotypeSample = getToolkit().getReadsDataSource().getHeader(id).getReadGroups().get(0).getSample();
            } catch (final NullPointerException npe) {
                throw new UserException.BadInput("Unable to fetch read group from the bam files tagged with " + GENOTYPE_BAM_TAG);
            }
        } else if (EVAL_BAM_TAG.equalsIgnoreCase(tag)) {
            try {
                if (getToolkit().getReadsDataSource().getHeader(id).getReadGroups().isEmpty())
                    throw new RuntimeException("No Read Groups found for Genotyping BAM -- Read Groups are Required in sequencing genotype mode!");
                evalSample = getToolkit().getReadsDataSource().getHeader(id).getReadGroups().get(0).getSample();
            } catch (final NullPointerException npe) {
                throw new UserException.BadInput("Unable to fetch read group from the bam files tagged with " + EVAL_BAM_TAG);
            }
        }

        if (evalSample == null || genotypeSample == null) {
            throw new UserException.BadInput("You must provide both a " + GENOTYPE_BAM_TAG + " tagged bam and a " + EVAL_BAM_TAG + " tagged bam file.  Please see the ContEst documentation");
        }
        if (laneStats == null) {
            laneStats = new HashSet<>();
            laneStats.add(ContaminationRunType.META);
        }

        for (final ContaminationRunType type : laneStats) {
            if (type == ContaminationRunType.READGROUP) {
                for (final SAMReadGroupRecord name : getHeaderForReads().getReadGroups()) {
                    this.contaminationNames.put(name.getId(), ContaminationRunType.READGROUP);
                }
            } else if (type == ContaminationRunType.SAMPLE) {
                for (final SAMReadGroupRecord name : getHeaderForReads().getReadGroups()) {
                    this.contaminationNames.put(name.getSample(), ContaminationRunType.SAMPLE);
                }
            } else if (type == ContaminationRunType.META)
                this.contaminationNames.put("META", ContaminationRunType.META);
            else {
                throw new IllegalArgumentException("Unknown type name " + laneStats);
            }
        }
        if (baseReport != null) {
            baseReport.println("lane\tchrom\tposition\trs_id\tref\tfreq_major_allele\tfreq_minor_allele\tgeli_gt\tmaf\tmajor_allele_counts\tminor_allele_counts\ta_counts\tc_counts\tg_counts\tt_counts");
        }

        this.populationsToEvaluate = (population == null || "EVERY".equals(population)) ? ALL_POPULATIONS : new String[]{population};

    }
    /**
     * our map function, which emits a contamination stats for each of the subgroups (lanes, samples, etc) that we encounter
     *
     * @param tracker the reference meta data tracker, from which we get the array truth data
     * @param referenceContext     the reference information at this position
     * @param context the read context, where we get the alignment data
     */
    @Override
    public void apply(final VariantContext variant, final ReadsContext readsContext, final ReferenceContext referenceContext, final FeatureContext tracker ){
        totalSites++;
        final VariantContext popVC = tracker.getFirstValue(pop);
        if (popVC == null) {
            return;
        }
        final byte referenceBase = referenceContext.getBase();
        countPopulationSites++;
        final Genotype genotype = getGenotype(context, referenceContext);

        // only use homozygous sites
        if (genotype == null || !genotype.isHomVar()) {
            countGenotypeNonHomVar++;
            return;
        } else {
            countGenotypeHomVar++;
        }


        // only use non-reference sites
        final byte myBase = genotype.getAllele(0).getBases()[0];

        final String rsNumber = "";

        // our map of contamination results
        final Map<String, Map<String, ContaminationStats>> stats = new HashMap<>();

        // get the base pileup.  This is only really required when we have both a genotyping and EVAL_BAM_TAG tagged bams
        // becuase we only want contamination estimates drawn from the eval tagged bam
        final ReadPileup defaultPile = context.getBasePileup().getPileupForSample(evalSample);

        // if we're by-lane, get those stats
        for (final Map.Entry<String, ContaminationRunType> namePair : contaminationNames.entrySet()) {
            final ReadPileup pile;
            if (namePair.getValue() == ContaminationRunType.READGROUP) {
                pile = defaultPile.getPileupForReadGroup(namePair.getKey());
            } else if (namePair.getValue() == ContaminationRunType.META) {
                pile = defaultPile;
            } else if (namePair.getValue() == ContaminationRunType.SAMPLE) {
                pile = defaultPile.getPileupForSample(namePair.getKey());
            } else {
                throw new IllegalStateException("Unknown state, contamination type = " + laneStats + " is unsupported");
            }
            if (pile != null) {

                final ReadPileup filteredPile = pile.getBaseAndMappingFilteredPileup(MIN_QSCORE, MIN_MAPQ);

                final byte[] bases = filteredPile.getBases();

                // restrict to sites that have greater than our required total depth
                if (bases.length < MIN_SITE_DEPTH) {
                    continue;
                } else {
                    countPassCoverage++;
                }

                final byte[] quals;
                if (FIXED_EPSILON == null) {
                    quals = filteredPile.getQuals();
                } else {
                    quals = new byte[bases.length];
                    Arrays.fill(quals, FIXED_EPSILON);
                }

                final Map<String, ContaminationStats> results =
                        calcStats(referenceBase,
                                bases,
                                quals,
                                myBase,
                                rsNumber,
                                popVC,
                                baseReport,
                                context.getLocation(),
                                precision,
                                namePair.getKey(),
                                populationsToEvaluate);

                if (!results.isEmpty()) {
                    countResults++;
                    stats.put(namePair.getKey(), results);
                }
            }
        }
    }

    /**
     * get the genotype for the sample at the current position
     * @param context the reads
     * @param referenceContext the reference information
     * @return a genotype call, which could be null
     */
    private Genotype getGenotype(final AlignmentContext context, final ReferenceContext referenceContext) {
        return getGenotypeFromSeq(
                context,
                referenceContext,
                this.alleles,
                this.MIN_GENOTYPE_RATIO,
                this.MIN_GENOTYPE_DEPTH_FOR_SEQ,
                this.MIN_UG_LOG_LIKELIHOOD,
                this.genotypeSample,
                this.sampleName);
    }
    
     static Genotype getGenotypeFromSeq(final AlignmentContext context,
                                        final ReferenceContext referenceContext,
                                        final Map<Integer, Allele> alleles,
                                        final double minGenotypeRatio,
                                        final int minGenotypingDepth,
                                        final double minGenotypingLOD,
                                        final String genotypingSample,
                                        final String sampleName) {
        final ReadPileup pileup = context.getBasePileup().getPileupForSample(genotypingSample);
        if (pileup == null || pileup.isEmpty()) return null;

        if (sum(pileup.getBaseCounts()) < minGenotypingDepth) {
            return null;
        }
        final int[] bases = pileup.getBaseCounts();
        final int mx = maxPos(bases);
        final int allGenotypes = sum(bases);
        final String refBase = String.valueOf((char)referenceContext.getBase());
        if (bases[mx] / (float)allGenotypes >= minGenotypeRatio && !refBase.equals(alleles.get(mx).getBaseString())) {
            final List<Allele> al = new ArrayList<>();
            al.add(alleles.get(mx));
            final GenotypeBuilder builder = new GenotypeBuilder(sampleName, al);
            return builder.make();
        }
        return null;
    }

    // utils
    private static int sum(final int[] a) {int sm = 0; for (final int i : a) {sm = sm + i;} return sm;}
    private static int maxPos(final int[] a) {int mx = 0; for (int i = 0; i < a.length; i++) {if (a[i] > a[mx]) mx = i;} return mx;}


    private static final class PopulationFrequencyInfo {
        private byte majorAllele;
        private byte minorAllele;
        private double minorAlleleFrequency;

        private PopulationFrequencyInfo(final byte majorAllele, final byte minorAllele, final double minorAlleleFrequency) {
            this.majorAllele = majorAllele;
            this.minorAllele = minorAllele;
            this.minorAlleleFrequency = minorAlleleFrequency;
        }

        public byte getMajorAllele() {
            return majorAllele;
        }

        public byte getMinorAllele() {
            return minorAllele;
        }

        public double getMinorAlleleFrequency() {
            return minorAlleleFrequency;
        }
    }

    private static PopulationFrequencyInfo parsePopulationFrequencyInfo(final VariantContext variantContext, final String population) {
        PopulationFrequencyInfo info = null;

        final List<String> values = (List<String>) variantContext.getAttribute(population);

        if (values != null) {
            byte majorAllele = 0;
            byte minorAllele = 0;
            double maf = -1;

            for (String str : values) {
                // strip off the curly braces and trim whitespace
                if (str.startsWith("{")) {
                    str = str.substring(1, str.length());
                }
                if (str.contains("}")) {
                    str = str.substring(0, str.indexOf("}"));
                }
                str = str.trim();
                final String[] spl = str.split("=");

                final byte allele = (byte) spl[0].trim().charAt(0);
                final double af = Double.valueOf(spl[1].trim());

                if (af <= 0.5 && minorAllele == 0) {
                    minorAllele = allele;
                    maf = af;
                } else {
                    majorAllele = allele;
                }

            }

            info = new PopulationFrequencyInfo(majorAllele, minorAllele, maf);
        }
        return info;
    }


    /**
     * Calculate the contamination values per division, be it lane, meta, sample, etc
     * @param referenceBase the reference base
     * @param bases the bases seen
     * @param quals and the bases qual values
     * @param myAllele the allele we have (our hom var genotype allele)
     * @param rsNumber the dbsnp number if available
     * @param popVC the population variant context from hapmap
     * @param baseReport if we're writing a base report, write it here
     * @param loc our location
     * @param precision the percision we're aiming for
     * @param lane the lane name information
     * @param pops our pops to run over
     * @return a mapping of each target population to their estimated contamination
     */
    private static Map<String, ContaminationStats> calcStats(final byte referenceBase,
                                                             final byte[] bases,
                                                             final byte[] quals,
                                                             final byte myAllele,
                                                             final String rsNumber,
                                                             final VariantContext popVC,
                                                             final PrintStream baseReport,
                                                             final GenomeLoc loc,
                                                             final Double precision,
                                                             final String lane,
                                                             final String[] pops) {
        final int[] alts = new int[4];
        int total = 0;
        // get the depth ratio we are aiming for
        for (final byte base : bases) {
            if (base == 'A' || base == 'a') alts[0]++;
            if (base == 'C' || base == 'c') alts[1]++;
            if (base == 'G' || base == 'g') alts[2]++;
            if (base == 'T' || base == 't') alts[3]++;
            total++;
        }

        final Map<String, ContaminationStats> ret = new HashMap<>();

        for (final String pop : pops) {
            final PopulationFrequencyInfo info = parsePopulationFrequencyInfo(popVC, pop);
            final double alleleFreq = info.getMinorAlleleFrequency();
            if (alleleFreq > 0.5) {
                throw new RuntimeException("Minor allele frequency is greater than 0.5, this is an error; we saw AF of " + alleleFreq);
            }

            final int majorCounts = alts[getBaseIndex(info.getMajorAllele())];
            final int minorCounts = alts[getBaseIndex(info.getMinorAllele())];
            final int otherCounts = total - majorCounts - minorCounts;


            // only use sites where this is the minor allele
            if (myAllele == info.minorAllele) {

                if (pops.length == 1) {
                    if (baseReport != null) {
                        baseReport.print(
                                StringUtil.join("\t",
                                        lane,
                                        loc.getContig(),
                                        "" + loc.getStart(),
                                        rsNumber,
                                        "" + (char) referenceBase,
                                        "" + (char) info.getMajorAllele(),
                                        "" + (char) info.getMinorAllele(),
                                        "" + (char) info.getMinorAllele() + "" + (char) info.getMinorAllele(),
                                        String.format("%1.4f", alleleFreq),
                                        "" + majorCounts,
                                        "" + minorCounts));

                        for (final long cnt : alts)
                            baseReport.print("\t" + cnt);
                        baseReport.println();
                    }
                }

                final ContaminationEstimate est = new ContaminationEstimate(precision, alleleFreq, bases, quals, info.getMinorAllele(), info.getMajorAllele(), pop, loc);
                ret.put(pop, new ContaminationStats(loc, 1, alleleFreq, minorCounts, majorCounts, otherCounts, alts, est));

            }

        }
//        final ContaminationResults res= new ContaminationResults(precision);
//        if (value != null) {
//            result.add(value)
//        }

        return ret;
    }

    private static int getBaseIndex(final byte base) {
        if (base == 'A' || base == 'a') return 0;
        if (base == 'C' || base == 'c') return 1;
        if (base == 'G' || base == 'g') return 2;
        if (base == 'T' || base == 't') return 3;
        return -1;
    }

    /**
     * on traversal done, output all the stats to the appropriate files
     */
    @Override
    public ContaminationResults onTraversalDone() {

        // filter out lanes / samples that don't have the minBaseCount
        final Map<String, Map<String, ContaminationStats>> cleanedMap = new HashMap<>();
        for (final Map.Entry<String, Map<String, ContaminationStats>> entry : result.getStats().entrySet()) {

            final Map<String, ContaminationStats> newMap = new HashMap<>();

            final Map<String, ContaminationStats> statMap = entry.getValue();
            for (final String popKey : statMap.keySet()) {
                final ContaminationStats stat = statMap.get(popKey);
                if (stat.getBasesMatching() + stat.getBasesMismatching() >= minBaseCount) {
                    newMap.put(popKey, stat);
                }
            }


            if (newMap.isEmpty()) {
                out.println("Warning: We're throwing out lane " + entry.getKey() + " since it has fewer than " + minBaseCount +
                        " read bases at genotyped positions");
            } else {
                cleanedMap.put(entry.getKey(), newMap);
            }
        }

        // output results at the end, based on the input parameters
        result.setStats(cleanedMap);
        result.outputReport(precision, out, TRIM_FRACTION, TRIM_INTERVAL, BETA_THRESHOLD);
        if (likelihoodFile != null) {
            result.writeCurves(likelihoodFile);
        }
        logger.info("Total sites:  " + totalSites);
        logger.info("Population informed sites:  " + countPopulationSites);
        logger.info("Non homozygous variant sites: " + countGenotypeNonHomVar);
        logger.info("Homozygous variant sites: " + countGenotypeHomVar);
        logger.info("Passed coverage: " + countPassCoverage);
        logger.info("Results: " + countResults);

        return result;
    }
}
