package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.primitives.Bytes;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.util.SequenceUtil;
import htsjdk.variant.variantcontext.*;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeAssignmentMethod;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.AlleleList;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * An {@code SVJunction} represents a suspected disagreement between a reference arrangement of DNA nucleotides and sample arrangement.
 * The disagreement should be anchored by agreements between the two system of arrangements surronding it, i.e. some flanking context
 * so that the location can be known and quantitative evaluation of the fitness of data (and the reverse genotype inference)
 * to either system can be performed.
 */
abstract class SVJunction implements Serializable {
    private static final long serialVersionUID = 1L;
    String debugString; // a place to put debugging information

    // -----------------------------------------------------------------------------------------------
    // Common base fields for all types of SV's. All should be set by parsing input VCF.
    // -----------------------------------------------------------------------------------------------
    protected final VariantContext vc;

    protected final GATKSVVCFHeaderLines.SVTYPES type;

    /**
     * 5'- and 3'- breakpoints locations.
     * For insertion these two would be the same.
     *
     * todo  Both will be a single base location for now.
     */
    protected final SimpleInterval fiveEndBPLoc;
    protected final SimpleInterval threeEndBPLoc;

    protected final int svLength;

    protected final BreakpointAnnotations breakpointAnnotations;

    protected final EvidenceAnnotations evidenceAnnotations;

    protected class BreakpointAnnotations implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * (Micro-)Homology around supposedly two breakpoints of the same SV event.
         * null when the VC doesn't contain this attribute.
         */
        protected final byte[] maybeNullHomology;

        /**
         * Inserted sequence around an structural variation breakpoint.
         * null when the VC doesn't contain this attribute.
         */
        protected final byte[] maybeNullInsertedSeq;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BreakpointAnnotations that = (BreakpointAnnotations) o;

            if (!Arrays.equals(maybeNullHomology, that.maybeNullHomology)) return false;
            return Arrays.equals(maybeNullInsertedSeq, that.maybeNullInsertedSeq);
        }

        @Override
        public int hashCode() {
            int result = Arrays.hashCode(maybeNullHomology);
            result = 31 * result + Arrays.hashCode(maybeNullInsertedSeq);
            return result;
        }

        BreakpointAnnotations(final byte[] maybeNullHomology,
                              final byte[] maybeNullInsertedSeq) {

            this.maybeNullHomology = maybeNullHomology;
            this.maybeNullInsertedSeq = maybeNullInsertedSeq;
        }
    }

    protected class EvidenceAnnotations implements Serializable {
        private static final long serialVersionUID = 1L;

        protected final List<Long> assemblyIDs;
        protected final List<Tuple2<Long, String>> contigIDs;
        /**
         * Number of high quality mappings (criteria decided by caller).
         */
        protected final int numHQMappings;

        /**
         * Maximal alignment length out of the contigs that support this breakpoint
         */
        protected final int maxAlignLength;

        EvidenceAnnotations(final List<Long> assemblyIDs, final List<Tuple2<Long, String>> contigIDs,
                            final int numHQMappings, final int maxAlignLength) {
            this.assemblyIDs = assemblyIDs;
            this.contigIDs = contigIDs;
            this.numHQMappings = numHQMappings;
            this.maxAlignLength = maxAlignLength;
        }
    }

    // -----------------------------------------------------------------------------------------------
    // Fields in this block must be computed, not parsed from input VC record
    // -----------------------------------------------------------------------------------------------
    protected static final SimpleInterval default_window_boundary = new SimpleInterval("1", 1, 1);
    // 5'- and 3'- end reference intervals that the reference alleles--one for each breakpoint--reside in
    protected Tuple2<SimpleInterval, SimpleInterval> referenceWindows = new Tuple2<>(default_window_boundary, default_window_boundary);

    /**
     * List of alleles for this junction.
     * Actual instances must return an even number for {@link AlleleList#numberOfAlleles()},
     * because of two breakpoints (5'- and 3'- end), except for insertion where the two breakpoints coincide.
     */
    protected List<SVDummyAllele> alleleList = new ArrayList<>();

    private int[] plVec = new int[] {0, 0, 0};

    /**
     * TODO: this is possible only because of single sample; full-blown cohort genotyping for SV is to be done (SNP and indel models should be learned)
     * Simply infer genotype of the single diploid sample from the computed PL.
     * @return a new genotype based on the PL of input genotype
     */
    @VisibleForTesting
    static Genotype inferGenotypeFromPL(final Genotype gtWithPL, final List<Allele> alleles){

        final GenotypeBuilder builder = new GenotypeBuilder(gtWithPL);
        if(!GATKVariantContextUtils.isInformative(gtWithPL.getLikelihoods().getAsVector())){
            builder.noPL();
        }

        final double[] ll = MathUtils.normalizeLog10(gtWithPL.getLikelihoods().getAsVector(), true, true);

        GATKVariantContextUtils.makeGenotypeCall(SingleDiploidSampleBiallelicSVGenotyperSpark.ploidy, builder, GenotypeAssignmentMethod.USE_PLS_TO_ASSIGN, ll, alleles);

        return builder.make();
    }

    // -----------------------------------------------------------------------------------------------
    // Common interfaces (mostly trivia)
    // -----------------------------------------------------------------------------------------------
    final VariantContext getOriginalVC(){ return vc;}

    /**
     * Get ids of putative breakpoints identified by {@link FindBreakpointEvidenceSpark}, that the caller believes supports this SV junction.
     */
    final List<Long> getAssemblyIDs(){
        return evidenceAnnotations.assemblyIDs;
    }

    final List<Tuple2<Long, String>> getContigIDs(){
        return evidenceAnnotations.contigIDs;
    }

    /**
     * Get the positions on reference this SV junction brought together (or set apart if it is an insertion).
     */
    final List<SimpleInterval> getLeftAlignedBreakpointLocations(){
        return Arrays.asList(fiveEndBPLoc, threeEndBPLoc);
    }

    /**
     * @return alleles around breakpoints of this junction. The reference alleles are guaranteed to come first.
     */
    final List<SVDummyAllele> getAlleles(){
        return alleleList;
    }

    final Tuple2<SimpleInterval, SimpleInterval> getReferenceWindows(){
        return referenceWindows;
    }

    /**
     * Since GL is most likely deprecated, no GL accessor is provided, only PL.
     */
    final int[] getPL(){
        return Arrays.copyOfRange(plVec, 0, plVec.length);
    }

    final SVJunction setPL(final int[] pls) {
        plVec = Arrays.copyOfRange(pls, 0, pls.length);
        return this;
    }

    /**
     * Add new attributes to the VC that was used for constructing this junction record
     * to the VC stored in the junction that just went under genotyping and return it.
     *
     * Note that FT will not be handled here.
     * TODO: works only for single sample
     */
    // TODO: 1/10/17 test
    final VariantContext getGenotypedVC(){

        final Genotype tobeDetermined = new GenotypeBuilder(SingleDiploidSampleBiallelicSVGenotyperSpark.testSampleName, GATKVariantContextUtils.noCallAlleles(2))
                .PL(getPL())
                .noDP()
                .noAD()
                .phased(false)
                .make();

        return new VariantContextBuilder(this.vc).genotypes(inferGenotypeFromPL(tobeDetermined, vc.getAlleles())).make();
    }

    // -----------------------------------------------------------------------------------------------
    // Overridable (mostly called in ctor)
    // -----------------------------------------------------------------------------------------------
    protected SVJunction(final VariantContext vc){

        // simply copy from caller VCF
        this.vc = vc;
        // TODO: is this really necessary?
        type = Enum.valueOf(GATKSVVCFHeaderLines.SVTYPES.class, vc.getAttributeAsString(GATKSVVCFHeaderLines.SVTYPE, "NONSENSE"));

        fiveEndBPLoc = new SimpleInterval( vc.getContig() , vc.getStart() , vc.getStart() );
        threeEndBPLoc = new SimpleInterval( vc.getContig() , vc.getEnd() , vc.getEnd() );

        svLength = vc.getAttributeAsInt(GATKSVVCFHeaderLines.SVLEN, -1);
        Utils.validateArg(svLength>0, vc.getID() + " missing " + GATKSVVCFHeaderLines.SVLEN);

        breakpointAnnotations = extractBreakpointAnnotations(vc);

        evidenceAnnotations = extractEvidenceAnnotations(vc);
        // lazy work is over, no more copying, compute
    }

    private BreakpointAnnotations extractBreakpointAnnotations(final VariantContext vc) {


        final Object maybeNullHomologyObj = vc.getAttribute(GATKSVVCFHeaderLines.HOMOLOGY);
        final byte[] maybeNullHomology = maybeNullHomologyObj ==null? null : ((String) maybeNullHomologyObj).getBytes();
        final Object maybeNullInsertedSeqObj = vc.getAttribute(GATKSVVCFHeaderLines.INSERTED_SEQUENCE);
        final byte[] maybeNullInsertedSeq = maybeNullInsertedSeqObj ==null? null : ((String) maybeNullInsertedSeqObj).getBytes();

        return new BreakpointAnnotations(maybeNullHomology, maybeNullInsertedSeq);
    }

    private EvidenceAnnotations extractEvidenceAnnotations(final VariantContext vc) {
        final int numHQMappings = vc.getAttributeAsInt(GATKSVVCFHeaderLines.HQ_MAPPINGS, -1);
        final int maxAlignLength = vc.getAttributeAsInt(GATKSVVCFHeaderLines.MAX_ALIGN_LENGTH, -1);
        Utils.validateArg(numHQMappings>0, vc.getID() + " missing " + GATKSVVCFHeaderLines.HQ_MAPPINGS);
        Utils.validateArg(maxAlignLength>0, vc.getID() + " missing " + GATKSVVCFHeaderLines.MAX_ALIGN_LENGTH);

        final List<Long> assemblyIDs = Arrays.stream( vc.getAttributeAsString(GATKSVVCFHeaderLines.ASSEMBLY_IDS, "").replace("[", "").replace("]", "").split(", ") ).map(Long::valueOf).collect(Collectors.toList());

        final List<String> unplacedcontigIDs = Arrays.stream( vc.getAttributeAsString(GATKSVVCFHeaderLines.CONTIG_IDS, "").replace("[", "").replace("]", "").split(", ") ).map(s -> s.replace(">", "")).collect(Collectors.toList());
        final List<Tuple2<Long, String>> contigIDs = new ArrayList<>(unplacedcontigIDs.size());
        for(int i=0; i<unplacedcontigIDs.size(); ++i){
            contigIDs.add(new Tuple2<>(assemblyIDs.get(i), unplacedcontigIDs.get(i)));
        }

        return new EvidenceAnnotations(assemblyIDs, contigIDs, numHQMappings, maxAlignLength);
    }

    /**
     * Converting from a discovery {@link VariantContext} call to an appropriate {@link SVJunction} suitable for genotyping.
     */
    static final SVJunction convertToSVJunction(final VariantContext vc,
                                                final Broadcast<Map<Long, List<LocalAssemblyContig>>> assembly2Alignments,
                                                final Broadcast<ReferenceMultiSource> referenceMultiSourceBroadcast) {

        final GATKSVVCFHeaderLines.SVTYPES type = Enum.valueOf(GATKSVVCFHeaderLines.SVTYPES.class, vc.getAttributeAsString(GATKSVVCFHeaderLines.SVTYPE, "NONSENSE"));

        switch (type){
            case INV:
                return new InversionJunction(vc, assembly2Alignments, referenceMultiSourceBroadcast);
            default:
                throw new IllegalArgumentException("Unsupported type of variant " + type.name() +" "+ vc.getID());
        }
    }

    /**
     * Construct windows around the two breakpoints identified.
     * @return the returned struct has string representations of the two reference contig names for the two breakpoints,
     *         first 5'-End then 3'-End, the int array is a length-4 array indicating the starting and ending points of the
     *         two windows, all inclusive.
     */
    protected abstract Tuple2<SimpleInterval, SimpleInterval> constructReferenceWindows();

    protected void constructAlleles(final ReferenceMultiSource reference){
        referenceWindows = constructReferenceWindows();

        final List<SVDummyAllele> alleles = constructReferenceAlleles(reference);
        alleles.addAll(constructAlternateAlleles(reference));
        alleleList = alleles;
    }

    /**
     * Construct reference alleles around the two breakpoints of the identified sv junction.
     *
     * By default, it simply extract bases in the windows marked by {@link #constructReferenceWindows()}.
     *
     * @return a list of reference alleles, with 5'-end being the first and 3'-end being the second. In case of inversion, only one will be .
     */
    @VisibleForTesting
    protected ArrayList<SVDummyAllele> constructReferenceAlleles(final ReferenceMultiSource reference){
        try{
            final SimpleInterval fiveEndWindow = referenceWindows._1();
            final SimpleInterval threeEndWindow = referenceWindows._2();

            return new ArrayList<>(Arrays.asList(new SVDummyAllele(reference.getReferenceBases(null, fiveEndWindow).getBases(), true),
                                 new SVDummyAllele(reference.getReferenceBases(null, threeEndWindow).getBases(), true)));

        } catch (final IOException ioex){
            throw new GATKException("Cannot resolve reference for constructing ref allele for junction.");
        }
    }

    protected abstract ArrayList<SVDummyAllele> constructAlternateAlleles(final ReferenceMultiSource reference);

    /**
     * Filter read that will be used for genotyping.
     * Assumes the logic applies to all junctions of the same type.
     * TODO: this interface implicitly works against pairedness.
     */
    protected abstract boolean readSuitableForGenotyping(final GATKRead read);

    /**
     * Update read based on result from its realignment to ref and alt contigs.
     * Input reads are deep copied for making the necessary changes.
     */
    protected abstract ReadLikelihoods<SVDummyAllele> updateReads(final ReadLikelihoods<SVDummyAllele> reads);

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final SVJunction that = (SVJunction) o;

        if (svLength != that.svLength) return false;
        if (type != that.type) return false;
        if (!fiveEndBPLoc.equals(that.fiveEndBPLoc)) return false;
        if (!threeEndBPLoc.equals(that.threeEndBPLoc)) return false;

        return breakpointAnnotations.equals(that.breakpointAnnotations);
    }

    @Override
    public int hashCode() {
        int result = type.ordinal();
        result = 31 * result + fiveEndBPLoc.hashCode();
        result = 31 * result + threeEndBPLoc.hashCode();
        result = 31 * result + svLength;
        result = 31 * result + breakpointAnnotations.hashCode();
        return result;
    }

// TODO: Serialization related problems prevents putting LocalAssemblyContig's as fields, VC class seems to be Java
    //         serializable (seems not the other way, but most likely because I haven't found a way yet)
    //         but the contigs must be Kryo serialized and are not currently used for constructing alleles;
    //         really should be put back in the future
    /**
     * Extract from the input map {@code assemblyID2ContigsAlignRecords} informative contigs for this particular junction,
     */
    @VisibleForTesting
    static List<LocalAssemblyContig> getAssociatedContigs(final Map<Long, List<LocalAssemblyContig>> asmID2ItsContigs,
                                                          final List<Tuple2<Long, String>> pairedASMidAndContigId){

        final List<LocalAssemblyContig> contigsInformation = new ArrayList<>(400); // blunt guess

        final Map<Long, Set<String>> map = pairedASMidAndContigId.stream().collect( Collectors.groupingBy(Tuple2::_1, Collectors.mapping(Tuple2::_2, Collectors.toSet())) );

        final Set<Long> asmids = map.keySet();

        for(final Long asmID : asmids){
            contigsInformation.addAll(asmID2ItsContigs.get(asmID).stream().filter( contig -> map.get(asmID).contains(contig.contigID)).collect(Collectors.toCollection(ArrayList::new)));
        }

        return contigsInformation;
    }









    /**
     * Custom class for holding information around two inversion breakpoints.
     */
    static final class InversionJunction extends SVJunction {
        private static final long serialVersionUID = 1L;

        @VisibleForTesting
        final BreakpointAllele.InversionType invType;


        InversionJunction(final VariantContext vc,
                          final ReferenceMultiSource reference,
                          final Map<Long, List<LocalAssemblyContig>> assemblyID2AlignmentRecords){

            super(vc);

            final int isFiveToThree = vc.getAttributeAsBoolean(GATKSVVCFHeaderLines.INV_5_TO_3, false) ? 0b1 : 0b0;
            final int bit = isFiveToThree + (vc.getAttributeAsBoolean(GATKSVVCFHeaderLines.INV_3_TO_5, false) ? 0b10 : 0b0);
            if (bit == 0b0) {
                invType = BreakpointAllele.InversionType.INV_NONE;
            } else if (bit == 0b01) {
                invType = BreakpointAllele.InversionType.INV_5_TO_3;
            } else if (bit == 0b10) {
                invType = BreakpointAllele.InversionType.INV_3_TO_5;
            } else {
                throw new IllegalArgumentException("Seemingly broken VCF, where the inversion breakpoint is of both type 5-to-3 and 3-to-5. Site: "
                        + vc.getContig() + ":" + vc.getID());
            }

            constructAlleles(reference);
        }

        /**
         * {@inheritDoc}
         */
        @VisibleForTesting
        InversionJunction(final VariantContext vc,
                          final Broadcast<Map<Long, List<LocalAssemblyContig>>> assembly2Alignments,
                          final Broadcast<ReferenceMultiSource> referenceMultiSourceBroadcast) {
            this(vc, referenceMultiSourceBroadcast.getValue(), assembly2Alignments.getValue());
        }

        /**
         * TODO: confirm that the breakpoint locations set by caller is what's assumed: BP is where the ref and alt begin to differ
         * TODO: get the short window case correct.
         */
        @VisibleForTesting
        @Override
        protected Tuple2<SimpleInterval, SimpleInterval> constructReferenceWindows(){

            final List<SimpleInterval> leftAlignedBreakpointLocations = getLeftAlignedBreakpointLocations();
            final SimpleInterval fiveEndBPLoc = leftAlignedBreakpointLocations.get(0);
            final SimpleInterval threeEndBPLoc = leftAlignedBreakpointLocations.get(1);

            final String contig = fiveEndBPLoc.getContig();

            final int flank = SingleDiploidSampleBiallelicSVGenotyperSpark.readLength - 1; // -1, think about it
            final int extraFlanking = (breakpointAnnotations.maybeNullHomology==null) ? 0 : breakpointAnnotations.maybeNullHomology.length;
            final int ll = fiveEndBPLoc.getStart()  - flank;
            final int lr = fiveEndBPLoc.getEnd()    + (flank-1) + (invType.equals(BreakpointAllele.InversionType.INV_NONE) ? 0 : extraFlanking);
            final int rl = threeEndBPLoc.getStart() - flank - (invType.equals(BreakpointAllele.InversionType.INV_NONE) ? 0 : extraFlanking);
            final int rr = threeEndBPLoc.getEnd()   + (flank-1);

            return new Tuple2<>(new SimpleInterval(contig, ll, lr), new SimpleInterval(contig, rl, rr));
        }

        //TODO: confirm that the breakpoint locations set by caller is what's assumed: BP is where the ref and alt begin to differ
        // TODO: because the assembler may decide to stop short of extending to the full windows around identified breakpoint,
        //       making use of the assembled contig to construct the alt allele is not mature yet until we can make sense out of the assembly graph
        //       so now we simply use the reference bases to construct the alt allele,
        //       what could be done intermediately is to amend the allele bases with the contigs, although this way
        //       there will be more one one alt allele for one end, because that's the reason why the assembler decides keep the bubble there
        @Override
        protected final ArrayList<SVDummyAllele> constructAlternateAlleles(final ReferenceMultiSource reference){

            try {
                final int flank = SingleDiploidSampleBiallelicSVGenotyperSpark.readLength - 1;
                final int hom = breakpointAnnotations.maybeNullHomology == null ? 0 : breakpointAnnotations.maybeNullHomology.length;
                final int ins = breakpointAnnotations.maybeNullInsertedSeq == null ? 0 : breakpointAnnotations.maybeNullInsertedSeq.length;

                final byte[] ll = reference.getReferenceBases(null, new SimpleInterval(fiveEndBPLoc.getContig(), fiveEndBPLoc.getStart() - flank, fiveEndBPLoc.getEnd() - 1)).getBases(); // flank bases left to 5-BP
                final byte[] rr = reference.getReferenceBases(null, new SimpleInterval(threeEndBPLoc.getContig(), threeEndBPLoc.getEnd(), threeEndBPLoc.getEnd()+flank-1)).getBases();
                if (hom!=0 && ins!=0) { // both insertion and homology

                    final byte[] lr = reference.getReferenceBases(null, new SimpleInterval(fiveEndBPLoc.getContig(), fiveEndBPLoc.getEnd()+hom, fiveEndBPLoc.getEnd()+hom+flank-1)).getBases();
                    final byte[] rl = reference.getReferenceBases(null, new SimpleInterval(threeEndBPLoc.getContig(), threeEndBPLoc.getStart()-hom-flank, threeEndBPLoc.getEnd()-hom-1)).getBases();
                    SequenceUtil.reverseComplement(rl);

                    byte[] firstHalfOfLeft;
                    byte[] firstHalfOfRight;

                    // case 3a: homology + insertion in '+' strand representation of a 5-to-3 inversion
                    firstHalfOfLeft  = Bytes.concat(ll, breakpointAnnotations.maybeNullHomology, breakpointAnnotations.maybeNullInsertedSeq);
                    final SVDummyAllele leftAltAlleleHomIns = new SVDummyAllele(Bytes.concat(firstHalfOfLeft, rl), false);
                    firstHalfOfRight = Bytes.concat(breakpointAnnotations.maybeNullHomology, breakpointAnnotations.maybeNullInsertedSeq, lr);
                    SequenceUtil.reverseComplement(firstHalfOfRight);
                    final SVDummyAllele rightAltAlleleHomIns = new SVDummyAllele(Bytes.concat(firstHalfOfRight, rr), false);

                    // case 3b: insertion + homology in '+' strand representation of a 5-to-3 inversion
                    firstHalfOfLeft  = Bytes.concat(ll, breakpointAnnotations.maybeNullInsertedSeq, breakpointAnnotations.maybeNullHomology);
                    final SVDummyAllele leftAltAlleleInsHom = new SVDummyAllele(Bytes.concat(firstHalfOfLeft, rl), false);
                    firstHalfOfRight = Bytes.concat(breakpointAnnotations.maybeNullInsertedSeq, breakpointAnnotations.maybeNullHomology, lr);
                    SequenceUtil.reverseComplement(firstHalfOfRight);
                    final SVDummyAllele rightAltAlleleInsHom = new SVDummyAllele(Bytes.concat(firstHalfOfRight, rr), false);

                    return new ArrayList<>( Arrays.asList(leftAltAlleleHomIns, rightAltAlleleHomIns, leftAltAlleleInsHom, rightAltAlleleInsHom) );
                } else {
                    byte[] tobeFlipped = reference.getReferenceBases(null, new SimpleInterval(threeEndBPLoc.getContig(), threeEndBPLoc.getStart()-flank-hom, threeEndBPLoc.getStart()-hom-1)).getBases();
                    SequenceUtil.reverseComplement(tobeFlipped);

                    final SVDummyAllele leftAltAllele;
                    if (hom==0 && ins==0) leftAltAllele = new SVDummyAllele(Bytes.concat(ll,                       tobeFlipped), false);    // nothing, very clean
                    else if(hom!=0)       leftAltAllele = new SVDummyAllele(Bytes.concat(ll, breakpointAnnotations.maybeNullHomology,    tobeFlipped), false);    // only homology
                    else                  leftAltAllele = new SVDummyAllele(Bytes.concat(ll, breakpointAnnotations.maybeNullInsertedSeq, tobeFlipped), false);    // only insertion

                    tobeFlipped = reference.getReferenceBases(null, new SimpleInterval(fiveEndBPLoc.getContig(), fiveEndBPLoc.getEnd()+hom, fiveEndBPLoc.getEnd()+hom+flank-1)).getBases();
                    if (hom!=0)      tobeFlipped = Bytes.concat(breakpointAnnotations.maybeNullHomology, tobeFlipped);
                    else if (ins!=0) tobeFlipped = Bytes.concat(breakpointAnnotations.maybeNullInsertedSeq, tobeFlipped);
                    SequenceUtil.reverseComplement(tobeFlipped);
                    final SVDummyAllele rightAltAllele = new SVDummyAllele(Bytes.concat(tobeFlipped, rr), false);

                    return new ArrayList<>(Arrays.asList(leftAltAllele, rightAltAllele));
                }
            } catch (final IOException ioex){
                throw new GATKException("Cannot resolve reference for constructing ref allele for inversion junction");
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            final InversionJunction that = (InversionJunction) o;

            return invType == that.invType;

        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + invType.ordinal();
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean readSuitableForGenotyping(final GATKRead read){
            if(read.getAttributeAsInteger("RC")!=null && read.getAttributeAsInteger("RC")==1) return true; // these are FASTQ reconstructed reads, always use
            return readResideInJunctionWindow(read); // TODO: read could also share k-mer with ref or alt
        }

        /**
         * Test if a particular read starts after or ends before the two windows spanned by the inversion junction.
         */
        @VisibleForTesting
        boolean readResideInJunctionWindow(final GATKRead read){

            if(read.isUnmapped()) return false; // TODO: really stupid!

            final Tuple2<SimpleInterval, SimpleInterval> windows = this.getReferenceWindows();
            return windows._1().contains(read) || windows._2().contains(read);
        }

        /**
         * {@inheritDoc}
         *
         * Normalizes the read likelihoods, see {@link ReadLikelihoods#normalizeLikelihoods(boolean, double)}.
         * filter out poorly modeled reads, see {@link ReadLikelihoods#filterPoorlyModeledReads(double)},
         * and
         * marginalizes the read likelihoods by reducing from multiple ref alleles and multiple alt alleles
         * used in the RLC (read likelihood calculation) step to a single symbolic ref allele and
         * a single symbolic alt allele (see {@link ReadLikelihoods#marginalize(Map)} for logic)
         * so that the following assumption is made:
         * (P=2, A=2) =&gt; [0/0, 0/1, 1/1] three possible genotypes for a diploid biallelic sample.
         * TODO: is this marginalization logic correct?
         *
         * TODO: Re-alignment of reads back to their best allele is not implemented yet (as it happens in HC).
         */
        @VisibleForTesting
        @Override
        protected ReadLikelihoods<SVDummyAllele> updateReads(final ReadLikelihoods<SVDummyAllele> matrix){

            matrix.normalizeLikelihoods(false, QualityUtils.qualToErrorProbLog10(SingleDiploidSampleBiallelicSVGenotyperSpark.maximumLikelihoodDifferenceCap));
            matrix.filterPoorlyModeledReads(SingleDiploidSampleBiallelicSVGenotyperSpark.expectedBaseErrorRate);

            final Map<SVDummyAllele, List<SVDummyAllele>> symbolicOnes2RealAlleles = new HashMap<>();
            final List<SVDummyAllele> alleles = this.getAlleles();
            final List<SVDummyAllele> vcAlleles = this.getOriginalVC().getAlleles().stream().map(SVDummyAllele::new).collect(Collectors.toList());
            symbolicOnes2RealAlleles.put(vcAlleles.get(0), alleles.stream().filter(SVDummyAllele::isReference).collect(Collectors.toList()));
            symbolicOnes2RealAlleles.put(vcAlleles.get(1), alleles.stream().filter(SVDummyAllele::isNonReference).collect(Collectors.toList()));

            final ReadLikelihoods<SVDummyAllele> result = matrix.marginalize(symbolicOnes2RealAlleles);
            if(SingleDiploidSampleBiallelicSVGenotyperSpark.in_debug_state){
                final StringBuilder builder = new StringBuilder();
                builder.append("UPDATED_RLL_MATRIX\n").append(result.sampleMatrix(0).toString());
                this.debugString += builder.toString();
            }
            return result;
        }
    }

//    static final class InsertionJunction extends SVJunction {
//        private static final long serialVersionUID = 1L;
//    }
//
//    static final class DeletionJunction extends SVJunction {
//        private static final long serialVersionUID = 1L;
//    }
}
