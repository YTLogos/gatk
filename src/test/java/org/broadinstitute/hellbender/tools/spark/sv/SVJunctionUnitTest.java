package org.broadinstitute.hellbender.tools.spark.sv;

import com.github.lindenb.jbwa.jni.AlnRgn;
import com.github.lindenb.jbwa.jni.ShortRead;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.fastq.FastqRecord;
import htsjdk.samtools.util.SequenceUtil;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.datasources.ReferenceWindowFunctions;
import org.broadinstitute.hellbender.engine.spark.SparkContextFactory;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.IndexedAlleleList;
import org.broadinstitute.hellbender.utils.genotyper.IndexedSampleList;
import org.broadinstitute.hellbender.utils.genotyper.LikelihoodMatrix;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.broadinstitute.hellbender.tools.spark.sv.BreakpointAllele.InversionType;
import static org.broadinstitute.hellbender.tools.spark.sv.SVJunction.InversionJunction;

public final class SVJunctionUnitTest extends BaseTest{

    private static JavaSparkContext ctx;

    private ReferenceMultiSource referenceMultiSource;

    private List<VariantContext> vcs;
    private List<InversionJunction> inversions;

    private final Map<Long, List<LocalAssemblyContig>> assemblyID2assembleContents = new HashMap<>(); // empty because currently this is not used

    @BeforeClass
    private void setupSparkAndTestFile(){
        SparkContextFactory.enableTestSparkContext();
        ctx = SparkContextFactory.getTestSparkContext(Collections.emptyMap());

        referenceMultiSource = new ReferenceMultiSource((PipelineOptions)null, new File(b37_reference_20_21).getAbsolutePath(), ReferenceWindowFunctions.IDENTITY_FUNCTION);

        final String inputVCF = new File("src/test/resources/org/broadinstitute/hellbender/tools/spark/sv/SingleDiploidSampleBiallelicSVGenotyperSpark").getAbsolutePath() + "/inversions.vcf";
        try(final VCFFileReader reader = new VCFFileReader(new File(inputVCF), false)){
            vcs = Utils.stream(reader).collect(Collectors.toList());
            inversions = vcs.stream().map(vc -> SVJunction.convertToSVJunction(vc, ctx.broadcast(assemblyID2assembleContents), ctx.broadcast(referenceMultiSource))).map(obj -> (InversionJunction)obj).collect(Collectors.toList());
        }

    }

    // set up test data
    private static ReadLikelihoods<SVDummyAllele> readPostprocessingTestDataBuilder(final InversionJunction junction, final List<GATKRead> reads) {

        reads.addAll(IntStream.range(1, 5).mapToObj(i -> {
            final GATKRead read = ArtificialReadUtils.createRandomRead(151); read.setName("read_"+i); return read;
        }).collect(Collectors.toList()));

        final Map<String, List<GATKRead>> sample2Reads = new HashMap<>();
        sample2Reads.put(SingleDiploidSampleBiallelicSVGenotyperSpark.testSampleName, reads);

        final ReadLikelihoods<SVDummyAllele> rll = new ReadLikelihoods<>(new IndexedSampleList(SingleDiploidSampleBiallelicSVGenotyperSpark.testSampleName),
                new IndexedAlleleList<>(junction.getAlleles()), sample2Reads);

        final int alleleCnt = junction.getAlleles().size();

        // copied from ReadLikelihoodsUnitTest.java with modification of Allele type
        Utils.resetRandomGenerator();
        final Random rnd = Utils.getRandomGenerator();
        final double[][] likelihoods = new double[alleleCnt][reads.size()];
        final LikelihoodMatrix<SVDummyAllele> sampleLikelihoods = rll.sampleMatrix(0);
        for (int a = 0; a < alleleCnt; a++) {
            for (int r = 0; r < likelihoods[0].length; r++)
                sampleLikelihoods.set(a,r,likelihoods[a][r] = -1.0-Math.abs(rnd.nextGaussian()));
        }
        sampleLikelihoods.set(0, 0, likelihoods[0][0] = -0.1 + rnd.nextGaussian()*0.01);
        sampleLikelihoods.set(1, 1, likelihoods[1][1] = -0.1 + rnd.nextGaussian()*0.01);
        sampleLikelihoods.set(2, 2, likelihoods[2][2] = -0.1 + rnd.nextGaussian()*0.01);
        sampleLikelihoods.set(3, 3, likelihoods[3][3] = -0.1 + rnd.nextGaussian()*0.01);

        return rll;
    }

    @Test
    public void testGetAssociatedContigs(){
        final AlignmentRegion expected = new AlignmentRegion("1", "contig-0", TextCigarCodec.decode("151M"), true, new SimpleInterval("20", 1000000, 1000000), 60, 1, 151, 0);
        final LocalAssemblyContig onezero = new LocalAssemblyContig(1L, "contig-0", "AAA", new ArrayList<>(Collections.singletonList(expected)));
        final LocalAssemblyContig oneone = new LocalAssemblyContig(1L, "contig-1", "CCC");
        final LocalAssemblyContig onetwo = new LocalAssemblyContig(1L, "contig-2", "GGG");
        final LocalAssemblyContig onethree = new LocalAssemblyContig(1L, "contig-3", "TTT");

        final LocalAssemblyContig twozero = new LocalAssemblyContig(2L, "contig-0", "TTT");
        final LocalAssemblyContig twoone = new LocalAssemblyContig(2L, "contig-1", "GGG");
        final LocalAssemblyContig twotwo = new LocalAssemblyContig(2L, "contig-2", "CCC");
        final LocalAssemblyContig twothree = new LocalAssemblyContig(2L, "contig-3", "AAA");

        // first construct input map
        final Map<Long, List<LocalAssemblyContig>> inputMap = new HashMap<>();
        inputMap.put(1L, Arrays.asList(onezero, oneone, onetwo, onethree));
        inputMap.put(2L, Arrays.asList(twozero, twoone, twotwo, twothree)); // opposite order of 1L

        // second construct input list
        final List<Tuple2<Long, String>> inputList = Arrays.asList(new Tuple2<>(1L, "contig-0"), new Tuple2<>(2L, "contig-1"), new Tuple2<>(2L, "contig-2"), new Tuple2<>(2L, "contig-0"));

        final List<LocalAssemblyContig> tobeTested = SVJunction.getAssociatedContigs(inputMap, inputList);

        Assert.assertEquals(tobeTested.size(), 4);

        Assert.assertTrue(tobeTested.containsAll(Arrays.asList(onezero, twozero, twoone, twotwo)));
    }

    @Test
    public void testInversionEquals(){
        final InversionJunction invOne = inversions.get(0);
        final InversionJunction invTwo = inversions.get(1);
        Assert.assertNotEquals(invOne, invTwo);
    }

    @Test
    public void testInversionHashcode(){
        final InversionJunction invOne = inversions.get(0);
        final InversionJunction invTwo = inversions.get(1);
        Assert.assertNotEquals(invOne.hashCode(), invTwo.hashCode());
    }

    @Test
    public void testInversionConstructReferenceWindows(){
        final int readL = SingleDiploidSampleBiallelicSVGenotyperSpark.readLength;

        inversions.forEach( inv -> {
            int flank = inv.breakpointAnnotations.maybeNullHomology == null ? 0 : inv.breakpointAnnotations.maybeNullHomology.length;

            Tuple2<SimpleInterval, SimpleInterval> refWindows = inv.getReferenceWindows();
            Assert.assertEquals(refWindows._1().getContig(), refWindows._2().getContig());
            Assert.assertEquals(inv.getLeftAlignedBreakpointLocations().get(0).getStart() - refWindows._1().getStart(), readL-1);
            Assert.assertEquals(refWindows._1().getEnd() - inv.getLeftAlignedBreakpointLocations().get(0).getStart(), readL + flank * (inv.invType== InversionType.INV_NONE ? 0 : 1) - 2);
            Assert.assertEquals(inv.getLeftAlignedBreakpointLocations().get(1).getStart() - refWindows._2().getStart(), readL + flank * (inv.invType== InversionType.INV_NONE ? 0 : 1) - 1);
            Assert.assertEquals(refWindows._2().getEnd() - inv.getLeftAlignedBreakpointLocations().get(1).getStart(), readL-2);
        });
    }

    @Test
    public void testInversionConstructRefAlleles(){

        try (final ContigAligner contigAligner = new ContigAligner(b37_reference_20_21)) {
            inversions.forEach( inv -> {
                final List<SVDummyAllele> alleles = inv.getAlleles();

                testRefAllelesByAlignment(alleles.get(0), true, inv, contigAligner);
                testRefAllelesByAlignment(alleles.get(1), false, inv, contigAligner);
                Assert.assertEquals(alleles.get(0).length(), alleles.get(1).length());
            });
        }catch (final IOException e) {
            throw new GATKException("Cannot run BWA-MEM", e);
        }
    }

    public void testRefAllelesByAlignment(final SVDummyAllele allele, final boolean is5Side, final InversionJunction inv, final ContigAligner contigAligner){
        try{
            final int offsetTobeTurnedOffWhenJBWAIsFixed = 1;
            final byte[] bases = allele.getBases();
            final byte[] qual = new byte[bases.length];
            final ShortRead alleleRead = new ShortRead(inv.getOriginalVC().getID(), bases, qual);
            final AlnRgn[] alnRgns = contigAligner.bwaMem.align(alleleRead);
            Assert.assertTrue(alnRgns.length!=0);
            Assert.assertEquals(alnRgns[0].getPos()+offsetTobeTurnedOffWhenJBWAIsFixed, is5Side ? inv.getReferenceWindows()._1().getStart() : inv.getReferenceWindows()._2().getStart() );
            Assert.assertEquals(alnRgns[0].getCigar(), String.valueOf(bases.length)+"M");
        } catch (final IOException e) {
            throw new GATKException("Cannot run BWA-MEM", e);
        }
    }

    @Test
    public void testInversionConstructAltAlleles(){
        inversions.forEach( inv -> {
            final List<SVDummyAllele> alleles = inv.getAlleles();
            final int ins = inv.breakpointAnnotations.maybeNullInsertedSeq==null?  0 : inv.breakpointAnnotations.maybeNullInsertedSeq.length;
            testAltAlleles(inv, alleles.get(2), true);
            testAltAlleles(inv, alleles.get(3), false);
            Assert.assertEquals(alleles.get(2).length()-ins, alleles.get(0).length());
        });
    }

    public void testAltAlleles(final InversionJunction inv, final SVDummyAllele allele, final boolean is5Side){

        final int hom = inv.breakpointAnnotations.maybeNullHomology==null ? 0 : inv.breakpointAnnotations.maybeNullHomology.length;
        final int ins = inv.breakpointAnnotations.maybeNullInsertedSeq==null ? 0 : inv.breakpointAnnotations.maybeNullInsertedSeq.length;
        Assert.assertEquals(allele.length(), SingleDiploidSampleBiallelicSVGenotyperSpark.readLength*2+ins+hom-2);

        if(is5Side){
            // test first "half" of the alt allele is the same as the first "half" of the left ref allele
            byte[] s = Arrays.copyOfRange(inv.getAlleles().get(0).getBases(), 0, SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1);
            final int distL = StringUtils.getLevenshteinDistance(new String(Arrays.copyOfRange(allele.getBases(), 0, SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1)),
                                                                 new String(s));
            Assert.assertEquals(distL, 0);

            // test second "half" of the alt allele is the same as RC of the first "half" of the right ref allele
                   s = Arrays.copyOfRange(inv.getAlleles().get(1).getBases(), 0, SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1);
            SequenceUtil.reverseComplement(s);
            final int distR = StringUtils.getLevenshteinDistance(new String(Arrays.copyOfRange(allele.getBases(), SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1 + hom + ins, allele.length())), // skip insertion and homology
                                                                 new String(s));
            Assert.assertEquals(distR, 0);
        } else {
            // test first "half" of the alt allele is the same as the second "half" of the left ref allele
            SVDummyAllele a = inv.getAlleles().get(0);
            byte[] s = Arrays.copyOfRange(a.getBases(), SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1+hom, a.length());                                                                        // skip homology in ref
            SequenceUtil.reverseComplement(s);
            final int distL = StringUtils.getLevenshteinDistance(new String(Arrays.copyOfRange(allele.getBases(), 0, SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1)),
                                                                 new String(s));
            Assert.assertEquals(distL, 0);

            // test second "half" of the alt allele is the same as the second "half" of the right ref allele
            a = inv.getAlleles().get(1);
            s = Arrays.copyOfRange(a.getBases(), SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1+hom, a.length());
            final int distR = StringUtils.getLevenshteinDistance(new String(Arrays.copyOfRange(allele.getBases(), SingleDiploidSampleBiallelicSVGenotyperSpark.readLength-1 + hom + ins, allele.length())), // skip insertion and homology
                                                                 new String(s));
            Assert.assertEquals(distR, 0);
        }
    }

    @Test
    public void testReadSuitableForGenotypingJunction(){

        final InversionJunction junction = inversions.get(0);

        final String fakeSeq = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        final String fakeQual = "#######################################################################################################################################################";
        final GATKRead fastqRead = SVFastqUtils.convertToRead(new FastqRecord("11259L mapping=unmapped", fakeSeq, "+", fakeQual));

        Assert.assertTrue(junction.readSuitableForGenotyping(fastqRead));

        final GATKRead unmappedRead = ArtificialReadUtils.createRandomRead(151); unmappedRead.setName("unmappedRead"); unmappedRead.setIsUnmapped(); unmappedRead.setIsSecondOfPair();
        Assert.assertFalse( junction.readResideInJunctionWindow(unmappedRead) );

        final GATKRead wrongChr = ArtificialReadUtils.createRandomRead(151);wrongChr.setName("wrongChr"); wrongChr.setPosition("21", 27374100);wrongChr.setIsFirstOfPair();
        Assert.assertFalse( junction.readResideInJunctionWindow(wrongChr) );

        final GATKRead tooFarLeft = ArtificialReadUtils.createRandomRead(151);tooFarLeft.setName("tooFarLeft");tooFarLeft.setPosition("20", 26210700); tooFarLeft.setIsFirstOfPair(); // too far left
        Assert.assertFalse( junction.readResideInJunctionWindow(tooFarLeft) );

        final GATKRead tooFarRight = ArtificialReadUtils.createRandomRead(151);tooFarRight.setName("tooFarRight"); tooFarRight.setPosition("20", 26210908);tooFarRight.setIsSecondOfPair(); // too far right
        Assert.assertFalse( junction.readResideInJunctionWindow(tooFarRight) );

        final GATKRead correctRead = ArtificialReadUtils.createRandomRead(151); correctRead.setName("correctRead"); correctRead.setPosition("20", 26210857); correctRead.setIsFirstOfPair();
        Assert.assertTrue( junction.readResideInJunctionWindow(correctRead) );

    }

    @Test
    public void testConvertToSVJunction(){

        Assert.assertEquals(inversions.size(), 5);

        final InversionJunction junction =  inversions.get(0);

        // test only one element, not all
        Assert.assertEquals(junction.getOriginalVC(), vcs.get(0));
        Assert.assertEquals(junction.getOriginalVC(), vcs.get(0));
        Assert.assertEquals(junction.type, GATKSVVCFHeaderLines.SVTYPES.INV);
        Assert.assertEquals(junction.fiveEndBPLoc, new SimpleInterval("20", 26210907, 26210907));
        Assert.assertEquals(junction.threeEndBPLoc, new SimpleInterval("20", 26211109, 26211109));
        Assert.assertEquals(junction.svLength, 202);
        Assert.assertEquals(junction.evidenceAnnotations.assemblyIDs, Arrays.asList(5718L, 7161L));
        Assert.assertEquals(junction.evidenceAnnotations.contigIDs, Arrays.asList(new Tuple2<>(5718L, "contig-7"), new Tuple2<>(7161L, "contig-3")));
        Assert.assertEquals(junction.evidenceAnnotations.numHQMappings, 2);
        Assert.assertEquals(junction.evidenceAnnotations.maxAlignLength, 81);
        Assert.assertNull(junction.breakpointAnnotations.maybeNullHomology);
        Assert.assertEquals(junction.breakpointAnnotations.maybeNullInsertedSeq, "CACACACACACGTAGCCTCATAATACCATATATATATA".getBytes());
        Assert.assertEquals(junction.invType, BreakpointAllele.InversionType.INV_5_TO_3);
    }

    @Test
    public void testUpdateReads(){

        final InversionJunction junction = inversions.get(0);
        // set up test data
        final List<GATKRead> reads = new ArrayList<>();
        final ReadLikelihoods<SVDummyAllele> input = readPostprocessingTestDataBuilder(junction, reads);

        // because normalization, filtering and marginalization are all tested, we simply test the end output
        final ReadLikelihoods<SVDummyAllele> output = junction.updateReads(input);
        Assert.assertEquals(output.samples().size(), 1);
        Assert.assertEquals(output.samples().get(0), SingleDiploidSampleBiallelicSVGenotyperSpark.testSampleName);

        final LikelihoodMatrix<SVDummyAllele> mat = output.sampleMatrix(0);
        Assert.assertEquals(mat.alleles(), junction.getGenotypedVC().getAlleles()); // test alleles

        Assert.assertEquals(mat.reads().size(), 4); // no reads filtered out because test data is designed to be well modelled
        final List<Double> firstRow = Arrays.asList(mat.get(0, 0), mat.get(0, 1), mat.get(0, 2), mat.get(0, 3));
        Assert.assertTrue(firstRow.indexOf(Collections.max(firstRow))<2);
        final List<Double> secondRow = Arrays.asList(mat.get(1, 0), mat.get(1, 1), mat.get(1, 2), mat.get(1, 3));
        Assert.assertTrue(secondRow.indexOf(Collections.max(secondRow))>1);
        Assert.assertTrue(mat.get(0, 2) < -1.0);
        Assert.assertTrue(mat.get(0, 3) < -1.0);
        Assert.assertTrue(mat.get(1, 0) < -1.0);
        Assert.assertTrue(mat.get(1, 1) < -1.0);
    }
}
