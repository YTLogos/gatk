package org.broadinstitute.hellbender.utils.baq;


import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.SAMUtils;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.hellbender.engine.ReferenceDataSource;
import org.broadinstitute.hellbender.engine.ReferenceMemorySource;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic unit test for BAQ calculation
 */
public class BAQUnitTest extends BaseTest {
    private SAMFileHeader header;
    private final int startChr = 1;
    private final int numChr = 2;
    private final int chrSize = 1000;
    ReferenceDataSource fasta = null;

    @BeforeMethod
    public void before() {
        header = ArtificialReadUtils.createArtificialSamHeader(numChr, startChr, chrSize);
//        fasta = new ReferenceFileSource( new File(b37_reference_20_21));
        final ReferenceBases bases= new ReferenceBases(new byte[10000], new SimpleInterval("1", 1, 10000));
        List<SAMSequenceRecord> list= new ArrayList<>(1);
        list.add(new SAMSequenceRecord("1", 10000));
        SAMSequenceDictionary dict= new SAMSequenceDictionary(list);
        fasta = new ReferenceMemorySource(bases, dict);
    }

    private class BAQTest {
        String readBases, refBases;
        byte[] quals, expected;
        String cigar;
        int refOffset;
        int pos;

        public BAQTest(String _refBases, String _readBases, String _quals, String _expected) {
            this(0, -1, null, _readBases, _refBases, _quals, _expected);
        }

        public BAQTest(int refOffset, String _refBases, String _readBases, String _quals, String _expected) {
            this(refOffset, -1, null, _refBases, _readBases, _quals, _expected);
        }

        public BAQTest(long pos, String cigar, String _readBases, String _quals, String _expected) {
            this(0, pos, cigar, null, _readBases, _quals, _expected);
        }


        public BAQTest(int _refOffset, long _pos, String _cigar, String _refBases, String _readBases, String _quals, String _expected) {
            refOffset = _refOffset;
            pos = (int)_pos;
            cigar = _cigar;
            readBases = _readBases;
            refBases = _refBases;

            quals = new byte[_quals.getBytes().length];
            expected = new byte[_quals.getBytes().length];
            for ( int i = 0; i < quals.length; i++) {
                quals[i] = (byte)(_quals.getBytes()[i] - 33);
                expected[i] = (byte)(_expected.getBytes()[i] - 33);
            }
        }

        public String toString() { return readBases; }

        public GATKRead createRead() {
            GATKRead read = ArtificialReadUtils.createArtificialRead(header, "foo", 0, pos > 0 ? pos + (refOffset > 0 ? refOffset : 0): 1, readBases.getBytes(), quals);
            //if ( cigar != null ) read.setAlignmentEnd(readBases.getBytes().length + pos);
            read.setCigar( cigar == null ? String.format("%dM", quals.length) : cigar);
            return read;
        }
    }


    @DataProvider(name = "data")
    public Object[][] createData1() {
        List<BAQTest> params = new ArrayList<>();

        params.add(new BAQTest("GCTGCTCCTGGTACTGCTGGATGAGGGCCTCGATGAAGCTAAGCTTTTTCTCCTGCTCCTGCGTGATCCGCTGCAG",
                               "GCTGCTCCTGGTACTGCTGGATGAGGGCCTCGATGAAGCTAAGCTTTTCCTCCTGCTCCTGCGTGATCCGCTGCAG",
                               "?BACCBDDDFFBCFFHHFIHFEIFHIGHHGHBFEIFGIIGEGIIHGGGIHHIIHIIHIIHGICCIGEII@IGIHCG",
                               "?BACCBDDDFFBCFFHHFIHFEIFHIGHHGHBFEIFGIIGEGII410..0HIIHIIHIIHGICCIGEII@IGIHCE"));

        params.add(new BAQTest("GCTTTTTCTCCTCCTG",
                               "GCTTTTCCTCCTCCTG",
                               "IIHGGGIHHIIHHIIH",
                               "EI410..0HIIHHIIE"));

        // big and complex, also does a cap from 3 to 4!
        params.add(new BAQTest(-3, 810l, "49M1I126M1I20M1I25M",
                                "AAATTCAAGATTTCAAAGGCTCTTAACTGCTCAAGATAATTTTTTTTTTTTGAGACAGAGTCTTGCTGTGTTGCCCAGGCTGGAGTGCAGTGGCGTGATCTTGGCTCACTGCAAGCTCCGCCTCCCGGGTTCACGCCATTCTCCTGCCTCAGCCTCCCGAGTAGCTGGGACTACAGGCACCCACCACCACGCCTGGCCAATTTTTTTGTATTTTTAGTAGAGATAG",
                                "TTCAAGATTTCAAAGGCTCTTAACTGCTCAAGATAATTTTTTTTTTTTGTAGACAGAGTCTTGCTGTGTTGCCCAGGCTGGAGTGCAGTGGCGTGATCTTGGCTCACTGCAAGCTCCGCCTCCCGGGTTCACGCCATTCTCCTGCCTCAGCCTCCCGAGTAGCTGGGACTACAGGCCACCCACCACCACGCCTGGCCTAATTTTTTTGTATTTTTAGTAGAGA",
                                ">IHFECEBDBBCBCABABAADBD?AABBACEABABC?>?B>@A@@>A?B3BBC?CBDBAABBBBBAABAABBABDACCCBCDAACBCBABBB:ABDBACBBDCCCCABCDCCBCC@@;?<B@BC;CBBBAB=;A>ACBABBBABBCA@@<?>>AAA<CA@AABBABCC?BB8@<@C<>5;<A5=A;>=64>???B>=6497<<;;<;>2?>BA@??A6<<A59",
                                ">EHFECEBDBBCBCABABAADBD?AABBACEABABC?>?B>@A@@>A?838BC?CBDBAABBBBBAABAABBABDACCCBCDAACBCBABBB:ABDBACBBDCCCCABCDCCBCC@@;?<B@BC;CBBBAB=;A>ACBABBBABBCA@@<?>>AAA<CA@AABBABCC?BB8@<@%<>5;<A5=A;>=64>???B;86497<<;;<;>2?>BA@??A6<<A59"));

        // now changes
        params.add(new BAQTest(-3, 966l, "36M",
                                "CCGAGTAGCTGGGACTACAGGCACCCACCACCACGCCTGGCC",
                                "AGTAGCTGGGACTACAGGCACCCACCACCACGCCTG",
                                "A?>>@>AA?@@>A?>A@?>@>>?=>?'>?=>7=?A9",
                                "A?>>@>AA?@@>A?>A@?>@>>?=>?'>?=>7=?A9"));

        // raw base qualities are low -- but they shouldn't be capped
        params.add(new BAQTest(-3, 993l, "4=13X2=3X1=4X2=4X1=2X",
                                "CCACCACGCCTGGCCAATTTTTTTGTATTTTTAGTAGAGATA",
                                "CCACGCTTGGCAAAGTTTTCCGTACGTTTAGCCGAG",
                                "33'/(7+270&4),(&&-)$&,%7$',-/61(,6?8",
                                "33'/(7+270&4),(&&-)$&,%7$',-/61(,6?8"));

        // soft clipping
        // todo soft clip testing just doesn't work right now!

//        params.add(new BAQTest(29, 10000109l, "29S190M",
//                                null, "GAAGGTTGAATCAAACCTTCGGTTCCAACGGATTACAGGTGTGAGCCACCGCGACCGGCCTGCTCAAGATAATTTTTAGGGCTAACTATGACATGAACCCCAAAATTCCTGTCCTCTAGATGGCAGAAACCAAGATAAAGTATCCCCACATGGCCACAAGGTTAAGCTCTTATGGACACAAAACAAGGCAGAGAAATGTCATTTGGCATTGGTTTCAGG",
//                                "3737088:858278273772:3<=;:?;5=9@>@?>@=<>8?>@=>>?>4=5>?=5====A==@?A@=@6@A><?B:A;:;>@A?>?AA>@?AA>A?>==?AAA@@A>=A<A>>A=?A>AA==@A?AA?>?AA?A@@C@:?A@<;::??AA==>@@?BB=<A?BA>>A>A?AB=???@?BBA@?BA==?A>A?BB=A:@?ABAB>>?ABB>8A@BAIGA",
//                                "3737088:858278273772:3<=;:?;5=9@>@?>@=<>8?>@=>>?>4=5>?=5====A==@?A@=@6@A><?B:A;:;>@A?>?AA>@?AA>A?>==?AAA@@A>=A<A>>A=?A>AA==@A?AA?>?AA?A@@C@:?A@<;::??AA==>@@?BB=<A?BA>>A>A?AB=???@?BBA@?BA==?A>A?BB=A:@?ABAB>>?ABB>8A@BAI>;"));

//        params.add(new BAQTest(30, 10000373l, "30S69M1D2M",
//                                null, "TGAAATCCTGCCTTATAGTTCCCCTAAACCCACGTTCTATCCCCAGATACTCCCCTCTTCATTACAGAACAACAAAGAAAGACAAATTCTTAGCATCAATG",
//                                "###############################=89>B;6<;96*>.1799>++66=:=:8=<-.9>><;9<':-+;*+::=;8=;;.::<:;=/2=70<=?-",
//                                "###############################=89>B;6<;96*>.1799>++66=:=:8=<-.9>><;9<':-+;*+::=;8=;;.::<:;=/2=7000%%"));


//        params.add(new BAQTest(5, 10000109l, "5S5M",
//                                "GAAGGTTGAA",
//                                null,
//                                "HHHHHHHHHH",
//                                "HHHHHHHHHE"));

//        params.add(new BAQTest(10009480l, "102M1I18M1I16M1I43M1I10M1D9M1I7M1I7M1I16M1I9M1I8M1I14M2I18M",
//                                "AGAGATGGGGTTTCGCCATGTTGTCCAGGCTGGTCTTGAACTCCTGACCTCAAGTGATCTGCCCACCTCGGCCTCCCAAAGTGCTGGGATTACACGTGTGAAACCACCATGCCTGGTCTCTTAATTTTTCNGATTCTAATAAAATTACATTCTATTTGCTGAAAGNGTACTTTAGAGTTGAAAGAAAAAGAAAGGNGTGGAACTTCCCCTAGTAAACAAGGAAAAACNTCCATGTTATTTATTGGACCTTAAAAATAGTGAAACATCTTAAGAAAAAAAATCAATCCTA",
//                                "@HI@BA<?C@?CA>7>=AA>9@==??C???@?>:?BB@BA>B?=A@@<=B?AB???@@@@@?=?A==B@7<<?@>==>=<=>???>=@@A?<=B:5?413577/675;><;==@=<>>968;6;>????:#;=?>:3072077726/6;3719;9A=9;774771#30532676??=8::97<7144448/4425#65688821515986255/5601548355551#218>96/5/8<4/.2344/914/55553)1047;:30312:4:63556565631=:62610",
//                                "@HI@BA<?C@?CA>7>=AA>9@==??C???@?>:?BB@BA>B?=A@@<=B?AB???@@@@@?=?A==B@7<<?@>==>=<=>???>=@@A?<=B:5?413&!7/675;><;==@=<>>96!;6;>????:#;=?>:3!72077726/6;3719;9A=9;774771#30532676??=8::&!<7144448'$!25#65687421515986255/560!548355551#218>96!5/8<4/.2344/614(%!!53)1047;:30312:4:63556565631=:62610"));

        List<Object[]> params2 = new ArrayList<>();
        for ( BAQTest x : params ) params2.add(new Object[]{x});
        return params2.toArray(new Object[][]{});
    }



    @Test(dataProvider = "data", enabled = true)
    public void testBAQWithProvidedReference(BAQTest test) {
        if ( test.refBases != null ) {
            testBAQ(test, false);
        }
    }

    @Test(dataProvider = "data", enabled = true)
    public void testBAQWithCigarAndRefLookup(BAQTest test) {
        if ( test.cigar != null ) {
            testBAQ(test, true);
        }
    }

    @Test(enabled = true)
    public void testBAQQualRange() {
        BAQ baq = new BAQ(1e-3, 0.1, 7, (byte)4, false);         // matches current samtools parameters
        final byte ref = (byte)'A';
        final byte alt = (byte)'A';

        for (int i = 0; i <= SAMUtils.MAX_PHRED_SCORE; i++ )
            Assert.assertTrue(baq.calcEpsilon( ref, alt, (byte)i) >= 0.0, "Failed to get baq epsilon range");
    }

    @Test
    public void testBAQOverwritesExistingTagWithNull() {

        // create a read with a single base off the end of the contig, which cannot be BAQed
        final GATKRead read = ArtificialReadUtils.createArtificialRead(header, "foo", 0, header.getSequenceDictionary().getSequence(0).getSequenceLength() + 1, 1);
        read.setBases(new byte[] {(byte) 'A'});
        read.setBaseQualities(new byte[] {(byte) 20});
        read.setCigar("1M");
        read.setAttribute("BQ", "A");

        // try to BAQ and tell it to RECALCULATE AND ADD_TAG
        BAQ baq = new BAQ(1e-3, 0.1, 7, (byte)4, false);
        baq.baqRead(read, fasta, BAQ.CalculationMode.RECALCULATE, BAQ.QualityMode.ADD_TAG);

        // did we remove the existing tag?
        Assert.assertFalse(read.hasAttribute("BQ"));
    }

    public void testBAQ(BAQTest test, boolean lookupWithFasta) {
        BAQ baqHMM = new BAQ(1e-3, 0.1, 7, (byte)4, false);         // matches current samtools parameters

        GATKRead read = test.createRead();
        BAQ.BAQCalculationResult result;
        if ( lookupWithFasta && test.cigar != null )
            result = baqHMM.calcBAQFromHMM(read, fasta);
        else
            result = baqHMM.calcBAQFromHMM(read, test.refBases.getBytes(), test.refOffset);

        Assert.assertNotNull(result, "BAQ result null");
        System.out.println(StringUtils.repeat('-', 40));
        System.out.println("reads   : " + new String(test.readBases));
        printQuals(System.out, "in-quals:", test.quals, false);
        printQuals(System.out, "bq-quals:", result.bq, false);
        for (int i = 0; i < test.quals.length; i++) {
            //result.bq[i] = baqHMM.capBaseByBAQ(result.rawQuals[i], result.bq[i], result.state[i], i);
            Assert.assertTrue(result.bq[i] >= baqHMM.getMinBaseQual() || test.expected[i] < baqHMM.getMinBaseQual(), "BQ < min base quality");
            Assert.assertEquals(result.bq[i], test.expected[i], "Did not see the expected BAQ value at " + i);
        }

    }

    public final static void printQuals(PrintStream out, String prefix, byte[] quals, boolean asInt ) {
        out.print(prefix);
        for ( int i = 0; i < quals.length; i++) {
            if ( asInt ) {
                out.printf("%2d", (int)quals[i]);
                if ( i+1 != quals.length ) out.print(",");
            } else
                out.print((char)(quals[i]+33));
        }
        out.println();
    }
}