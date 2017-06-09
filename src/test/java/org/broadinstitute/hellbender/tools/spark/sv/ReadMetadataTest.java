package org.broadinstitute.hellbender.tools.spark.sv;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.tools.spark.utils.IntHistogram;
import org.broadinstitute.hellbender.utils.IntHistogramTest;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.Set;

public class ReadMetadataTest extends BaseTest {
    private final static IntHistogram.CDF cdf = IntHistogramTest.genLogNormalSample(400, 175, 10000).getCDF();

    @Test(groups = "spark")
    void testEverything() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithGroups(2, 1, 10000000, 1);
        final String chr1Name = header.getSequenceDictionary().getSequence(0).getSequenceName();
        final String chr2Name = header.getSequenceDictionary().getSequence(1).getSequenceName();
        final String groupName = header.getReadGroups().get(0).getReadGroupId();
        final Set<Integer> crossContigIgnoreSet = new HashSet<>(3);
        crossContigIgnoreSet.add(1);
        final ReadMetadata readMetadata = new ReadMetadata(crossContigIgnoreSet, header, cdf, null, 1L, 1L, 1);
        Assert.assertEquals(readMetadata.getContigID(chr1Name), 0);
        Assert.assertEquals(readMetadata.getContigID(chr2Name), 1);
        Assert.assertFalse(readMetadata.ignoreCrossContigID(0));
        Assert.assertTrue(readMetadata.ignoreCrossContigID(1));
        Assert.assertThrows(() -> readMetadata.getContigID("not a real name"));
        Assert.assertEquals(readMetadata.getLibraryStatistics(readMetadata.getLibraryName(groupName)), cdf);
        Assert.assertThrows(() -> readMetadata.getLibraryName("not a real name"));
    }

    @Test(groups = "spark")
    void serializationTest() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithGroups(1, 1, 10000000, 1);
        final Set<Integer> crossContigIgnoreSet = new HashSet<>(3);
        crossContigIgnoreSet.add(0);
        final ReadMetadata readMetadata =
                new ReadMetadata(crossContigIgnoreSet, header, cdf, new ReadMetadata.PartitionBounds[0], 1L, 1L, 1);

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final Output out = new Output(bos);
        final Kryo kryo = new Kryo();
        kryo.writeClassAndObject(out, readMetadata);
        out.flush();

        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        final Input in = new Input(bis);
        final ReadMetadata readMetadata2 = (ReadMetadata)kryo.readClassAndObject(in);
        Assert.assertEquals(readMetadata.getCrossContigIgnoreSet(), readMetadata2.getCrossContigIgnoreSet());
        Assert.assertEquals(readMetadata.getContigNameMap(), readMetadata2.getContigNameMap());
        Assert.assertEquals(readMetadata.getReadGroupToLibraryMap(), readMetadata2.getReadGroupToLibraryMap());
        Assert.assertEquals(readMetadata.getNReads(), readMetadata2.getNReads());
        Assert.assertEquals(readMetadata.getMaxReadsInPartition(), readMetadata2.getMaxReadsInPartition());
        Assert.assertEquals(readMetadata.getCoverage(), readMetadata2.getCoverage());
        Assert.assertEquals(readMetadata.getAllPartitionBounds(), readMetadata2.getAllPartitionBounds());
        Assert.assertEquals(readMetadata.getAllLibraryStatistics().keySet(), readMetadata2.getAllLibraryStatistics().keySet());
        for ( final String readGroupName : readMetadata.getReadGroupToLibraryMap().keySet() ) {
            final ReadMetadata.ZCalc zCalc1 = readMetadata.getZCalc(readGroupName);
            final ReadMetadata.ZCalc zCalc2 = readMetadata2.getZCalc(readGroupName);
            Assert.assertEquals(zCalc1.getMedian(),zCalc2.getMedian());
            Assert.assertEquals(zCalc1.getNegativeMAD(),zCalc2.getNegativeMAD());
            Assert.assertEquals(zCalc1.getPositiveMAD(),zCalc2.getPositiveMAD());
        }
    }
}
