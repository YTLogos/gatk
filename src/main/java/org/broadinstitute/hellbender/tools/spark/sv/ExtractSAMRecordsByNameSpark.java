package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

/**
 * Created by shuang on 3/3/17.
 */
@CommandLineProgramProperties(summary="Find reads that have the requested read names.",
        oneLineSummary="Dump reads that have the requested read names.",
        programGroup = StructuralVariationSparkProgramGroup.class)
public final class ExtractSAMRecordsByNameSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "file containing list of read names", fullName = "readNameFile")
    private String readNameFile;

    @Argument(doc = "file to write reads to", fullName = "outputFileName")
    private String outputFileName;

    @Override
    public boolean requiresReads() {
        return true;
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {

        final Broadcast<HashSet<String>> namesToLookForBroadcast = ctx.broadcast(parseReadNames());
        final Broadcast<SAMFileHeader>           headerBroadCast = ctx.broadcast(getHeaderForReads());

        final JavaRDD<SAMRecord> reads = getReads().repartition(80)
                                                   .filter(read -> namesToLookForBroadcast.getValue().contains(read.getName()))
                                                   .map(read -> read.convertToSAMRecord(headerBroadCast.getValue())).cache();
        logger.info("Found these many reads: " + reads.count());

        reads.map(SAMRecord::getSAMString).coalesce(1).saveAsTextFile(outputFileName);
    }

    private HashSet<String> parseReadNames() {
        final HashSet<String> namesToLookFor = new HashSet<>();

        try ( final BufferedReader rdr =
                      new BufferedReader(new InputStreamReader(BucketUtils.openFile(readNameFile))) ) {
            String line;
            while ( (line = rdr.readLine()) != null ) {
                namesToLookFor.add(line.replace("@", "")
                                       .replace("/1", "")
                                       .replace("/2", ""));
            }
        }
        catch ( final IOException ioe ) {
            throw new GATKException("Unable to read names file from "+readNameFile, ioe);
        }
        logger.info("Number of read names: " + namesToLookFor.size());
        return namesToLookFor;
    }
}
