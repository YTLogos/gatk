package org.broadinstitute.hellbender.tools.spark.pipelines;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.testng.annotations.Test;

import java.io.File;

public final class GetHetPulldownSparkIntegrationTest extends CommandLineProgramTest {
//    private static final String TEST_SUB_DIR = publicTestDir + "org/broadinstitute/hellbender/tools/exome/";
//
//    private static final File NORMAL_BAM_FILE = new File(TEST_SUB_DIR + "normal.sorted.bam");
//    private static final File TUMOR_BAM_FILE = new File(TEST_SUB_DIR + "tumor.sorted.bam");
//    private static final File SNP_FILE = new File(TEST_SUB_DIR + "common_SNP.interval_list");
//    private static final File REF_FILE = new File(hg19MiniReference);

    private static final String TEST_SUB_DIR = "/home/slee/working/hellbender-protected/alleliccapsegger/";

    private static final File NORMAL_BAM_FILE = new File(TEST_SUB_DIR + "HCC1143BL_chr22_27M_37M.actual.bam");
    private static final File TUMOR_BAM_FILE = new File(TEST_SUB_DIR + "HCC1143_chr22_27M_37M.actual.bam");
    private static final File SNP_FILE = new File(TEST_SUB_DIR + "common_SNPs.chr22_27Mto37M.interval_list");
    private static final File REF_FILE = new File(TEST_SUB_DIR + "Homo_sapiens_assembly19.fasta");

    @Test
    public void testGetHetCoverageSpark() {
        final File normalOutputFile = createTempFile("normal-test", ".txt");
//        final File normalSNPFile = createTempFile("normal-test-snp", ".txt");
//        final File tumorOutputFile = createTempFile("tumor-test", ".txt");

        final String[] normalArguments = {
                "-" + GetHetPulldownSpark.MODE_SHORT_NAME, GetHetPulldownSpark.NORMAL_MODE_ARGUMENT,
                "-" + StandardArgumentDefinitions.INPUT_SHORT_NAME, NORMAL_BAM_FILE.getAbsolutePath(),
                "-" + "snp", SNP_FILE.getAbsolutePath(),
                "-" + StandardArgumentDefinitions.REFERENCE_SHORT_NAME, REF_FILE.getAbsolutePath(),
                "-" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME, normalOutputFile.getAbsolutePath(),
        };
        runCommandLine(normalArguments);

//        final String[] tumorArguments = {
//                "-" + GetHetPulldownSpark.MODE_SHORT_NAME, GetHetPulldownSpark.TUMOR_MODE_ARGUMENT,
//                "-" + StandardArgumentDefinitions.INPUT_SHORT_NAME, TUMOR_BAM_FILE.getAbsolutePath(),
//                "-" + "snp", normalSNPFile.getAbsolutePath(),
//                "-" + StandardArgumentDefinitions.REFERENCE_SHORT_NAME, REF_FILE.getAbsolutePath(),
//                "-" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME, tumorOutputFile.getAbsolutePath(),
//        };
//        runCommandLine(tumorArguments);
    }
}
