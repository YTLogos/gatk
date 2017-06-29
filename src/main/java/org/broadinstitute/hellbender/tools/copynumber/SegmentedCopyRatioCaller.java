package org.broadinstitute.hellbender.tools.copynumber;

import org.apache.logging.log4j.Level;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hdf5.HDF5File;
import org.broadinstitute.hdf5.HDF5Library;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.CopyNumberProgramGroup;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.exome.*;
import org.broadinstitute.hellbender.tools.pon.coverage.pca.HDF5PCACoveragePoN;
import org.broadinstitute.hellbender.tools.pon.coverage.pca.PCACoveragePoN;
import org.broadinstitute.hellbender.tools.pon.coverage.pca.PCATangentNormalizationResult;
import org.broadinstitute.hellbender.utils.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * //TODO SL
 */
@CommandLineProgramProperties(
        summary = "Denoise read counts for a case sample using a panel of normals and call segmented copy-ratio events",
        oneLineSummary = "Denoise read counts for a case sample using a panel of normals and call segmented copy-ratio events",
        programGroup = CopyNumberProgramGroup.class
)
@DocumentedFeature
public final class SegmentedCopyRatioCaller extends CommandLineProgram {

    public static final String READ_COUNTS_FILE_FULL_NAME = StandardArgumentDefinitions.INPUT_LONG_NAME;
    public static final String READ_COUNTS_FILE_SHORT_NAME = StandardArgumentDefinitions.INPUT_SHORT_NAME;

    @Argument(
            doc = "Input read counts for the case sample.",
            shortName = READ_COUNTS_FILE_SHORT_NAME,
            fullName = READ_COUNTS_FILE_FULL_NAME,
            optional = false
    )
    protected File readCountsFile;

    @Argument(
            doc = "Panel of normals HDF5 file (output of CreatePanelOfNormals).  " +
                    "Genomic intervals must match those of the case sample.",
            shortName = CopyNumberStandardArgumentDefinitions.PON_FILE_SHORT_NAME,
            fullName = CopyNumberStandardArgumentDefinitions.PON_FILE_LONG_NAME,
            optional = false
    )
    protected File ponFile;

    @Argument(
            doc = "Output PCA-denoised read counts for the case sample.",
            shortName = CopyNumberStandardArgumentDefinitions.TANGENT_NORMALIZED_COUNTS_FILE_SHORT_NAME,
            fullName = CopyNumberStandardArgumentDefinitions.TANGENT_NORMALIZED_COUNTS_FILE_LONG_NAME,
            optional = false
    )
    protected File tangentNormalizationOutFile;

    @Argument(
            doc = "Output median-denoised read counts for the case sample.",
            shortName = CopyNumberStandardArgumentDefinitions.PRE_TANGENT_NORMALIZED_COUNTS_FILE_SHORT_NAME,
            fullName = CopyNumberStandardArgumentDefinitions.PRE_TANGENT_NORMALIZED_COUNTS_FILE_LONG_NAME,
            optional = true
    )
    protected File preTangentNormalizationOutFile;

    @Argument(
            doc = "Tangent normalization Beta Hats output file",
            shortName = TANGENT_BETA_HATS_SHORT_NAME,
            fullName = TANGENT_BETA_HATS_LONG_NAME,
            optional = true
    )
    protected File betaHatsOutFile;

    @Argument(
            doc = "Factor normalized counts output",
            shortName = FACTOR_NORMALIZED_COUNTS_SHORT_NAME,
            fullName = FACTOR_NORMALIZED_COUNTS_LONG_NAME,
            optional = true
    )
    protected File fntOutFile;

    @Override
    protected Object doWork() {
        if (! new HDF5Library().load(null)){ //Note: passing null means using the default temp dir.
            throw new UserException.HardwareFeatureException("Cannot load the required HDF5 library. " +
                    "HDF5 is currently supported on x86-64 architecture and Linux or OSX systems.");
        }
        IOUtils.canReadFile(ponFile);
        try (final HDF5File hdf5PoNFile = new HDF5File(ponFile)) {
            final PCACoveragePoN pon = new HDF5PCACoveragePoN(hdf5PoNFile, logger);
            final TargetCollection<Target> targetCollection = readTargetCollection(targetFile);
            final ReadCountCollection proportionalCoverageProfile = readInputReadCounts(readCountsFile, targetCollection);
            final PCATangentNormalizationResult tangentNormalizationResult = pon.normalize(proportionalCoverageProfile);;
            tangentNormalizationResult.write(getCommandLine(), tangentNormalizationOutFile, preTangentNormalizationOutFile, betaHatsOutFile, fntOutFile);
            return "SUCCESS";
        }
    }

    /**
     * Reads the target collection from a file.
     * @param targetFile the input target file.
     * @return never {@code null}.
     */
    private TargetCollection<Target> readTargetCollection(final File targetFile) {
        if (targetFile == null) {
            return null;
        } else {
            IOUtils.canReadFile(targetFile);
            logger.log(Level.INFO, String.format("Reading target intervals from exome file '%s' ...", new Object[]{targetFile.getAbsolutePath()}));
            final List<Target> targets = TargetTableReader.readTargetFile(targetFile);
            return new HashedListTargetCollection<>(targets);
        }
    }

    /**
     * Reads the read-counts from the input file using a target collection (if provided) to resolve target names if this
     * are missing.
     *
     * Even though the tangent normalization code works for multiple samples, our workflows are designed for single samples
     * and so we enforce it here.
     *
     * @param readCountsFile the read-counts file.
     * @param targetCollection the input target collection. {@code null} indicates that no collection was provided by the user.
     * @return never {@code null}.
     * @throws UserException.CouldNotReadInputFile if there was some problem
     *                                             trying to read from {@code inputReadCountsFile}.
     * @throws UserException.BadInput              if there is some format issue with the input file {@code inputReadCountsFile},
     *                                             or it does not contain target names and {@code targetCollection} is {@code null}
     *                                             or the input read counts contain more thanb one sample.
     */
    private ReadCountCollection readInputReadCounts(final File readCountsFile,
                                                    final TargetCollection<Target> targetCollection) {
        try {
            final ReadCountCollection result = ReadCountCollectionUtils.parse(readCountsFile, targetCollection, false);
            if (result.columnNames().size() > 1) {
                throw new UserException.BadInput("Only single-sample input read counts are allowed");
            }
            return result;
        } catch (final IOException ex) {
            throw new UserException.CouldNotReadInputFile(readCountsFile, ex.getMessage(), ex);
        }
    }
}
