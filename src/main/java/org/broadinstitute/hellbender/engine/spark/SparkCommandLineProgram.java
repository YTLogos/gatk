package org.broadinstitute.hellbender.engine.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;

import java.io.Serializable;


public abstract class SparkCommandLineProgram extends CommandLineProgram implements Serializable {
    private static final long serialVersionUID = 1l;

    @Argument(doc = "API Key for google cloud authentication",
            shortName = "apiKey", fullName = "apiKey", optional=true, mutex={"client_secret"})
    protected String apiKey = null;


    @Argument(fullName = "sparkMaster", doc="URL of the Spark Master to submit jobs to when using the Spark pipeline runner.", optional = true)
    protected String sparkMaster;

    @Override
    protected Object doWork() {
        final JavaSparkContext ctx = buildContext();
        runPipeline(ctx);
        afterPipeline(ctx);

        return null;
    }

    private JavaSparkContext buildContext() {
        String masterUrl = "local[2]"; // For local testing.
        if (sparkMaster != null) {
            masterUrl = sparkMaster;
        }

        SparkConf sparkConf = new SparkConf().setAppName(getProgramName())
                .setMaster(masterUrl)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator");

        return new JavaSparkContext(sparkConf);
    }

    // ---------------------------------------------------
    // Functions meant for overriding

    protected abstract void runPipeline(final JavaSparkContext ctx);

    /**
     * Override this to run code after the pipeline returns.
     */
    protected void afterPipeline(final JavaSparkContext ctx) {
        ctx.stop();
    }

    protected abstract String getProgramName();
    // ---------------------------------------------------
    // Helpers

}