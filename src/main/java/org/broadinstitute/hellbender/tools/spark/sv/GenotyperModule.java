package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeLikelihoodCalculator;
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeLikelihoodCalculators;
import org.broadinstitute.hellbender.utils.genotyper.ReadLikelihoods;
import org.broadinstitute.hellbender.utils.genotyper.SampleList;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shuang on 1/10/17.
 */
class GenotyperModule implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LogManager.getLogger(GenotyperModule.class);

    // TODO: 1/10/17 as we grow to support more types of evidence (e.g. pairs), this should be modified
    static Tuple2<SVJunction, GenotypeLikelihoods> genotype(final Tuple2<SVJunction, List<GATKRead>> readsForAJunction){
        return new Tuple2<>( readsForAJunction._1 ,getGenotypeLikelihoods(computeReadLikelihoods(readsForAJunction)) );
    }

    static ReadLikelihoods<SVDummyAllele> computeReadLikelihoods(final Tuple2<SVJunction, List<GATKRead>> readsForAJunction){

        final SVReadLikelihoodCalculator readLikelihoodCalculator = new InversionReadLikelihoodCalculator();
        readLikelihoodCalculator.initialize();
        readLikelihoodCalculator.configure();
        final List<GATKRead> preprocessedReads = readLikelihoodCalculator.preprocessReads(readsForAJunction._2());
        logger.debug(".........DONE PREPROCESSING READS.........");

        final SVJunction junction = readsForAJunction._1();
        final Map<String, List<GATKRead>> sample2Reads = new LinkedHashMap<>();

        final SampleList sampleList = SampleList.singletonSampleList(SingleDiploidSampleBiallelicSVGenotyperSpark.testSampleName);
        sample2Reads.put(sampleList.getSample(0), preprocessedReads);

        final ReadLikelihoods<SVDummyAllele> rll = readLikelihoodCalculator.computeReadLikelihoods(junction, sampleList, preprocessedReads, sample2Reads);
        readLikelihoodCalculator.close();
        logger.debug(".........RLC DONE.........");

        final ReadLikelihoods<SVDummyAllele> result = junction.updateReads(rll);
        logger.debug(".........POSTPROCESSING DONE.........");

        return result;
    }

    static GenotypeLikelihoods getGenotypeLikelihoods(final ReadLikelihoods<SVDummyAllele> rll){
        final GenotypeLikelihoodCalculator genotypeLikelihoodCalculator
                = new GenotypeLikelihoodCalculators().getInstance(SingleDiploidSampleBiallelicSVGenotyperSpark.ploidy,
                SingleDiploidSampleBiallelicSVGenotyperSpark.alleleCount); // TODO: diploid, biallelic assumption
        final GenotypeLikelihoods result = genotypeLikelihoodCalculator.genotypeLikelihoods( rll.sampleMatrix(0) );
        logger.debug(".........GLC DONE.........");
        return result;
    }

    static void writeDebugReadLikelihoods(final JavaRDD<SVJunction> readLikelihoodsJavaPairRDD,
                                          final String debugStringSavingPath){
        readLikelihoodsJavaPairRDD.map(j -> j.debugString).saveAsTextFile(debugStringSavingPath);
    }
}
