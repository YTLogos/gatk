package org.broadinstitute.hellbender.tools.walkers.genotyper.afcalc;

import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public final class AFPriorProviderUnitTest extends BaseTest {

    private static final double TOLERANCE = 0.0001;

    @Test(dataProvider="HeterozygosityProviderData")
    public void testHeterozygosityProvider(final double h, final int useCount, final int minPloidy, final int maxPloidy) {
        final double het = h / maxPloidy;
        final Random rdn = Utils.getRandomGenerator();
        final int[] plodies = new int[useCount];
        for (int i = 0; i < useCount; i++)
            plodies[i] = rdn.nextInt(maxPloidy - minPloidy + 1) + minPloidy;

        final AFPriorProvider provider = new HeterozygosityAFPriorProvider(het);
        for (int i = 0; i < useCount; i++) {
            final int ploidy = plodies[i];
            double[] priors = provider.forTotalPloidy(ploidy);
            Assert.assertNotNull(priors);
            Assert.assertEquals(priors.length, ploidy + 1);
            Assert.assertEquals(MathUtils.approximateLog10SumLog10(priors), 0, TOLERANCE);
            for (int j = 0; j < priors.length; j++) {
                Assert.assertTrue(!Double.isNaN(priors[j]));
                Assert.assertTrue(priors[j] < 0);
                if (j > 0) Assert.assertEquals(priors[j], Math.log10(het) - Math.log10(j), TOLERANCE);
            }
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testErrorNegativeHet() throws Exception {
        new HeterozygosityAFPriorProvider(-0.1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testErrorTooHighHet() throws Exception {
        new HeterozygosityAFPriorProvider(1.1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testErrorNaNHet() throws Exception {
        new HeterozygosityAFPriorProvider(Double.NaN);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testErrorHeterozygosityTooHighForPloidy() throws Exception {
        new HeterozygosityAFPriorProvider(0.999).buildPriors(2);
    }


    private double[] hets = { 0.00001, 0.001, 0.1, 0.5, 0.99, 0.999 };
    private int[] useCounts = { 10, 100, 1000 };

    private int[] ploidy = { 1 , 2, 3, 10, 100, 200, 500};

    @DataProvider(name="HeterozygosityProviderData")
    public Object[][] heterozygosityProviderData() {
        final Object[][] result = new Object[hets.length * useCounts.length * ((ploidy.length + 1) * (ploidy.length) / 2)][];
        int idx = 0;
        for (double h : hets)
            for (int sc : useCounts)
                for (int i = 0; i < ploidy.length; i++)
                    for (int j = i; j < ploidy.length; j++)
                        result[idx++] = new Object[] { h, sc, ploidy[i], ploidy[j]};
        return result;
    }
}
