package org.broadinstitute.hellbender.tools.walkers.genotyper.afcalc;

import htsjdk.variant.variantcontext.Allele;
import org.broadinstitute.hellbender.utils.MathUtils;
import org.broadinstitute.hellbender.utils.QualityUtils;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

public final class AFCalculationResultUnitTest extends BaseTest {

    private static final Allele C = Allele.create("C");
    private static final Allele A = Allele.create("A", true);
    static final List<Allele> alleles = Arrays.asList(A, C);

    @DataProvider(name = "TestIsPolymorphic")
    public Object[][] makeTestIsPolymorphic() {
        List<Object[]> tests = new ArrayList<>();

        final List<Double> pValues = new LinkedList<>();
        for ( final double p : Arrays.asList(0.01, 0.1, 0.9, 0.99, 0.999, 1 - 1e-4, 1 - 1e-5, 1 - 1e-6) )
            for ( final double espilon : Arrays.asList(-1e-7, 0.0, 1e-7) )
                pValues.add(p + espilon);

        for ( final double pNonRef : pValues  ) {
            for ( final double pThreshold : pValues ) {
                final boolean shouldBePoly = pNonRef >= pThreshold;
                if ( pNonRef != pThreshold)
                    // let's not deal with numerical instability
                    tests.add(new Object[]{ pNonRef, pThreshold, shouldBePoly });
            }
        }

        return tests.toArray(new Object[][]{});
    }

    private AFCalculationResult makePolymorphicTestData(final double pNonRef) {
        return new AFCalculationResult(
                new int[]{0},
                alleles,
                MathUtils.normalizeLog10(new double[]{1 - pNonRef, pNonRef}),
                Collections.singletonMap(C, Math.log10(1 - pNonRef)));
    }

    @Test(dataProvider = "TestIsPolymorphic")
    private void testIsPolymorphic(final double pNonRef, final double pThreshold, final boolean shouldBePoly) {
            final AFCalculationResult result = makePolymorphicTestData(pNonRef);
            final boolean actualIsPoly = result.isPolymorphic(C, Math.log10(1 - pThreshold));
            Assert.assertEquals(actualIsPoly, shouldBePoly,
                    "isPolymorphic with pNonRef " + pNonRef + " and threshold " + pThreshold + " returned "
                            + actualIsPoly + " but the expected result is " + shouldBePoly);
    }

    @Test(dataProvider = "TestIsPolymorphic")
    private void testIsPolymorphicQual(final double pNonRef, final double pThreshold, final boolean shouldBePoly) {
        final AFCalculationResult result = makePolymorphicTestData(pNonRef);
        final double qual = QualityUtils.phredScaleCorrectRate(pThreshold);
        final boolean actualIsPoly = result.isPolymorphicPhredScaledQual(C, qual);
        Assert.assertEquals(actualIsPoly, shouldBePoly,
                "isPolymorphic with pNonRef " + pNonRef + " and threshold " + pThreshold + " returned "
                        + actualIsPoly + " but the expected result is " + shouldBePoly);
    }
}
