package org.broadinstitute.hellbender.utils.test;

import htsjdk.variant.variantcontext.*;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class VariantContextTestUtilsUnitTest {

    private static final Allele ARef = Allele.create("A", true);
    private static final Allele T = Allele.create("T");
    private static final Allele C = Allele.create("C");
    private static final Allele G = Allele.create("G");

    @DataProvider(name="valuesToNormalize")
    public Object[][] getValuesToNormalize(){
        final Object aSpecificObject = new Object();
        return new Object[][] {
                {"-2.172e+00", -2.172},
                {"-2.172", -2.172},
                {"-2.172e+01", -21.72},
                {-21.72, -21.72},
                {10, 10},
                {"SomeValue", "SomeValue"},
                {aSpecificObject,  aSpecificObject}

        };
    }

    @Test(dataProvider = "valuesToNormalize")
    public void testNormalizeScientificNotation(Object toNormalize, Object expected){
        Assert.assertEquals(VariantContextTestUtils.normalizeScientificNotation(toNormalize), expected);
    }

    @DataProvider
    public Object[][] getEqualVariantContexts(){

        return new Object[][] {
                {GenotypeBuilder.create("sample", Arrays.asList(T, G)), GenotypeBuilder.create("sample", Arrays.asList(G,T))}
        };
    }

    @Test(dataProvider = "getEqualVariantContexts")
    public void testGenotypeEquality(Genotype left, Genotype right){
        VariantContextTestUtils.assertGenotypesAreEqual(left,right);
    }


    @DataProvider
    public Object[][] getTListsToRemap(){
        return new Object[][]{
                {Arrays.asList(1, 2, 3, 4), Arrays.asList(1, 3, 4, 2), Arrays.asList(0, 2, 3, 1)},
                {Arrays.asList(1, 2, 3, 4), Arrays.asList(1, 2, 3, 4), Arrays.asList(0, 1, 2, 3)},
                {Arrays.asList("a", "b", "c","d"), Arrays.asList("a", "d", "c", "b"), Arrays.asList(0, 3,2,1)}
        };
    }


    @Test(dataProvider = "getTListsToRemap")
    public void testRemapping(List<Integer> original, List<Integer> expectedRemappedValues, List<Integer> mapping){
        Assert.assertEquals(VariantContextTestUtils.remapRTypeValues(original, mapping), expectedRemappedValues);
    }

    @DataProvider
    public Object[][] getAListsToRemap(){
        return new Object[][]{
                {Arrays.asList(1, 2, 3), Arrays.asList(2, 3, 1), Arrays.asList(0, 2, 3, 1)},
                {Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3), Arrays.asList(0, 1, 2, 3)},
                {Arrays.asList("a", "b", "c"), Arrays.asList("c", "b" , "a"), Arrays.asList(0, 3, 2, 1)}
        };
    }

    @Test(dataProvider = "getAListsToRemap")
    public void testARemapping(List<Integer> original, List<Integer> expectedRemappedValues, List<Integer> mapping){
        Assert.assertEquals(VariantContextTestUtils.remapATypeValues(original, mapping), expectedRemappedValues);
    }


    @DataProvider
    public Object[][] getGListsToRemap() {
        return new Object[][]{
            {Arrays.asList(0,1,2), Arrays.asList(2,1,0), Arrays.asList(ARef, T), Arrays.asList(T, ARef), 2},
            {Arrays.asList(0,1,2,3,4,5), Arrays.asList(2,1,0,4,3,5), Arrays.asList(ARef, T, C), Arrays.asList(T, ARef, C), 2}
        };
    }

    @Test(dataProvider = "getGListsToRemap")
    public void testGListsToRemap(List<Object> original, List<Object> expected, List<Allele> originalAlleles, List<Allele> remappedAlleles, int ploidy){
        Assert.assertEquals(VariantContextTestUtils.remapGTypeValues(original, originalAlleles, ploidy,
                                                                     VariantContextTestUtils.createAlleleIndexMap(
                                                                             originalAlleles, remappedAlleles)), expected);
    }
}


