package org.broadinstitute.hellbender.utils.help;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.RootDoc;
import org.broadinstitute.barclay.help.BashCompletionDocWorkUnitHandler;
import org.broadinstitute.barclay.help.*;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jonn on 6/15/17.
 */
public class BashCompletionDoclet extends HelpDoclet {

    private static final String DEFAULT_INDEX_TEMPLATE_NAME = "bash-completion.template.sh";

    protected static final String outputFileExtension = "sh";
    protected static final String indexFileExtension = "sh";

    /**
     * Create a doclet to process class information into a shell completion file using FreeMarker templates properties.
     * @param rootDoc The starting point of the doclet.
     * @throws IOException If an error occurs during the creation of the shell completion files.
     */
    public static boolean start(RootDoc rootDoc) throws IOException {
        return new BashCompletionDoclet().startProcessDocs(rootDoc);
    }

    @Override
    protected DocWorkUnit createWorkUnit(
            final DocumentedFeature documentedFeature,
            final ClassDoc classDoc,
            final Class<?> clazz)
    {
        return new DocWorkUnit(
                new BashCompletionDocWorkUnitHandler(this ),
                documentedFeature,
                classDoc,
                clazz);
    }

    /**
     * Adds a super-category so that we can custom-order the categories in the doc index
     *
     * @param docWorkUnit
     * @return
     */
    @Override
    protected final Map<String, String> getGroupMap(final DocWorkUnit docWorkUnit) {
        final Map<String, String> root = super.getGroupMap(docWorkUnit);

        /**
         * Add-on super-category definitions. The super-category and spark value strings need to be the
         * same as used in the Freemarker template.
         */
        root.put("supercat", HelpConstants.getSuperCategoryProperty(docWorkUnit.getGroupName()));
        root.put("isspark", HelpConstants.getSparkCategoryProperty(docWorkUnit.getGroupName()));

        return root;
    }

    /**
     * @return the name of the index template to be used for this doclet
     */
    public String getIndexTemplateName() { return DEFAULT_INDEX_TEMPLATE_NAME; }

}
