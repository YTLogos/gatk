package org.broadinstitute.hellbender.utils.help;

import org.broadinstitute.barclay.help.DefaultDocWorkUnitHandler;
import org.broadinstitute.barclay.help.DocWorkUnit;
import org.broadinstitute.barclay.help.HelpDoclet;

/**
 * Created by jonn on 6/15/17.
 */
public class BashCompletionDocWorkUnitHandler extends DefaultDocWorkUnitHandler {

    private static final String DEFAULT_FREEMARKER_BASH_COMPLETION_TEMPLATE_NAME = "bash-completion.placeholder.template.sh";

    public BashCompletionDocWorkUnitHandler(HelpDoclet doclet) {
        super(doclet);
    }

    /**
     * Return the template to be used for the particular workUnit. Must be present in the location
     * specified is the -settings-dir doclet parameter.
     *
     * @param workUnit workUnit for which a template ie being requested
     * @return name of the template file to use, relative to -settings-dir
     */
    @Override
    public String getTemplateName(final DocWorkUnit workUnit) {
        return DEFAULT_FREEMARKER_BASH_COMPLETION_TEMPLATE_NAME;
    }
}
