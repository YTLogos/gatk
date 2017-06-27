package org.broadinstitute.hellbender.utils.help;

import org.broadinstitute.barclay.help.*;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.RootDoc;
import freemarker.template.Configuration;
import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;
import freemarker.template.TemplateException;

import java.io.*;
import java.util.*;

/**
 * For testing of help documentation generation.
 */
public class BashCompletionDoclet extends HelpDoclet {

    private static final String FILE_EXTENSION = "sh";

    protected static final String outputFileExtension = FILE_EXTENSION;

    protected static final String indexFileBaseName = "shell-completion";
    protected static final String indexFileExtension = FILE_EXTENSION;

    public static boolean start(RootDoc rootDoc) {
        try {
            return new BashCompletionDoclet().startProcessDocs(rootDoc);
        } catch (IOException e) {
            throw new DocException("Exception processing javadoc", e);
        }
    }

    @Override
    protected void processWorkUnitTemplate(
            final Configuration cfg,
            final DocWorkUnit workUnit,
            final List<Map<String, String>> indexByGroupMaps,
            final List<Map<String, String>> featureMaps) {

        // For the Bash Test Doclet, this is a no-op.
        // We only care about the index file.
    }

    /**
     * The Index file in the Bash Test Doclet is what generates the actual tab-completion script.
     *
     * This will actually write out the shell completion output file.
     * The Freemarker instance will see a top-level variable that looks like the following:
     *
     * SimpleMap tools = SimpleMap { ToolName : MasterPropertiesMap }
     *
     *     where
     *
     *     MasterPropertiesMap is a map containing the following Keys:
     *         all
     *         common
     *         positional
     *         hidden
     *         advanced
     *         deprecated
     *         optional
     *         dependent
     *         required
     *
     *         Each of those keys maps to a List&lt;SimpleMap&gt; representing each property.
     *         These property maps each contain the following keys:
     *
     *             kind
     *             name
     *             summary
     *             fulltext
     *             otherArgumentRequired
     *             synonyms
     *             exclusiveOf
     *             type
     *             options
     *             attributes
     *             required
     *             minRecValue
     *             maxRecValue
     *             minValue
     *             maxValue
     *             defaultValue
     *
     * @param cfg
     * @param workUnitList
     * @param groupMaps
     * @throws IOException
     */
    @Override
    protected void processIndexTemplate(
            final Configuration cfg,
            final List<DocWorkUnit> workUnitList,
            final List<Map<String, String>> groupMaps
    ) throws IOException {
        // Create a root map for all the work units so we can access all the info we need:
        Map<String, Object> propertiesMap = new HashMap<>();
        workUnits.stream().forEach( workUnit -> propertiesMap.put(workUnit.getName(), workUnit.getRootMap()) );

        // Add everything into a nice package that we can iterate over
        // while exposing the command line program names as keys:
        Map<String, Object> rootMap = new HashMap<>();
        rootMap.put("tools", propertiesMap);

        // Get or create a template
        final Template template = cfg.getTemplate(getIndexTemplateName());

        // Create the output file
        final File indexFile = new File(getDestinationDir(),
                getIndexBaseFileName() + "." + getIndexFileExtension()
        );

        // Run the template and merge in the data
        try (final FileOutputStream fileOutStream = new FileOutputStream(indexFile);
             final OutputStreamWriter outWriter = new OutputStreamWriter(fileOutStream)) {
            template.process(rootMap, outWriter);
        } catch (TemplateException e) {
            throw new DocException("Freemarker Template Exception during documentation index creation", e);
        }
    }

    /**
     * @return the name of the index template to be used for this doclet
     */
    @Override
    public String getIndexTemplateName() { return "bash-completion.template.ftl"; }

    /**
     * @return The base filename for the index file associated with this doclet.
     */
    @Override
    public String getIndexBaseFileName() { return indexFileBaseName; }

}
