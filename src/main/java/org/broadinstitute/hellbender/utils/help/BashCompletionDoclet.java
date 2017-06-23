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

    protected static final String outputFileExtension = "sh";

    protected static final String indexFileBaseName = "shell-completion";
    protected static final String indexFileExtension = "sh";

    public static boolean start(RootDoc rootDoc) {
        try {
            return new BashCompletionDoclet().startProcessDocs(rootDoc);
        } catch (IOException e) {
            throw new DocException("Exception processing javadoc", e);
        }
    }

    @Override
    protected DocWorkUnit createWorkUnit(
            final DocumentedFeature documentedFeature,
            final ClassDoc classDoc,
            final Class<?> clazz)
    {
        return new DocWorkUnit(
                new DefaultDocWorkUnitHandler(this ),
                documentedFeature,
                classDoc,
                clazz);
    }

    /**
     * Actually write out the shell completion output file.
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
     */
    @Override
    protected void emitOutputFromTemplates (
            final List<Map<String, String>> groupMaps,
            final List<Map<String, String>> featureMaps)
    {
        try {
            /* ------------------------------------------------------------------- */
            /* You should do this ONLY ONCE in the whole application life-cycle:   */
            final Configuration cfg = new Configuration();
            cfg.setDirectoryForTemplateLoading(settingsDir); // where the template files come from
            cfg.setObjectWrapper(new DefaultObjectWrapper());

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
            final File indexFile = new File(getDestinationDir()
                    + File.separator
                    + getIndexBaseFileName()
                    + "."
                    + getIndexFileExtension()
            );

            // Run the template and merge in the data
            try (final FileOutputStream fileOutStream = new FileOutputStream(indexFile);
                 final OutputStreamWriter outWriter = new OutputStreamWriter(fileOutStream)) {
                template.process(rootMap, outWriter);
            } catch (TemplateException e) {
                throw new DocException("Freemarker Template Exception during documentation index creation", e);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException("FileNotFoundException processing javadoc template", e);
        } catch (IOException e) {
            throw new RuntimeException("IOException processing javadoc template", e);
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
