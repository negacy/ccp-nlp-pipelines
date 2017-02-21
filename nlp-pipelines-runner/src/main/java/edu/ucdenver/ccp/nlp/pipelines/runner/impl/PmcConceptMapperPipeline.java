package edu.ucdenver.ccp.nlp.pipelines.runner.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;

import edu.ucdenver.ccp.common.file.CharacterEncoding;
import edu.ucdenver.ccp.nlp.pipelines.conceptmapper.ConceptMapperParams;
import edu.ucdenver.ccp.nlp.pipelines.conceptmapper.ConceptMapperPipelineCmdOpts;
import edu.ucdenver.ccp.nlp.pipelines.conceptmapper.ConceptMapperPipelineFactory;
import edu.ucdenver.ccp.nlp.pipelines.runlog.Document.FileVersion;
import edu.ucdenver.ccp.nlp.pipelines.runlog.DocumentCollection.PMC_OA_DocumentCollection;
import edu.ucdenver.ccp.nlp.pipelines.runner.DeploymentParams;
import edu.ucdenver.ccp.nlp.pipelines.runner.PipelineBase;
import edu.ucdenver.ccp.nlp.pipelines.runner.PipelineKey;
import edu.ucdenver.ccp.nlp.pipelines.runner.PipelineParams;
import edu.ucdenver.ccp.nlp.pipelines.runner.RunCatalogAE;
import edu.ucdenver.ccp.nlp.pipelines.runner.serialization.AnnotationSerializer.IncludeCoveredText;
import edu.ucdenver.ccp.nlp.pipelines.runner.serialization.AnnotationSerializerAE;
import edu.ucdenver.ccp.nlp.uima.annotators.sentence_detection.ExplicitSentenceCasInserter;
import edu.ucdenver.ccp.nlp.uima.annotators.sentence_detection.OpenNlpSentenceDetectorAE;
import edu.ucdenver.ccp.nlp.uima.annotators.sentence_detection.Sentence;
import edu.ucdenver.ccp.nlp.uima.shims.document.impl.CcpDocumentMetadataHandler;
import edu.ucdenver.ccp.nlp.uima.util.View;

public class PmcConceptMapperPipeline extends PipelineBase {

	private static final Logger logger = Logger.getLogger(PmcConceptMapperPipeline.class);
	private static final String AGGREGATE_DESCRIPTOR_PATH_ON_CLASSPATH = "/pipeline_descriptors/pmc_conceptmapper_aggregate.xml";
	private static final String PIPELINE_DESCRIPTION = "Annotates the PMC corpus using the UIMA sandbox ConceptMapper tool.";
	private final ConceptMapperParams conceptMapperParams;
	private final File dictionaryFile;

	public PmcConceptMapperPipeline(File catalogDirectory, File configDir, int numToProcess, String brokerUrl,
			ConceptMapperParams conceptMapperParams, File dictionaryFile, int casPoolSize) throws Exception {
		super(new PipelineParams(new PMC_OA_DocumentCollection().getShortname(), FileVersion.LOCAL_TEXT,
				CharacterEncoding.UTF_8, View.DEFAULT.viewName(),
				PipelineKey.CONCEPTMAPPER.name() + "_" + conceptMapperParams.name(), PIPELINE_DESCRIPTION,
				catalogDirectory, numToProcess, 0, brokerUrl, casPoolSize), configDir);
		this.conceptMapperParams = conceptMapperParams;
		this.dictionaryFile = dictionaryFile;
	}

	/**
	 * @return the path to the aggregate descriptor on the classpath
	 */
	@Override
	protected String getAggregateDescriptorPath() {
		return AGGREGATE_DESCRIPTOR_PATH_ON_CLASSPATH;
	}

	@Override
	protected TypeSystemDescription getPipelineTypeSystem() {
		return TypeSystemDescriptionFactory.createTypeSystemDescription("edu.ucdenver.ccp.nlp.core.uima.TypeSystem",
				"edu.ucdenver.ccp.nlp.wrapper.conceptmapper.TypeSystem",
				"edu.ucdenver.ccp.nlp.uima.annotators.TypeSystem", "analysis_engine.primitive.DictTerm",
				"org.apache.uima.conceptMapper.support.tokenizer.TokenAnnotation", "uima.tt.TokenAnnotation");
	}

	@Override
	protected DeploymentParams getPipelineDeploymentParams() {
		String serviceName = "PMC_CONCEPTMAPPER";
		String endpoint = "pmc_conceptmapper_pipelineQ";
		int scaleup = 1;
		int errorThresholdCount = 1;

		return new DeploymentParams(serviceName, PIPELINE_DESCRIPTION, scaleup, errorThresholdCount, endpoint,
				getPipelineParams().getBrokerUrl());
	}

	@Override
	protected List<ServiceEngine> createServiceEngines() throws ResourceInitializationException {
		List<ServiceEngine> engines = new ArrayList<ServiceEngine>();

		{
			/* configure the sentence detector AE */
			boolean treatLineBreaksAsSentenceBoundaries = true;
			AnalysisEngineDescription sentenceDetecterDesc = OpenNlpSentenceDetectorAE.createAnalysisEngineDescription(
					getPipelineTypeSystem(), ExplicitSentenceCasInserter.class, treatLineBreaksAsSentenceBoundaries);

			int sentdetect_scaleup = getPipelineParams().getCasPoolSize();
			int sentdetect_errorThreshold = 0;
			String sentdetect_endpoint = "sentdetectQ";

			DeploymentParams sentdetectDeployParams = new DeploymentParams("sentdetect",
					"Adds sentence annotations to the CAS.", sentdetect_scaleup, sentdetect_errorThreshold,
					sentdetect_endpoint, getPipelineParams().getBrokerUrl());
			ServiceEngine xml2txtEngine = new ServiceEngine(sentenceDetecterDesc, sentdetectDeployParams,
					"sentDetectAE", DescriptorType.PRIMITIVE);
			engines.add(xml2txtEngine);
		}
		{
			/* create the ConceptMapper AE */
			ConceptMapperPipelineCmdOpts cmdOptions;
			try {
				cmdOptions = getConceptMapperCmdOpts(dictionaryFile);
			} catch (IOException e) {
				throw new ResourceInitializationException(e);
			}
			/*
			 * the next command returns three AE descriptions 1) ConceptMapper,
			 * 2) CCP type system conversion AE 3) token removal
			 */
			List<AnalysisEngineDescription> cmAeDescriptions;
			try {
				cmAeDescriptions = ConceptMapperPipelineFactory.getPipelineAeDescriptions(getPipelineTypeSystem(),
						cmdOptions, conceptMapperParams.paramIndex());
			} catch (UIMAException | IOException e) {
				throw new ResourceInitializationException(e);
			}

			AnalysisEngineDescription conceptMapperAeDesc = AnalysisEngineFactory.createEngineDescription(
					cmAeDescriptions.toArray(new AnalysisEngineDescription[cmAeDescriptions.size()]));

			conceptMapperAeDesc.setAnnotatorImplementationName("conceptmapper");
			int conceptMapper_scaleup = getPipelineParams().getCasPoolSize();
			int conceptMapper_errorThreshold = 0;
			String conceptMapper_endpoint = "conceptMapperQ";

			DeploymentParams conceptMapperDeployParams = new DeploymentParams("ConceptMapper",
					"Runs ConceptMapper over sentences in the CAS.", conceptMapper_scaleup,
					conceptMapper_errorThreshold, conceptMapper_endpoint, getPipelineParams().getBrokerUrl());
			ServiceEngine conceptMapperEngine = new ServiceEngine(conceptMapperAeDesc, conceptMapperDeployParams,
					"conceptMapperAAE", DescriptorType.PRIMITIVE);
			engines.add(conceptMapperEngine);
		}
		/* TODO: add sentence removal filter here? */
		{
			/* serialize the conceptmapper annotations */
			String sourceViewName = View.DEFAULT.viewName();
			String outputViewName = View.DEFAULT.viewName();
			boolean compressOutput = true;
			String outputFileInfix = PipelineKey.CONCEPTMAPPER.name() + "_" + conceptMapperParams.name();
			AnalysisEngineDescription annotSerializerDesc = AnnotationSerializerAE
					.getDescription_SaveToSourceFileDirectory(getPipelineTypeSystem(), CcpDocumentMetadataHandler.class,
							sourceViewName, outputViewName, compressOutput, outputFileInfix, IncludeCoveredText.NO);

			int annotSerializer_scaleup = getPipelineParams().getCasPoolSize() / 2;
			int annotSerializer_errorThreshold = 0;
			String annotSerializer_endpoint = "annotSerializerQ";

			DeploymentParams annotSerializerDeployParams = new DeploymentParams("AnnotSerializer",
					"Serializes the annotations to file.", annotSerializer_scaleup, annotSerializer_errorThreshold,
					annotSerializer_endpoint, getPipelineParams().getBrokerUrl());
			ServiceEngine annotSerializerEngine = new ServiceEngine(annotSerializerDesc, annotSerializerDeployParams,
					"annotSerializerAE", DescriptorType.PRIMITIVE);
			engines.add(annotSerializerEngine);
		}
		{
			/* configure catalog AE */
			AnalysisEngineDescription catalogAeDesc = RunCatalogAE.getDescription(getPipelineTypeSystem(),
					getPipelineParams().getCatalogDirectory(), CcpDocumentMetadataHandler.class,
					getPipelineParams().getPipelineKey());

			int catalogAe_scaleup = 1;
			int catalogAe_errorThreshold = 0;
			String catalogAe_endpoint = "catalogAeQ";

			DeploymentParams catalogAeDeployParams = new DeploymentParams("RunCatalog",
					"Catalogs new annotation-output and document files.", catalogAe_scaleup, catalogAe_errorThreshold,
					catalogAe_endpoint, getPipelineParams().getBrokerUrl());
			ServiceEngine catalogAeEngine = new ServiceEngine(catalogAeDesc, catalogAeDeployParams, "runCatalogAE",
					DescriptorType.PRIMITIVE);
			engines.add(catalogAeEngine);
		}
		return engines;

	}

	private static ConceptMapperPipelineCmdOpts getConceptMapperCmdOpts(File dictionaryFile) throws IOException {
		ConceptMapperPipelineCmdOpts cmdOptions = new ConceptMapperPipelineCmdOpts();
		cmdOptions.setDictionaryFile(dictionaryFile);
		cmdOptions.setSpanClass(Sentence.class);
		return cmdOptions;
	}

	/**
	 * @param args
	 *            args[0] = catalog directory <br>
	 *            args[1] = config directory (a work directory where UIMA
	 *            descriptor files will be written)
	 */
	public static void main(String[] args) {
		BasicConfigurator.configure();
		Logger.getRootLogger().setAdditivity(false);
		File catalogDirectory = new File(args[0]);
		File configDirectory = new File(args[1]);
		String brokerUrl = args[2];
		int numToProcess = -1; // <0 = process all
		int casPoolSize = Integer.parseInt(args[3]);
		ConceptMapperParams conceptMapperParams = ConceptMapperParams.valueOf(args[3]);
		File dictionaryDirectory = new File(args[4]);
		logger.info("Starting PmcNxml2TxtPipeline...\nCatalog directory=" + catalogDirectory.getAbsolutePath()
				+ "\nConfig directory=" + configDirectory.getAbsolutePath() + "\nNum-to-process=" + numToProcess
				+ "\nBroker URL: " + brokerUrl);
		try {
			PmcConceptMapperPipeline pipeline = new PmcConceptMapperPipeline(catalogDirectory, configDirectory,
					numToProcess, brokerUrl, conceptMapperParams,
					UpdateConceptMapperDictionaryFiles.getDictionaryFile(dictionaryDirectory, conceptMapperParams),
					casPoolSize);

			pipeline.configurePipeline();

			/* turn on debugging mode */
			pipeline.setDebugFlag(true);

			logger.info("Deploying pipeline components...");
			pipeline.deployPipeline();
			logger.info("Running pipeline...");
			pipeline.runPipeline();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}