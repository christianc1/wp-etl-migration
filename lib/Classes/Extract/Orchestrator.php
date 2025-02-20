<?php
/**
 * Class Extract\Orchestrator
 *
 * Orchestrates the extraction process by managing extractors, normalizers and pipelines.
 * Handles extracting data from configured sources, normalizing it, and running it through
 * configured transformation pipelines.
 *
 * @package TenupETL
 */

namespace TenupETL\Classes\Extract;

use TenupETL\Classes\Config\{GlobalConfig, JobConfig};
use TenupETL\Classes\Extract\Extractors\LocalFileSystemExtractor;
use TenupETL\Classes\Extract\Factories\AdapterFactory;
use TenupETL\Classes\Extract\Normalizers\DataNormalizer;
use TenupETL\Classes\Load\LedgerRegistry;
use TenupETL\Utils\WithLogging;
use TenupETL\Classes\Extract\Factories\MultiSourceFactory;

use function TenupETL\Classes\Transform\Transformers\{prefix_ref, rename_regex};
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\DSL\{data_frame, from_array, to_array, to_output};

/**
 * Class Orchestrator
 *
 * This class orchestrates an extraction job by coordinating extractors, normalizers
 * and transformation pipelines. It manages the state of the extraction process and
 * ensures data flows correctly through each step.
 *
 * @package TenupETL\Utils\Extract
 */
class Orchestrator {
	use WithLogging;

	/**
	 * Current state of the extraction process
	 *
	 * @var mixed
	 */
	protected $state;

	/**
	 * Configuration for the extraction steps
	 *
	 * @var array
	 */
	protected $step_config;

	/**
	 * Current transaction being processed
	 *
	 * @var array
	 */
	protected $transaction;

	/**
	 * Constructor
	 *
	 * @param GlobalConfig        $config          Global configuration settings.
	 * @param JobConfig           $job_config      Job-specific configuration.
	 * @param LedgerRegistry      $ledger_registry Ledger registry instance.
	 * @param AdapterFactory|null $adapter_factory Factory for creating data adapters.
	 * @param DataNormalizer|null $normalizer      Normalizer for standardizing data.
	 */
	public function __construct(
		protected GlobalConfig $config,
		protected JobConfig $job_config,
		protected LedgerRegistry $ledger_registry,
		protected AdapterFactory|null $adapter_factory = null,
		protected DataNormalizer|null $normalizer = null
	) {
		$this->step_config = $job_config->get_value( 'extract' );
		$this->transaction = [];

		if ( ! $this->adapter_factory ) {
			$fs_extractor          = new LocalFileSystemExtractor( $this->config->get_value( 'sources.localfs.path' ) );
			$this->adapter_factory = new AdapterFactory( $fs_extractor );
		}

		if ( ! $this->normalizer ) {
			$this->normalizer = new DataNormalizer();
		}
	}

	/**
	 * Process the extraction
	 *
	 * Executes the extraction process by iterating through configured extraction steps
	 * and applying them to the initial state.
	 *
	 * @param mixed $state Initial state to begin extraction from.
	 * @return self
	 */
	public function process( $state ) {
		$this->log( 'Building extraction pipeline...', 'progress' );
		$this->state = $state;

		foreach ( $this->step_config as $extraction ) {
			$this->apply_extraction( $extraction );
		}

		return $this;
	}

	/**
	 * Apply extraction configuration
	 *
	 * Processes a single extraction step by extracting data from source,
	 * normalizing it, and running it through the configured pipeline.
	 *
	 * @param array $extraction Configuration for this extraction step.
	 * @return self
	 */
	protected function apply_extraction( $extraction ) {
		if ( ! empty( $extraction['adapter'] ) && 'rss' === $extraction['adapter'] ) {
			$this->extract_from_multi_source( $extraction );
		} else {
			$this->extract_from_source( $extraction );
		}

		$normalize = $extraction['normalize'] ?? true;

		if ( $normalize ) {
			$this->state = $this->normalizer->normalize( $this->state );
			$this->state = $this->normalizer->prefix( $this->state, $extraction['prefix'] );
		}

		$configured_pipeline = $extraction['pipeline'];

		if ( ! class_exists( $configured_pipeline ) ) {
			$this->log( 'ExtractionPipeline not found: ' . $this->job_config->get_value( 'name' ), 'warning' );
			return $this;
		}

		$extraction_pipeline = new $configured_pipeline( $this->state, new LedgerRegistry( $this->config ) );
		$this->state         = $extraction_pipeline->run()->get_final_state();

		return $this;
	}

	/**
	 * Extract from configured source
	 *
	 * Handles extraction from the configured data source using the appropriate adapter.
	 *
	 * @param array $extraction Configuration specifying the source and extraction details.
	 * @return self
	 */
	protected function extract_from_source( $extraction ) {
		if ( ! isset( $extraction['adapter'] ) ) {
			$this->log( 'No adapter specified for extraction', 'error' );
			return $this;
		}

		$this->state = $this->get_current_state()
			->extract(
				$this->adapter_factory->create( $extraction['adapter'], $extraction )
			);

		return $this;
	}

	/**
	 * Extract from multiple sources
	 *
	 * @param array $extraction Extraction configuration
	 * @return self
	 */
	protected function extract_from_multi_source( array $extraction ): self {
		$factory  = new MultiSourceFactory();
		$iterator = $factory->create( $extraction );

		if ( ! $iterator ) {
			$this->log( 'Failed to create source iterator', 'error' );
			return $this;
		}

		$initial_state = clone $this->state;

		// Perform initial extraction
		$this->extract_from_source( $extraction );

		// Iterate through the iterator and perform additional extractions
		foreach ( $iterator as $index => $url ) {
			if ( $index === 0 ) {
				continue;
			}

			$this->log( 'Extracting from ' . $url, 'progress' );
			$current_state = $this->get_current_state();

			// Reset the state to the initial state
			$this->state = $initial_state;

			// Create a copy of the extraction config with the current URL
			$current_extraction                           = $extraction;
			$current_extraction['source']['rss']['url']   = $url;
			$current_extraction['source']['rss']['multi'] = false;

			// Perform extraction
			$this->extract_from_source( $current_extraction );

			$new_state = $this->get_current_state();

			// Merge the new state with the current state
			$this->state = $this->merge_states( $current_state, $new_state );
		}

		return $this;
	}

	public function merge_states( $current_state, $new_state ) {
		$current = [];
		$new     = [];
		$current_state->write( to_array( $current ) )->run();
		$new_state->write( to_array( $new ) )->run();

		$merged = data_frame()
			->extract( from_array( array_merge( $current, $new ) ) );

		unset( $current, $new );
		return $merged;
	}

	/**
	 * Get current state
	 *
	 * Returns the current state of the extraction process.
	 *
	 * @return mixed Current state of the extraction.
	 */
	public function get_current_state() {
		return $this->state;
	}
}
