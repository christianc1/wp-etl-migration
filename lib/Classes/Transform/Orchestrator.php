<?php
/**
 * Class Transform\Orchestrator
 *
 * This class orchestrates the transformation phase of an ETL job by applying configured
 * transformation pipelines to the data state.
 *
 * @package TenupETL\Classes\Transform
 */

namespace TenupETL\Classes\Transform;

use Symfony\Component\Yaml\Yaml;
use Symfony\Component\Finder\Finder;
use TenupETL\Classes\Config\{GlobalConfig, JobConfig};
use TenupETL\Utils\WithLogging;
use TenupETL\Classes\Load\LedgerRegistry;

use AmazingFactsMigration\Transformations\PageTransformations;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, from_array, to_array, to_output};

/**
 * Class Orchestrator
 *
 * This class orchestrates the transformation phase by:
 * - Processing configured transformation pipelines
 * - Managing the data state between transformations
 * - Tracking transformation progress
 *
 * @package TenupETL\Classes\Transform
 */
class Orchestrator {
	use WithLogging;

	/**
	 * The current state of the data being transformed
	 *
	 * @var mixed
	 */
	protected $state;

	/**
	 * Configuration for the transformation step
	 *
	 * @var array
	 */
	protected $step_config;

	/**
	 * Transaction tracking array
	 *
	 * @var array
	 */
	protected $transaction;

	/**
	 * Constructor
	 *
	 * @param GlobalConfig   $config          Global configuration object.
	 * @param JobConfig      $job_config      Job-specific configuration object.
	 * @param LedgerRegistry $ledger_registry Ledger registry instance.
	 */
	public function __construct(
		protected GlobalConfig $config,
		protected JobConfig $job_config,
		protected LedgerRegistry $ledger_registry
	) {
		$this->config      = $config;
		$this->job_config  = $job_config;
		$this->step_config = $job_config->get_value( 'transform' );
		$this->transaction = [];
	}

	/**
	 * Process the transformation step
	 *
	 * Applies each configured transformation pipeline to the data state in sequence.
	 *
	 * @param mixed $state The initial data state to transform.
	 * @return self
	 */
	public function process( $state ) {
		$this->log( 'Building transformation pipeline...', 'progress' );
		$this->state = $state;

		foreach ( $this->step_config as $transformation ) {
			$this->apply_transformation( $transformation );
		}

		return $this;
	}

	/**
	 * Apply a single transformation pipeline
	 *
	 * Creates and runs the specified transformation pipeline class on the current state.
	 *
	 * @param array $transformation Configuration for the transformation to apply.
	 * @return self
	 */
	protected function apply_transformation( $transformation ) {
		$configured_pipeline = $transformation['pipeline'];

		if ( ! class_exists( $configured_pipeline ) ) {
			$this->log( 'TransformationPipeline not found: ' . $configured_pipeline, 'progress' );

			return $this;
		}

		$transformation_pipeline = new $configured_pipeline( $this->state, $this->ledger_registry );

		$this->state = $transformation_pipeline->run()->get_final_state();

		return $this;
	}

	/**
	 * Get the current state of the data
	 *
	 * @return mixed The current transformed state.
	 */
	public function get_current_state() {
		return $this->state;
	}
}
