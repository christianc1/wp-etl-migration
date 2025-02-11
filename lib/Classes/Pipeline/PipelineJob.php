<?php
/**
 * Class: PipelineJob
 *
 * Handles individual ETL pipeline jobs by managing orchestrators for extract, transform and load steps.
 *
 * @package TenupETL\Classes\Pipeline
 */

namespace TenupETL\Classes\Pipeline;

use TenupETL\Classes\Config\{ GlobalConfig, JobConfig};
use TenupETL\Classes\Extract\Orchestrator as ExtractOrchestrator;
use TenupETL\Classes\Transform\Orchestrator as TransformOrchestrator;
use TenupETL\Classes\Load\Orchestrator as LoadOrchestrator;
use TenupETL\Utils\WithLogging;
use TenupETL\Classes\Load\LedgerRegistry;

use function Flow\ETL\DSL\{data_frame, to_array};

/**
 * Class PipelineJob
 *
 * Manages individual ETL pipeline jobs and their orchestrators.
 */
class PipelineJob {
	use WithLogging;

	/**
	 * Array of orchestrators for each pipeline step.
	 *
	 * @var array
	 */
	protected $orchestrators;

	/**
	 * Mapping of pipeline job types to orchestrator classes.
	 *
	 * @var array
	 */
	protected $orchestrator_map = [
		PipelineJobType::Extract->value   => ExtractOrchestrator::class,
		PipelineJobType::Transform->value => TransformOrchestrator::class,
		PipelineJobType::Load->value      => LoadOrchestrator::class,
	];

	/**
	 * Current state of the pipeline job.
	 *
	 * @var object
	 */
	public $state;

	/**
	 * Ledger registry instance
	 *
	 * @var LedgerRegistry
	 */
	protected $ledger_registry;

	/**
	 * Constructor
	 *
	 * @param GlobalConfig   $config          Global configuration object.
	 * @param JobConfig      $job_config      Job configuration object.
	 * @param LedgerRegistry $ledger_registry Ledger registry instance.
	 */
	public function __construct( GlobalConfig $config, JobConfig $job_config, LedgerRegistry $ledger_registry ) {
		$this->config          = $config;
		$this->job_config      = $job_config;
		$this->ledger_registry = $ledger_registry;
		$this->state           = data_frame();
	}

	/**
	 * Build the pipeline job by creating orchestrators for each step.
	 *
	 * @return PipelineJob Current pipeline job instance.
	 */
	public function build() {
		$steps = [ 'extract', 'transform', 'load' ];

		foreach ( $steps as $_step ) {
			$step = PipelineJobType::from( $_step );

			$this->orchestrators[ $step->value ] = $this->create_orchestrator( $step );
		}

		return $this;
	}

	/**
	 * Create an orchestrator for a given pipeline step.
	 *
	 * @param PipelineJobType $step The pipeline step type.
	 * @return object The created orchestrator instance.
	 */
	protected function create_orchestrator( PipelineJobType $step ) {
		$orchestrator = $this->orchestrator_map[ $step->value ];

		return new $orchestrator( $this->config, $this->job_config, $this->ledger_registry );
	}

	/**
	 * Process one, or all steps in the pipeline job.
	 *
	 * @param PipelineJobType|null $step The pipeline step type to process.
	 * @return PipelineJob Current pipeline job instance.
	 */
	public function process( $step = null ) {
		if ( $step ) {
			if ( ! $step instanceof PipelineJobType ) {
				$this->log( 'Step must be an instance of PipelineJobType', 'error' );
				return $this;
			}

			$this->load_dependencies( $step );
			$this->process_transaction( $step, $this->orchestrators[ $step->value ] );
			$this->unload_dependencies();

			return $this;
		}

		foreach ( $this->orchestrators as $step => $orchestrator ) {
			$this->load_dependencies( $step );
			$this->process_transaction( $step, $orchestrator );
			$this->unload_dependencies();
		}

		return $this;
	}

	/**
	 * Process a single transaction step with its orchestrator.
	 *
	 * @param string $step         The step being processed.
	 * @param object $orchestrator The orchestrator handling the step.
	 * @return PipelineJob Current pipeline job instance.
	 */
	protected function process_transaction( $step, $orchestrator ) {
		$this->state = $orchestrator->process( $this->state )->get_current_state();

		return $this;
	}

	/**
	 * Load dependencies for the pipeline job.
	 *
	 * @param string $step The step being processed.
	 * @return PipelineJob Current pipeline job instance.
	 */
	public function load_dependencies( $step ) {
		$dependencies = $this->job_config->get_value( 'depends_on' );

		if ( empty( $dependencies ) ) {
			return;
		}

		foreach ( $dependencies as $dependency ) {
			if ( ! $this->ledger_registry->get_ledger( $dependency ) ) {
				$this->log( 'Dependency not found: ' . $dependency, 'error' );
			}
		}

		return $this;
	}

	/**
	 * Unload dependencies for the pipeline job.
	 *
	 * @return PipelineJob Current pipeline job instance.
	 */
	public function unload_dependencies() {
		$dependencies = $this->job_config->get_value( 'depends_on' );

		if ( empty( $dependencies ) ) {
			return;
		}

		foreach ( $dependencies as $dependency ) {
			$this->ledger_registry->unload( $dependency );
		}

		return $this;
	}

	/**
	 * Get the current state of the pipeline job.
	 *
	 * @return object Current state object.
	 */
	public function get_current_state() {
		return $this->state;
	}
}
