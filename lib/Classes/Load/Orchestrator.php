<?php
/**
 * Class Extract\Orchestrator
 *
 * @package TenupETL
 */

namespace TenupETL\Classes\Load;

use Symfony\Component\Yaml\Yaml;
use Symfony\Component\Finder\Finder;
use TenupETL\Classes\Config\{GlobalConfig, JobConfig};
use TenupETL\Classes\Load\Factories\AdapterFactory;
use TenupETL\Utils\WithLogging;
use TenupETL\Classes\Load\LedgerRegistry;
use TenupETL\Classes\Load\Loaders;
use Flow\ETL\Join\{Join, Expression};
use Flow\ETL\{DataFrame, Rows, FlowContext};

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, from_array, to_array, to_output, to_stream, uuid_v4};
use function Flow\ETL\Adapter\JSON\{to_json};


/**
 * Class Orchestrator
 *
 * This class orchestrates an extraction job
 *
 * @package TenupETL\Utils\Extract
 */
class Orchestrator {
	use WithLogging;

	/**
	 * The current ETL state
	 *
	 * @var DataFrame
	 */
	protected $state;

	/**
	 * Configuration for the current load step
	 *
	 * @var array
	 */
	protected $step_config;

	/**
	 * Adapter factory instance
	 *
	 * @var AdapterFactory
	 */
	protected $adapter_factory;

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
	) {
		$this->step_config = $job_config->get_value( 'load' );
		$this->loaders     = [];
		$this->adapter_factory = new AdapterFactory( $this->config );
	}

	/**
	 * Process the ETL state through the load pipeline
	 *
	 * @param DataFrame $state The ETL state to process.
	 * @return $this
	 */
	public function process( DataFrame $state ) {
		$this->log( 'Building load pipeline...', 'progress' );
		$this->state = $state;

		// Add a uid to the state
		$this->get_current_state()
			->withEntry( 'etl.uid', uuid_v4() );

		$this->get_current_state()
			->write( $this->get_synchronous_loader() );

		// Run the pipeline.
		$this->log( 'Running full pipeline...', 'progress' );
		$this->get_current_state()->run();

		return $this;
	}

	public function get_synchronous_loader() {
		$loaders = [];
		foreach ( $this->step_config as $load_operation ) {
			array_push( $loaders, $this->adapter_factory->create( $load_operation ) );
		}

		return new Loaders\SynchronousPipelineLoader( $loaders );
	}

	/**
	 * Get the current ETL state
	 *
	 * @return DataFrame The current ETL state.
	 */
	public function get_current_state(): DataFrame {
		return $this->state;
	}

}
