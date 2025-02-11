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
use TenupETL\Utils\WithLogging;
use TenupETL\Classes\Load\LedgerRegistry;
use TenupETL\Classes\Load\Loaders;
use Flow\ETL\Join\{Join, Expression};
use Flow\ETL\DataFrame;

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
	use Loaders\WithLedger;

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
		$this->step_config = $job_config->get_value( 'load' );
		$this->loaders     = [];
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

		foreach ( $this->step_config as $load_operation ) {
			$this->perform_load_operation( $load_operation );
		}

		// Run the pipeline.
		$this->log( 'Running full pipeline...', 'progress' );
		$this->get_current_state()->run();

		// Handle ledgers.
		$this->write_ledgers();

		return $this;
	}

	/**
	 * Execute a single load operation
	 *
	 * @param array $load_operation Configuration for the load operation.
	 * @return void
	 */
	protected function perform_load_operation( $load_operation ) {
		$loader = $this->get_loader( $load_operation );

		$loader->run(
			$this->get_current_state()
				->batchSize( 5 )
		);

		array_push( $this->loaders, $loader );
	}

	/**
	 * Create loader instance
	 *
	 * @param array $step_config Loader configuration.
	 * @return object Loader instance.
	 */
	protected function create_loader( array $step_config ) {
		$loader_class = $this->get_loader_class( $step_config['loader'] );
		return new $loader_class( $step_config, $this->ledger_registry );
	}

	/**
	 * Get the appropriate loader instance for a load operation
	 *
	 * @param array $load_operation Configuration for the load operation.
	 * @return object The loader instance.
	 */
	protected function get_loader( $load_operation ) {
		$loader = $load_operation['loader'];

		return match ( strtolower( $loader ) ) {
			'json' => new Loaders\JsonLoader( $load_operation, $this->config ),
			'wp_post' => new Loaders\WordPressPostLoader( $load_operation, $this->config ),
			'wp_media' => new Loaders\WordPressMediaLoader( $load_operation, $this->config ),
			'wp_term' => new Loaders\WordPressTermLoader( $load_operation, $this->config ),
			'ledger' => new Loaders\LedgerLoader( $load_operation, $this->config ),
			'custom' => new($load_operation['pipeline'])( $load_operation, $this->config ),
			default => new Loaders\JsonLoader( $load_operation, $this->config ),
		};
	}

	/**
	 * Get the current ETL state
	 *
	 * @return DataFrame The current ETL state.
	 */
	public function get_current_state(): DataFrame {
		return $this->state;
	}

	/**
	 * Write ledger files for all loaders
	 *
	 * @return void
	 */
	protected function write_ledgers() {
		$ledgers = [];
		$loaders = $this->get_loaders_with_ledgers();

		// Exit early if there are no loaders with ledgers at all.
		if ( empty( $loaders ) ) {
			return;
		}

		// If there is only one ledger, write it to the root ledger path.
		if ( count( $loaders ) === 1 ) {
			$this->write_ledger( $loaders[0], $this->job_config->get_value( 'name' ) );
			return;
		}

		// If there are multiple ledgers, write each to its own file in the ledger path.
		foreach ( $loaders as $loader ) {
			$ledgers[ $loader->get_name() ] = $this->write_ledger( $loader );
		}

		// Determine the primary ledger to create a unified ledger.
		$primary_loader = $this->get_primary_loader_ledger();
		$primary_df     = data_frame( from_array( $primary_loader->get_ledger() ) );
		unset( $ledgers[ $primary_loader->get_name() ] );

		// Join other ledgers and write it to disk.
		foreach ( $ledgers as $ledger_name => $ledger ) {
			$primary_df->join(
				data_frame( from_array( $ledger->get_ledger() ) ),
				Expression::on( [ 'uid' => 'uid' ] ),
				Join::left
			);
		}

		$destination = $this->get_ledger_destination();

		$primary_df->batchSize( 100 )
			->write( to_json( $destination, )->withFlags( JSON_PRETTY_PRINT ) )
			->run();

		$this->log( 'Wrote primary ledger: ' . $ledger_name, 'progress' );
	}

	/**
	 * Get the primary loader ledger based on configuration
	 *
	 * @return object The primary loader instance
	 */
	protected function get_primary_loader_ledger() {
		$primary = $this->loaders[0];

		foreach ( $this->loaders as $loader ) {
			if ( 'wp_post' === strtolower( $loader->config['loader'] ) ) {
				$primary = $loader;
			}
		}

		foreach ( $this->loaders as $loader ) {
			if ( true === $loader->config['ledger']['primary'] ) {
				$primary = $loader;
			}
		}

		return $primary;
	}

	/**
	 * Get all loaders that have ledgers
	 *
	 * @return array Array of loader instances with ledgers
	 */
	protected function get_loaders_with_ledgers() {
		if ( ! is_array( $this->loaders ) ) {
			return [];
		}

		return array_values( array_filter( $this->loaders, fn ( $loader ) => $loader->has_ledger() ) );
	}

	/**
	 * Write a single ledger file
	 *
	 * @param object      $loader The loader instance
	 * @param string|null $name   Optional name override for the ledger
	 * @return DataFrame|null The ledger DataFrame or null if no ledger
	 */
	protected function write_ledger( $loader, $name = null ) {
		$ledger_name = $name ? $name : $loader->get_name();
		if ( $loader->has_ledger() ) {
			if ( $loader->get_ledger_schema() ) {
				$df = data_frame()->read( from_array( $loader->get_ledger() )->withSchema( $loader->get_ledger_schema() ) );
			} else {
				$df = data_frame()->read( from_array( $loader->get_ledger() ) );
			}
		}

		if ( ! $df ) {
			return;
		}

		$destination = $this->get_ledger_destination( $ledger_name );

		$df->batchSize( 100 )
			->write( to_json( $destination, )->withFlags( JSON_PRETTY_PRINT ) )
			->run();

		$this->log( 'Wrote ledger: ' . $ledger_name, 'progress' );

		return $df;
	}

	/**
	 * Get the destination path for a ledger file
	 *
	 * @param string|null $name Optional name override for the ledger
	 * @return string The full path for the ledger file
	 */
	protected function get_ledger_destination( $name = null ) {
		$name = $name ? $name : $this->job_config->get_value( 'name' );
		return join(
			DIRECTORY_SEPARATOR,
			[
				untrailingslashit( TENUP_ETL_PLUGIN_DIR ),
				untrailingslashit( ltrim( $this->config->get_value( 'ledger.path' ), './' ) ),
				untrailingslashit( ltrim( $this->job_config->get_value( 'ledger.path' ), './' ) ),
				$name . '-ledger-' . time() . '.json',
			]
		);
	}
}
