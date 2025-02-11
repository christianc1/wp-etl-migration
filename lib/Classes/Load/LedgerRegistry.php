<?php
/**
 * Class LedgerRegistry
 *
 * Manages access to ETL ledger files that track migration operations and their results.
 * Provides on-demand loading of ledger data from disk with caching.
 *
 * @package TenupETL\Classes\Load
 */

namespace TenupETL\Classes\Load;

use TenupETL\Utils\WithLogging;
use TenupETL\Classes\Config\GlobalConfig;
use function Flow\ETL\DSL\{data_frame};
use function Flow\ETL\Adapter\JSON\{from_json};

/**
 * Registry for managing and accessing ETL ledger files
 */
class LedgerRegistry {
	use WithLogging;

	/**
	 * Cache of loaded ledger data
	 *
	 * @var array
	 */
	protected $ledgers = [];

	/**
	 * Base path for ledger files
	 *
	 * @var string
	 */
	protected $base_path;

	/**
	 * Global configuration object
	 *
	 * @var GlobalConfig
	 */
	protected $global_config;

	/**
	 * Constructor
	 *
	 * @param GlobalConfig $global_config Global configuration object.
	 */
	public function __construct( GlobalConfig $global_config ) {
		$this->global_config = $global_config;
	}

	/**
	 * Add a ledger to track
	 *
	 * @param string $name The name of the ledger to add.
	 * @return LedgerRegistry Returns $this for method chaining.
	 */
	public function add_ledger( string $name ) {
		$this->ledgers[] = $name;
		return $this;
	}

	/**
	 * Get ledger data by name
	 *
	 * Loads ledger from disk if not already cached.
	 *
	 * @param string $name The ledger name to retrieve.
	 * @return array|false The ledger data or false if not found.
	 */
	public function get_ledger( string $name ) {
		if ( ! isset( $this->ledgers[ $name ] ) ) {
			$load = $this->load_ledger( $name );

			if ( ! $load ) {
				return false;
			}
		}

		return $this->ledgers[ $name ];
	}

	/**
	 * Load a ledger file from disk
	 *
	 * Finds the most recent ledger file matching the name pattern and loads it.
	 *
	 * @param string $name The ledger name to load.
	 * @return bool True if ledger loaded successfully, false otherwise.
	 */
	protected function load_ledger( string $name ) {
		// Find the right migration config.
		$migration_config = array_values( array_filter( $this->global_config->get_value( 'migration' ), fn ( $config ) => $config['name'] === $name ) );

		$migration_config = $migration_config[0] ?? null;

		// Return early if the migration doesn't exist.
		if ( ! $migration_config ) {
			$this->log( "No migration config found for {$name}", 'warning' );
			return false;
		}

		// Get a list of ledgers for the migration.
		$pattern          = join(
			DIRECTORY_SEPARATOR,
			[
				untrailingslashit( TENUP_ETL_PLUGIN_DIR ),
				untrailingslashit( ltrim( $this->global_config->get_value( 'ledger.path' ), './' ) ),
				untrailingslashit( ltrim( $migration_config['ledger']['path'], './' ) ),
				$name . '-ledger-*.json',
			]
		);

		$files = glob( $pattern );

		if ( empty( $files ) ) {
			$this->log( "No ledger file found for {$name}", 'warning' );
			return false;
		}

		// Get most recent ledger file
		$latest_file = end( $files );

		$this->ledgers[ $name ] = data_frame()->read( from_json( $latest_file ) );

		return true;
	}

	/**
	 * Remove a ledger from the registry
	 *
	 * @param string $name The name of the ledger to unload.
	 * @return void
	 */
	public function unload( string $name ) {
		unset( $this->ledgers[ $name ] );
	}
}
