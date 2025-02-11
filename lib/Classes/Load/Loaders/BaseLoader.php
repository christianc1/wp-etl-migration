<?php
/**
 * Base loader class that other loaders extend from.
 *
 * Provides common loader functionality and implements the Flow ETL Loader interface.
 *
 * @package TenupETL\Classes\Load
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Classes\Config\GlobalConfig;
use TenupETL\Utils\WithLogging;
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\DSL\{data_frame};

/**
 * Base loader class that implements core loader functionality.
 */
class BaseLoader implements Loader {
	use WithLogging;
	use WithLedger;

	/**
	 * Name of the loader
	 *
	 * @var string
	 */
	protected $name;

	/**
	 * Configuration for this load step
	 *
	 * @var array
	 */
	public $config;

	/**
	 * Constructor
	 *
	 * @param array        $step_config   Configuration for this load step.
	 * @param GlobalConfig $global_config Global configuration.
	 */
	public function __construct( protected array $step_config, protected GlobalConfig $global_config ) {
		$this->config = $step_config;
		$this->name   = $step_config['name'];
		$this->uid    = time();

		if ( isset( $this->step_config['ledger'] ) ) {
			$this->ledger = [];
		}
	}

	/**
	 * Get the name of the loader
	 *
	 * @return string
	 */
	public function get_name(): string {
		return $this->name;
	}

	/**
	 * Run the loader
	 *
	 * @param object $state The ETL state object.
	 * @return self
	 */
	public function run( $state ) {
		$this->log( 'Running loader using adapter: ' . $this->name, 'progress' );

		$state->write( $this );

		return $this;
	}

	/**
	 * Load rows into destination
	 *
	 * @param Rows        $rows Rows to load.
	 * @param FlowContext $context Flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {}

	/**
	 * Reduce a row to only fields with a given prefix
	 *
	 * @param Row    $row Row to reduce.
	 * @param string $prefix Prefix to filter on.
	 * @return array Filtered array of values.
	 */
	protected function reduce_row_on_prefix( Row $row, string $prefix, bool $unpack = false, string $unpack_delimiter = '.' ): array {
		// Ensure prefix has a trailing dot
		$prefix = rtrim( $prefix, '.' ) . '.';

		$arr = $row->toArray();

		// Reduce the array to only the keys that are prefixed with the prefix.
		$arr = array_filter(
			$arr,
			function ( $key ) use ( $prefix ) {
				return strpos( $key, $prefix ) === 0;
			},
			ARRAY_FILTER_USE_KEY
		);

		// Combine new keys with the original values
		$arr = array_combine(
			array_map(
				function ( $key ) use ( $prefix ) {
					return strpos( $key, $prefix ) === 0
						? substr( $key, strlen( $prefix ) )
						: $key;
				},
				array_keys( $arr )
			),
			array_values( $arr )
		);

		if ( ! $unpack ) {
			return $arr;
		}

		// Handle unpacking nested keys recursively
		$result = [];
		foreach ( $arr as $key => $value ) {
			$parts = explode( $unpack_delimiter, $key );
			$this->unpack_recursive( $result, $parts, $value );
		}

		return $result;
	}

	public function unpack_recursive( array &$array, array $keys, $value ) {
		$key = array_shift( $keys );

		if ( empty( $keys ) ) {
			$array[ $key ] = $value;
			return;
		}

		if ( ! isset( $array[ $key ] ) || ! is_array( $array[ $key ] ) ) {
			$array[ $key ] = [];
		}

		$this->unpack_recursive( $array[ $key ], $keys, $value );
	}

	/**
	 * Check if loader has a ledger
	 *
	 * @return bool True if loader has ledger property and it's truthy.
	 */
	public function has_ledger() {
		return property_exists( $this, 'ledger' ) && (bool) $this->ledger;
	}
}
