<?php
/**
 * Synchronous loader class that runs multiple loaders in order, passing mutable rows through the loader chain.
 *
 * @package TenupETL\Classes\Load
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Classes\Config\GlobalConfig;
use TenupETL\Utils\WithLogging;
use Flow\ETL\{DataFrame};
use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Loader\Closure;
use Flow\DSL\{data_frame};
use Flow\ETL\Adapter\WordPress\Exception\WPAdapterDatabaseException;

/**
 * Synchronous loader class that runs multiple loaders in order, passing mutable rows through the loader chain.
 */
final class SynchronousPipelineLoader implements Loader, Closure {
	use WithLogging;

	/**
	 * Constructor
	 *
	 * @param Loader[] $loaders Loaders to run.
	 */
	public function __construct(
		protected array $loaders,
	) {}

	/**
	 * Load rows into destination
	 *
	 * @param Rows        $rows Rows to load.
	 * @param FlowContext $context Flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {
		foreach ( $this->loaders as $loader ) {
			try {
				$loader->load( $rows, $context );
			} catch ( WPAdapterDatabaseException $e ) {
				$this->log( $e->getMessage(), 'warning' );
			} catch ( \Exception $e ) {
				error_log( $e->getMessage() );
			}

			if ( $loader instanceof RowMutator && $loader->has_mutated_rows() ) {
				unset( $rows );
				$rows = $loader->collect_mutated_rows();

			}
		}

		// Final batch memory cleanup
		$this->cleanup_memory();
		$processed_count = count( $rows );
		$this->log( "Batch complete - Memory cleaned after processing $processed_count posts.", 'progress' );

		$memory_usage = memory_get_usage( true );
		$this->log( "Memory usage: " . round( $memory_usage / 1024 / 1024, 2 ) . " MB", 'debug' );
	}

	/**
	 * Closure method for the loader
	 *
	 * Called after the generator signals all rows have been yielded.
	 *
	 * @param FlowContext $context Flow context.
	 * @return void
	 */
	public function closure( FlowContext $context ): void {
		foreach ( $this->loaders as $loader ) {
			if ( $loader instanceof Closure ) {
				$loader->closure( $context );
			}
		}
	}

	/**
	 * Cleanup memory by releasing references and triggering garbage collection
	 *
	 * @return void
	 */
	public function cleanup_memory(): void {
		global $wp_object_cache, $wpdb;

		if ( ! is_object( $wp_object_cache ) ) {
			return;
		}

		$properties = [
			'group_ops',
			'memcache_debug',
			'cache',
		];

		foreach ( $properties as $property ) {
			if ( property_exists( $wp_object_cache, $property ) ) {
				$wp_object_cache->$property = [];
			}
		}

		if ( method_exists( $wp_object_cache, '__remoteset' ) ) {
			$wp_object_cache->__remoteset(); // important
		}

		$wpdb->queries = array();

		// Force PHP garbage collection
		if (function_exists('gc_collect_cycles')) {
			gc_collect_cycles();
		}
	}
}
