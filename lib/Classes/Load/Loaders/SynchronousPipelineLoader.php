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

/**
 * Synchronous loader class that runs multiple loaders in order, passing mutable rows through the loader chain.
 */
final class SynchronousPipelineLoader implements Loader, Closure {

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
			$loader->load( $rows, $context );

			if ( $loader instanceof RowMutator && $loader->has_mutated_rows() ) {
				$rows = $loader->collect_mutated_rows();
			}
		}
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
}
