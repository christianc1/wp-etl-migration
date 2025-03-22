<?php
/**
 * Transformer: SimpleTransformer
 *
 * A simple transformer that wraps a callable function to transform rows.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\{FlowContext, Rows, Row, Transformer};
use TenupETL\Utils\WithLogging;

/**
 * Simple transformer that executes a callable function on rows.
 */
final class SimpleTransformer implements ScalarFunction, Transformer {
	use WithLogging;

	/**
	 * The callable function to execute.
	 *
	 * @var callable
	 */
	private $call;

	/**
	 * Constructor.
	 *
	 * @param callable $call The function to execute on each row.
	 * @param array    $args Additional arguments to pass to the function.
	 */
	public function __construct( callable $call, private readonly array $args ) {
		$this->call = $call;
	}

	/**
	 * Evaluates the transformer function on a row.
	 *
	 * @param Row $row The row to transform.
	 * @return mixed The transformed result.
	 */
	public function eval( Row $row ): mixed {
		return call_user_func( $this->call, $row, ...$this->args );
	}

	public function transform( Rows $rows, FlowContext $context ): Rows {
		$rows->each( fn( Row $row ) => $this->eval( $row ) );

		return $rows;
	}
}
