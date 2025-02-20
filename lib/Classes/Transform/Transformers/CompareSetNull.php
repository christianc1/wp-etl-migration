<?php
/**
 * Transformer: CompareSetNull
 *
 * Compares a value to a compare value and sets the value to null if they are equal.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\Function\{ScalarFunctionChain, Parameter};
use Flow\ETL\{Row};
use Flow\ETL\Type;

/**
 * Compares a value to a compare value and sets the value to null if they are equal.
 */
final class CompareSetNull extends ScalarFunctionChain {

	/**
	 * Constructor.
	 *
	 * @param ScalarFunction|string $value         The string to transform.
	 * @param mixed                 $compare_value The compare value.
	 */
	public function __construct(
		private readonly ScalarFunction|string $value,
		private readonly mixed $compare_value
	) {}

	/**
	 * Compares a value to a compare value and sets the value to null if they are equal.
	 *
	 * @param Row $row The row to transform.
	 * @return mixed The transformed result.
	 */
	public function eval( Row $row ): mixed {
		$value = $row->get( $this->value )->value();

		if ( null === $value ) {
			return null;
		}

		if ( $value === $this->compare_value ) {
			return null;
		}

		return $value;
	}
}
