<?php
/**
 * Transformer: Explode
 *
 * Explodes a string into an array.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\Function\{ScalarFunctionChain, Parameter};
use Flow\ETL\{Row};

/**
 * Explodes a string into an array.
 */
final class Explode extends ScalarFunctionChain {

	/**
	 * Constructor.
	 *
	 * @param ScalarFunction|string $value  The string to transform.
	 * @param ScalarFunction|string $delimeter The delimeter to explode the string by.
	 */
	public function __construct(
		private readonly ScalarFunction|string $value,
		private readonly ScalarFunction|string $delimeter = ','
	) {}

	/**
	 * Explodes a string into an array.
	 *
	 * @param Row $row The row to transform.
	 * @return mixed The transformed result.
	 */
	public function eval( Row $row ): mixed {
		$string = $row->get( $this->value );

		if ( null === $string ) {
			return null;
		}

		try {
			return array_map( 'trim', explode( $this->delimeter, $string ) );
		} catch ( \Exception $e ) {
			return explode( $this->delimeter, $string );
		}
	}
}
