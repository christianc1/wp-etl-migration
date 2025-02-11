<?php
/**
 * Transformer: SimpleTransformer
 *
 * A simple transformer that wraps a callable function to transform rows.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\Function\{ScalarFunctionChain, Parameter};
use Flow\ETL\{Row};

/**
 * Transforms a value
 */
final class ToSlug extends ScalarFunctionChain {

	/**
	 * Constructor.
	 *
	 * @param ScalarFunction|string $value  The string to transform.
	 * @param ScalarFunction|string $prefix The prefix to add to the string.
	 */
	public function __construct( private readonly mixed $value, private readonly ScalarFunction|string $prefix ) {}

	/**
	 * Sanitizes a string to a slug.
	 *
	 * @param Row $row The row to transform.
	 * @return mixed The transformed result.
	 */
	public function eval( Row $row ): mixed {
		$string = ( new Parameter( $this->value ) )->eval( $row );

		if ( null === $string ) {
			return null;
		}

		if ( ! function_exists( 'sanitize_title' ) ) {
			return $string;
		}

		return sanitize_title( $this->prefix . $string );
	}
}
