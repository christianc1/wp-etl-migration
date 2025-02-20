<?php
/**
 * Transformers
 *
 * Helper functions for creating ETL transformers.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Transformer;

/**
 * Creates a transformer that prefixes all entry references with the given prefix.
 *
 * @param string $prefix The prefix to add to entry references.
 * @return Transformer The prefixing transformer.
 */
function prefix_ref( string $prefix ): Transformer {
	return new RenameWithPrefixAllEntriesTransformer( $prefix );
}

/**
 * Creates a transformer that renames entries using regex pattern matching.
 *
 * @param string $pattern The regex pattern to match.
 * @param string $replacement The replacement pattern.
 * @return Transformer The regex renaming transformer.
 */
function rename_regex( string $pattern, string $replacement ): Transformer {
	return new RenameRegexReplaceAllEntriesTransformer( $pattern, $replacement );
}

/**
 * Creates a transformer that converts a string to null.
 *
 * @param string $string The string to convert to null.
 * @return Transformer The string to null transformer.
 */
function string_to_null( string $string ): Transformer {
	return new StringToNullTransformer( $string );
}

/**
 * Creates a transformer that selects entries with a given prefix.
 *
 * @param string  $prefix        The prefix to select entries with.
 * @param boolean $remove_prefix Whether to remove the prefix from the entries.
 * @return Transformer The select prefix transformer.
 */
function select_prefix( string $prefix, bool $remove_prefix = false ): Transformer {
	return new SelectPrefixTransformer( $prefix, $remove_prefix );
}

/**
 * Creates a simple transformer from a callable function.
 *
 * @param callable $call The function to use for transformation.
 * @param array    $args Optional arguments to pass to the transformer.
 * @return Transformer The simple transformer.
 */
function simple_transformer( callable $call, array $args = [] ): ScalarFunction {
	return new SimpleTransformer( call: $call, args: $args );
}

/**
 * Creates a slug transformer.
 *
 * @param mixed                 $ref    The reference to the string to transform.
 * @param ScalarFunction|string $prefix Optional prefix string.
 * @return ScalarFunction The slug transformer.
 */
function to_slug( mixed $ref, ScalarFunction|string $prefix = '' ): ScalarFunction {
	return new ToSlug( $ref, $prefix );
}

/**
 * Creates an explode transformer.
 *
 * @param ScalarFunction|string $delimeter The delimeter to explode the string by.
 * @param ScalarFunction|string $ref       The reference to the string to explode.
 * @return ScalarFunction The explode transformer.
 */
function to_explode( ScalarFunction|string $delimeter = ',', ScalarFunction|string $ref, ): ScalarFunction {
	return new Explode( $ref, $delimeter );
}

/**
 * Creates a compare set null transformer.
 *
 * @param mixed                 $ref The reference to the value to compare.
 * @param ScalarFunction|string $compare_value The value to compare against.
 */
function compare_set_null( mixed $ref, ScalarFunction|string $compare_value ): ScalarFunction {
	return new CompareSetNull( $ref, $compare_value );
}
