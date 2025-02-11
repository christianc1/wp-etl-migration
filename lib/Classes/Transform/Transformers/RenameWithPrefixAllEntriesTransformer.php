<?php
/**
 * Transformer: RenameWithPrefixAllEntriesTransformer
 *
 * Renames all entries in a row by adding a prefix to their names.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\{FlowContext, Rows, Row, Transformer};

/**
 * Transformer that adds a prefix to all entry names.
 */
final class RenameWithPrefixAllEntriesTransformer implements Transformer {

	/**
	 * Constructor.
	 *
	 * @param string $prefix The prefix to add to entry names.
	 */
	public function __construct( private readonly string $prefix ) {}

	/**
	 * Transforms rows by adding a prefix to all entry names.
	 *
	 * @param Rows        $rows    The rows to transform.
	 * @param FlowContext $context The flow context.
	 * @return Rows The transformed rows.
	 */
	public function transform( Rows $rows, FlowContext $context ): Rows {
		return $rows->map(
			function ( Row $row ): Row {
				foreach ( $row->entries()->all() as $entry ) {
					$row = $row->rename( $entry->name(), $this->prefix . $entry->name() );
				}

				return $row;
			}
		);
	}
}
